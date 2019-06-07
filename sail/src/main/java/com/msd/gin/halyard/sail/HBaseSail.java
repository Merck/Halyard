/*
 * Copyright 2016 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
 * Inc., Kenilworth, NJ, USA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.msd.gin.halyard.sail;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.eclipse.rdf4j.IsolationLevel;
import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.FN;
import org.eclipse.rdf4j.model.vocabulary.SPIF;
import org.eclipse.rdf4j.model.vocabulary.SPIN;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.spin.SpinParser;
import org.eclipse.rdf4j.spin.function.AskFunction;
import org.eclipse.rdf4j.spin.function.ConstructTupleFunction;
import org.eclipse.rdf4j.spin.function.EvalFunction;
import org.eclipse.rdf4j.spin.function.SelectTupleFunction;
import org.eclipse.rdf4j.spin.function.spif.CanInvoke;
import org.eclipse.rdf4j.spin.function.spif.ConvertSpinRDFToString;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.common.TimestampedValueFactory;
import com.msd.gin.halyard.optimizers.HalyardEvaluationStatistics;
import com.msd.gin.halyard.vocab.HALYARD;

/**
 * HBaseSail is the RDF Storage And Inference Layer (SAIL) implementation on top of Apache HBase.
 * It implements the interfaces - {@code Sail, SailConnection} and {@code FederatedServiceResolver}. Currently federated queries are
 * only supported for queries across multiple graphs in one Halyard database.
 * @author Adam Sotona (MSD)
 */
public class HBaseSail implements Sail, FederatedServiceResolver {

    /**
     * Ticker is a simple service interface that is notified when some data are processed.
     * It's purpose is to notify a caller (for example MapReduce task) that the execution is still alive.
     */
    public interface Ticker {

        /**
         * This method is called whenever a new Statement is populated from HBase.
         */
        public void tick();
    }

	/**
	 * Interface to make it easy to change connection implementations.
	 */
	public interface ConnectionFactory {
		SailConnection createConnection(HBaseSail sail);
	}

    private static final long STATUS_CACHING_TIMEOUT = 60000l;
    private static final String MIN_TIMESTAMP_QUERY_PARAM = "minTimestamp";
    private static final String MAX_TIMESTAMP_QUERY_PARAM = "maxTimestamp";
    private static final String MAX_VERSIONS_QUERY_PARAM = "maxVersions";

    private final Configuration config; //the configuration of the HBase database
    final String tableName;
    final boolean create;
    final boolean pushStrategy;
    final int splitBits;
	protected HalyardEvaluationStatistics statistics;
    final int evaluationTimeout;
    private boolean readOnly = false;
    private long readOnlyTimestamp = -1;
    final String elasticIndexURL;
    final Ticker ticker;
	FunctionRegistry functionRegistry = FunctionRegistry.getInstance();
	TupleFunctionRegistry tupleFunctionRegistry = TupleFunctionRegistry.getInstance();
	SpinParser spinParser = new SpinParser();
	long minTimestamp = 0;
	long maxTimestamp = Long.MAX_VALUE;
	int maxVersions = 1;
	final ConnectionFactory connFactory;
	Connection hConnection;
	final boolean hConnectionIsShared; //whether a Connection is provided or we need to create our own

	private final Cache<String, SailFederatedService> federatedServices = CacheBuilder.newBuilder().maximumSize(100)
			.removalListener((evt) -> ((SailFederatedService) evt.getValue()).shutdown()).build();


    private HBaseSail(Connection conn, Configuration config, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, String elasticIndexURL, Ticker ticker, ConnectionFactory connFactory) {
    	this.hConnection = conn;
    	this.hConnectionIsShared = (conn != null);
        this.config = config;
        this.tableName = tableName;
        this.create = create;
        this.splitBits = splitBits;
        this.pushStrategy = pushStrategy;
        this.evaluationTimeout = evaluationTimeout;
        this.elasticIndexURL = elasticIndexURL;
        this.ticker = ticker;
		this.connFactory = connFactory;
    }

    /**
	 * Construct HBaseSail object with given arguments.
	 * 
	 * @param config Hadoop Configuration to access HBase
	 * @param tableName HBase table name used to store data
	 * @param create boolean option to create the table if it does not exist
	 * @param splitBits int number of bits used for the calculation of HTable region pre-splits (applies for new tables only)
	 * @param pushStrategy boolean option to use {@link com.msd.gin.halyard.strategy.HalyardEvaluationStrategy} instead of
	 * {@link org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategy}
	 * @param evaluationTimeout int timeout in seconds for each query evaluation, negative values mean no timeout
	 * @param elasticIndexURL String optional ElasticSearch index URL
	 * @param ticker optional Ticker callback for keep-alive notifications
	 * @param connFactory {@link ConnectionFactory} for creating connections
	 */
    public HBaseSail(Configuration config, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, String elasticIndexURL, Ticker ticker, ConnectionFactory connFactory) {
    	this(null, config, tableName, create, splitBits, pushStrategy, evaluationTimeout, elasticIndexURL, ticker, connFactory);
    }

    public HBaseSail(Connection conn, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, String elasticIndexURL, Ticker ticker) {
		this(conn, conn.getConfiguration(), tableName, create, splitBits, pushStrategy, evaluationTimeout, elasticIndexURL, ticker, HBaseSailConnection.Factory.INSTANCE);
	}

    public HBaseSail(Configuration config, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, String elasticIndexURL, Ticker ticker) {
		this(null, config, tableName, create, splitBits, pushStrategy, evaluationTimeout, elasticIndexURL, ticker, HBaseSailConnection.Factory.INSTANCE);
	}

    /**
     * Not used in Halyard
     */
    @Override
    public void setDataDir(File dataDir) {
    }

    /**
     * Not used in Halyard
     */
    @Override
    public File getDataDir() {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns a new HTable connection.
     */
	Table getTable() {
        try {
			return HalyardTableUtils.getTable(hConnection, tableName, create, splitBits);
		} catch (IOException e) {
			throw new SailException(e);
		}
    }

	BufferedMutator getBufferedMutator(Table table) {
		try {
			return hConnection.getBufferedMutator(table.getName());
		} catch (IOException e) {
			throw new SailException(e);
		}
	}

	@Override
    public void initialize() throws SailException { //initialize the SAIL
    	if (!hConnectionIsShared) {
			// connections are thread-safe and very heavyweight - only do it once
        	if (hConnection != null) {
        		throw new IllegalStateException("Sail has already been initialized");
        	}
			try {
				hConnection = HalyardTableUtils.getConnection(config);
			} catch (IOException e) {
				throw new SailException(e);
			}
    	}

    	try {
			if (!create && !hConnection.getAdmin().tableExists(TableName.valueOf(tableName))) {
				throw new SailException(String.format("Table does not exist: %s", tableName));
			}
		}
		catch (IOException e) {
			throw new SailException(e);
		}

    	this.statistics = new HalyardEvaluationStatistics(new HalyardStatsBasedStatementPatternCardinalityCalculator(this), (String service) -> {
			SailFederatedService fedServ = getService(service);
			return fedServ != null ? ((HBaseSail) fedServ.getSail()).statistics : null;
		});

		registerSpinParsingFunctions();
		registerSpinParsingTupleFunctions();
    }

	private void registerSpinParsingFunctions() {
		if (!(functionRegistry.get(FN.CONCAT.toString()).get() instanceof org.eclipse.rdf4j.spin.function.Concat)) {
			functionRegistry.add(new org.eclipse.rdf4j.spin.function.Concat());
		}
		if (!functionRegistry.has(SPIN.EVAL_FUNCTION.toString())) {
			functionRegistry.add(new EvalFunction(spinParser));
		}
		if (!functionRegistry.has(SPIN.ASK_FUNCTION.toString())) {
			functionRegistry.add(new AskFunction(spinParser));
		}
		if (!functionRegistry.has(SPIF.CONVERT_SPIN_RDF_TO_STRING_FUNCTION.toString())) {
			functionRegistry.add(new ConvertSpinRDFToString(spinParser));
		}
		if (!functionRegistry.has(SPIF.CAN_INVOKE_FUNCTION.toString())) {
			functionRegistry.add(new CanInvoke(spinParser));
		}
	}

	void registerSpinParsingTupleFunctions() {
		if (!tupleFunctionRegistry.has(SPIN.CONSTRUCT_PROPERTY.stringValue())) {
			tupleFunctionRegistry.add(new ConstructTupleFunction(spinParser));
		}
		if (!tupleFunctionRegistry.has(SPIN.SELECT_PROPERTY.stringValue())) {
			tupleFunctionRegistry.add(new SelectTupleFunction(spinParser));
		}
	}

    @Override
	public SailFederatedService getService(String serviceUrl) throws QueryEvaluationException {
        //provide a service to query over Halyard graphs. Remote service queries are not supported.
        if (serviceUrl.startsWith(HALYARD.NAMESPACE)) {
			final String federatedTable;
        	List<NameValuePair> queryParams;
        	int queryParamsPos = serviceUrl.lastIndexOf('?');
        	if (queryParamsPos != -1) {
            	federatedTable = serviceUrl.substring(HALYARD.NAMESPACE.length(), queryParamsPos);
                queryParams = URLEncodedUtils.parse(serviceUrl.substring(queryParamsPos+1), StandardCharsets.UTF_8);
        	} else {
        		federatedTable = serviceUrl.substring(HALYARD.NAMESPACE.length());
        		queryParams = Collections.emptyList();
        	}

        	SailFederatedService federatedService;
            try {
				federatedService = federatedServices.get(serviceUrl, () -> {
				    HBaseSail sail = new HBaseSail(hConnection, config, federatedTable, false, 0, true, evaluationTimeout, null, ticker, HBaseSailConnection.Factory.INSTANCE);
					for (NameValuePair nvp : queryParams) {
						switch (nvp.getName()) {
						case MIN_TIMESTAMP_QUERY_PARAM:
							sail.minTimestamp = HalyardTableUtils.toHalyardTimestamp(Long.parseLong(nvp.getValue()),
									false);
							break;
						case MAX_TIMESTAMP_QUERY_PARAM:
							sail.maxTimestamp = HalyardTableUtils.toHalyardTimestamp(Long.parseLong(nvp.getValue()),
									false);
							break;
						case MAX_VERSIONS_QUERY_PARAM:
							sail.maxVersions = Integer.parseInt(nvp.getValue());
							break;
						}
					}
				    sail.initialize();
				    return new SailFederatedService(sail);
				});
			}
			catch (ExecutionException e) {
				if (e.getCause() instanceof RuntimeException) {
					throw (RuntimeException) e.getCause();
				} else {
					throw new AssertionError(e.getClass());
				}
			}
            return federatedService;
        } else {
            throw new QueryEvaluationException("Unsupported service URL: " + serviceUrl);
        }
    }

    @Override
    public void shutDown() throws SailException { //release resources
		if (!hConnectionIsShared && hConnection != null) {
			try {
				hConnection.close();
				hConnection = null;
			} catch (IOException e) {
				throw new SailException(e);
			}
		}

		federatedServices.invalidateAll(); // release the references to the services
    }

    @Override
    public boolean isWritable() throws SailException {
		long time = System.currentTimeMillis();
		if (readOnlyTimestamp + STATUS_CACHING_TIMEOUT < time) {
			try (Table table = getTable()) {
        		readOnly = table.getTableDescriptor().isReadOnly();
				readOnlyTimestamp = time;
	        } catch (IOException ex) {
	            throw new SailException(ex);
	        }
	    }
        return !readOnly;
    }

    @Override
	public SailConnection getConnection() throws SailException {
		if (hConnection == null) {
			throw new IllegalStateException("Sail is not initialized or has been shut down");
		}
		return connFactory.createConnection(this);
    }

    @Override
    public ValueFactory getValueFactory() {
        return TimestampedValueFactory.getInstance();
    }

    @Override
    public List<IsolationLevel> getSupportedIsolationLevels() {
        return Collections.singletonList((IsolationLevel) IsolationLevels.NONE); //limited by HBase's capabilities
    }

    @Override
    public IsolationLevel getDefaultIsolationLevel() {
        return IsolationLevels.NONE;
    }
}
