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

import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.common.IdValueFactory;
import com.msd.gin.halyard.common.IdentifiableValueIO;
import com.msd.gin.halyard.function.DynamicFunctionRegistry;
import com.msd.gin.halyard.optimizers.HalyardEvaluationStatistics;
import com.msd.gin.halyard.vocab.HALYARD;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.eclipse.rdf4j.IsolationLevel;
import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.FN;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.SPIF;
import org.eclipse.rdf4j.model.vocabulary.SPIN;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryContextInitializer;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.AbstractFederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
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

/**
 * HBaseSail is the RDF Storage And Inference Layer (SAIL) implementation on top of Apache HBase.
 * It implements the interfaces - {@code Sail, SailConnection} and {@code FederatedServiceResolver}. Currently federated queries are
 * only supported for queries across multiple graphs in one Halyard database.
 * @author Adam Sotona (MSD)
 */
public class HBaseSail implements Sail {

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

	static final class ScanSettings {
		long minTimestamp = 0;
		long maxTimestamp = Long.MAX_VALUE;
		int maxVersions = 1;
	}

	private static final long STATUS_CACHING_TIMEOUT = 60000l;

    private final Configuration config; //the configuration of the HBase database
	final TableName tableName;
	final boolean create;
    final boolean pushStrategy;
	final int splitBits;
	private SailConnection statsConnection;
	protected HalyardEvaluationStatistics statistics;
	final int evaluationTimeout; // secs
    private boolean readOnly = false;
    private long readOnlyTimestamp = -1;
    final String elasticIndexURL;
	boolean includeNamespaces = false;
    final Ticker ticker;
	private FederatedServiceResolver federatedServiceResolver;
	private IdentifiableValueIO valueIO;
	private ValueFactory valueFactory;
	private FunctionRegistry functionRegistry = new DynamicFunctionRegistry();
	private TupleFunctionRegistry tupleFunctionRegistry = TupleFunctionRegistry.getInstance();
	private SpinParser spinParser = new SpinParser();
	private final List<QueryContextInitializer> queryContextInitializers = new ArrayList<>();
	ScanSettings scanSettings = new ScanSettings();
	final ConnectionFactory connFactory;
	Connection hConnection;
	final boolean hConnectionIsShared; //whether a Connection is provided or we need to create our own


	private HBaseSail(Connection conn, Configuration config, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, String elasticIndexURL, Ticker ticker, ConnectionFactory connFactory) {
		this(conn, config, tableName, create, splitBits, pushStrategy, evaluationTimeout, elasticIndexURL, ticker, connFactory, new HBaseFederatedServiceResolver(conn, config, tableName, evaluationTimeout, ticker));
    }

	HBaseSail(Connection conn, Configuration config, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, String elasticIndexURL, Ticker ticker, ConnectionFactory connFactory,
			FederatedServiceResolver fsr) {
		this.hConnection = conn;
		this.hConnectionIsShared = (conn != null);
		this.config = config;
		this.tableName = TableName.valueOf(tableName);
		this.create = create;
		this.splitBits = splitBits;
		this.pushStrategy = pushStrategy;
		this.evaluationTimeout = evaluationTimeout;
		this.elasticIndexURL = elasticIndexURL;
		this.ticker = ticker;
		this.connFactory = connFactory;
		this.federatedServiceResolver = fsr;
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
			return hConnection.getTable(tableName);
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

		try (Table table = HalyardTableUtils.getTable(hConnection, tableName.getNameAsString(), create, splitBits)) {
			this.valueIO = IdentifiableValueIO.create(table);
		} catch (IOException e) {
			throw new SailException(e);
		}
		this.valueFactory = new IdValueFactory(valueIO);

		if (includeNamespaces) {
			addNamespaces();
		}

		statsConnection = getConnection();
		statistics = new HalyardEvaluationStatistics(new HalyardStatsBasedStatementPatternCardinalityCalculator(new SailConnectionTripleSource(statsConnection, false, getValueFactory()), valueIO), service -> {
			HalyardEvaluationStatistics fedStats = null;
			FederatedService fedServ = federatedServiceResolver.getService(service);
			if (fedServ instanceof SailFederatedService) {
				Sail sail = ((SailFederatedService) fedServ).getSail();
				if (sail instanceof HBaseSail) {
					fedStats = ((HBaseSail) sail).statistics;
				}
			}
			return fedStats;
		});

		registerSpinParsingFunctions();
		registerSpinParsingTupleFunctions();
    }

	private void addNamespaces() {
		try (SailConnection conn = getConnection()) {
			boolean nsExists = conn.hasStatement(HALYARD.SYSTEM_GRAPH_CONTEXT, RDF.TYPE, SD.GRAPH_CLASS, false, HALYARD.SYSTEM_GRAPH_CONTEXT);
			if (!nsExists) {
				for (Namespace ns : valueIO.getWellKnownNamespaces()) {
					conn.setNamespace(ns.getPrefix(), ns.getName());
				}
				conn.addStatement(HALYARD.SYSTEM_GRAPH_CONTEXT, RDF.TYPE, SD.GRAPH_CLASS, HALYARD.SYSTEM_GRAPH_CONTEXT);
			}
		}
	}

	private void registerSpinParsingFunctions() {
		if (!(functionRegistry.get(FN.CONCAT.stringValue()).get() instanceof org.eclipse.rdf4j.spin.function.Concat)) {
			functionRegistry.add(new org.eclipse.rdf4j.spin.function.Concat());
		}
		if (!functionRegistry.has(SPIN.EVAL_FUNCTION.stringValue())) {
			functionRegistry.add(new EvalFunction(spinParser));
		}
		if (!functionRegistry.has(SPIN.ASK_FUNCTION.stringValue())) {
			functionRegistry.add(new AskFunction(spinParser));
		}
		if (!functionRegistry.has(SPIF.CONVERT_SPIN_RDF_TO_STRING_FUNCTION.stringValue())) {
			functionRegistry.add(new ConvertSpinRDFToString(spinParser));
		}
		if (!functionRegistry.has(SPIF.CAN_INVOKE_FUNCTION.stringValue())) {
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

	public FunctionRegistry getFunctionRegistry() {
		return functionRegistry;
	}

	public void setFunctionRegistry(FunctionRegistry registry) {
		this.functionRegistry = registry;
	}

	public TupleFunctionRegistry getTupleFunctionRegistry() {
		return tupleFunctionRegistry;
	}

	public void setTupleFunctionRegistry(TupleFunctionRegistry registry) {
		this.tupleFunctionRegistry = registry;
	}

	public FederatedServiceResolver getFederatedServiceResolver() {
		return federatedServiceResolver;
	}

	public void setFederatedServiceResolver(FederatedServiceResolver resolver) {
		this.federatedServiceResolver = resolver;
	}

	public void addQueryContextInitializer(QueryContextInitializer initializer) {
		this.queryContextInitializers.add(initializer);
	}

	protected List<QueryContextInitializer> getQueryContextInitializers() {
		return this.queryContextInitializers;
	}

	public SpinParser getSpinParser() {
		return spinParser;
	}

	public void setSpinParser(SpinParser parser) {
		this.spinParser = parser;
	}

	public IdentifiableValueIO getValueIO() {
		return valueIO;
	}

    @Override
    public void shutDown() throws SailException { //release resources
		if (statsConnection == null) {
			throw new IllegalStateException("Sail has not been initialized");
		}
		try {
			statsConnection.close();
		} catch (SailException ignore) {
		}
		if (!hConnectionIsShared) {
			if (federatedServiceResolver instanceof AbstractFederatedServiceResolver) {
				((AbstractFederatedServiceResolver) federatedServiceResolver).shutDown();
			}

			if (hConnection != null) {
				try {
					hConnection.close();
					hConnection = null;
				} catch (IOException e) {
					throw new SailException(e);
				}
			}
		}
    }

    @Override
    public boolean isWritable() throws SailException {
		long time = System.currentTimeMillis();
		if (readOnlyTimestamp + STATUS_CACHING_TIMEOUT < time) {
			try (Table table = getTable()) {
				readOnly = table.getDescriptor().isReadOnly();
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
		if (valueFactory == null) {
			throw new IllegalStateException("Sail is not initialized");
		}
		return valueFactory;
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
