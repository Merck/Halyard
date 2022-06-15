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
import com.msd.gin.halyard.common.Keyspace;
import com.msd.gin.halyard.common.KeyspaceConnection;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.function.DynamicFunctionRegistry;
import com.msd.gin.halyard.optimizers.HalyardEvaluationStatistics;
import com.msd.gin.halyard.sail.spin.SpinFunctionInterpreter;
import com.msd.gin.halyard.sail.spin.SpinMagicPropertyInterpreter;
import com.msd.gin.halyard.vocab.HALYARD;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.eclipse.rdf4j.IsolationLevel;
import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SD;
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
	public interface SailConnectionFactory {
		SailConnection createConnection(HBaseSail sail) throws IOException;
	}

	static final class ScanSettings {
		long minTimestamp = 0;
		long maxTimestamp = Long.MAX_VALUE;
		int maxVersions = 1;
	}

	private static final long STATUS_CACHING_TIMEOUT = 60000l;

    private final Configuration config; //the configuration of the HBase database
	final TableName tableName;
	final String snapshotName;
	final Path snapshotRestorePath;
	final boolean create;
    final boolean pushStrategy;
	final int splitBits;
	private SailConnection statsConnection;
	protected HalyardEvaluationStatistics statistics;
	final int evaluationTimeout; // secs
	private boolean readOnly = true;
	private long readOnlyTimestamp = 0L;
    final String elasticIndexURL;
	boolean includeNamespaces = false;
    final Ticker ticker;
	private FederatedServiceResolver federatedServiceResolver;
	private RDFFactory rdfFactory;
	private ValueFactory valueFactory;
	private FunctionRegistry functionRegistry = new DynamicFunctionRegistry();
	private TupleFunctionRegistry tupleFunctionRegistry = TupleFunctionRegistry.getInstance();
	private SpinParser spinParser = new SpinParser();
	private final List<QueryContextInitializer> queryContextInitializers = new ArrayList<>();
	ScanSettings scanSettings = new ScanSettings();
	final SailConnectionFactory connFactory;
	Connection hConnection;
	final boolean hConnectionIsShared; //whether a Connection is provided or we need to create our own
	Keyspace keyspace;


	private HBaseSail(Connection conn, Configuration config, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, String elasticIndexURL, Ticker ticker, SailConnectionFactory connFactory) {
		this(conn, config, tableName, create, splitBits, pushStrategy, evaluationTimeout, elasticIndexURL, ticker, connFactory, new HBaseFederatedServiceResolver(conn, config, tableName, evaluationTimeout, ticker));
    }

	HBaseSail(Connection conn, Configuration config, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, String elasticIndexURL, Ticker ticker, SailConnectionFactory connFactory,
			FederatedServiceResolver fsr) {
		this.hConnection = conn;
		this.hConnectionIsShared = (conn != null);
		this.config = config;
		this.tableName = TableName.valueOf(tableName);
		this.create = create;
		this.splitBits = splitBits;
		this.snapshotName = null;
		this.snapshotRestorePath = null;
		this.pushStrategy = pushStrategy;
		this.evaluationTimeout = evaluationTimeout;
		this.elasticIndexURL = elasticIndexURL;
		this.ticker = ticker;
		this.connFactory = connFactory;
		this.federatedServiceResolver = fsr;
	}

	HBaseSail(Configuration config, String snapshotName, String snapshotRestorePath, boolean pushStrategy, int evaluationTimeout, String elasticIndexURL, Ticker ticker, SailConnectionFactory connFactory, FederatedServiceResolver fsr) {
		this.hConnection = null;
		this.hConnectionIsShared = false;
		this.config = config;
		this.tableName = null;
		this.create = false;
		this.splitBits = -1;
		this.snapshotName = snapshotName;
		this.snapshotRestorePath = new Path(snapshotRestorePath);
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
	 * @param connFactory {@link SailConnectionFactory} for creating connections
	 */
    public HBaseSail(Configuration config, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, String elasticIndexURL, Ticker ticker, SailConnectionFactory connFactory) {
    	this(null, config, tableName, create, splitBits, pushStrategy, evaluationTimeout, elasticIndexURL, ticker, connFactory);
    }

    public HBaseSail(Connection conn, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, String elasticIndexURL, Ticker ticker) {
		this(conn, conn.getConfiguration(), tableName, create, splitBits, pushStrategy, evaluationTimeout, elasticIndexURL, ticker, HBaseSailConnection.Factory.INSTANCE);
	}

    public HBaseSail(Configuration config, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, String elasticIndexURL, Ticker ticker) {
		this(null, config, tableName, create, splitBits, pushStrategy, evaluationTimeout, elasticIndexURL, ticker, HBaseSailConnection.Factory.INSTANCE);
	}

	public HBaseSail(Configuration config, String snapshotName, String snapshotRestorePath, boolean pushStrategy, int evaluationTimeout, String elasticIndexURL, Ticker ticker) {
		this(config, snapshotName, snapshotRestorePath, pushStrategy, evaluationTimeout, elasticIndexURL, ticker, HBaseSailConnection.Factory.INSTANCE, null);
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

	BufferedMutator getBufferedMutator() {
		if (hConnection == null) {
			throw new SailException("Snapshots are not modifiable");
		}
		try {
			return hConnection.getBufferedMutator(tableName);
		} catch (IOException e) {
			throw new SailException(e);
		}
	}

	@Override
	public void initialize() throws SailException {
		try {
			if (tableName != null) {
				if (!hConnectionIsShared) {
					// connections are thread-safe and very heavyweight - only do it once
					if (hConnection != null) {
						throw new IllegalStateException("Sail has already been initialized");
					}
					hConnection = HalyardTableUtils.getConnection(config);
				}
				if (create) {
					HalyardTableUtils.createTableIfNotExists(hConnection, tableName, splitBits);
				}
			}

			keyspace = HalyardTableUtils.getKeyspace(config, hConnection, tableName, snapshotName, snapshotRestorePath);
			try (KeyspaceConnection keyspaceConn = keyspace.getConnection()) {
				rdfFactory = RDFFactory.create(keyspaceConn);
			}
		} catch (IOException e) {
			throw new SailException(e);
		}
		this.valueFactory = IdValueFactory.INSTANCE;

		if (includeNamespaces) {
			addNamespaces();
		}

		statsConnection = getConnection();
		statistics = new HalyardEvaluationStatistics(new HalyardStatsBasedStatementPatternCardinalityCalculator(new SailConnectionTripleSource(statsConnection, false, valueFactory), rdfFactory), service -> {
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

		SpinFunctionInterpreter.registerSpinParsingFunctions(spinParser, pushStrategy ? functionRegistry : FunctionRegistry.getInstance(), pushStrategy ? tupleFunctionRegistry : TupleFunctionRegistry.getInstance());
		SpinMagicPropertyInterpreter.registerSpinParsingTupleFunctions(spinParser, pushStrategy ? tupleFunctionRegistry : TupleFunctionRegistry.getInstance());
    }

	private boolean isInitialized() {
		return (keyspace != null) && (rdfFactory != null);
	}

	private void addNamespaces() {
		try (SailConnection conn = getConnection()) {
			boolean nsExists = conn.hasStatement(HALYARD.SYSTEM_GRAPH_CONTEXT, RDF.TYPE, SD.GRAPH_CLASS, false, HALYARD.SYSTEM_GRAPH_CONTEXT);
			if (!nsExists) {
				for (Namespace ns : rdfFactory.getWellKnownNamespaces()) {
					conn.setNamespace(ns.getPrefix(), ns.getName());
				}
				conn.addStatement(HALYARD.SYSTEM_GRAPH_CONTEXT, RDF.TYPE, SD.GRAPH_CLASS, HALYARD.SYSTEM_GRAPH_CONTEXT);
			}
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

	public RDFFactory getRDFFactory() {
		return rdfFactory;
	}

    @Override
    public void shutDown() throws SailException { //release resources
		if (statsConnection != null) {
			try {
				statsConnection.close();
			} catch (SailException ignore) {
			}
			statsConnection = null;
		}
		if (!hConnectionIsShared) {
			if (federatedServiceResolver instanceof AbstractFederatedServiceResolver) {
				((AbstractFederatedServiceResolver) federatedServiceResolver).shutDown();
			}

			if (hConnection != null) {
				try {
					hConnection.close();
				} catch (IOException ignore) {
				}
				hConnection = null;
			}
		}
		if (keyspace != null) {
			try {
				keyspace.destroy();
			} catch (IOException ignore) {
			}
			keyspace = null;
		}
    }

    @Override
    public boolean isWritable() throws SailException {
		if (hConnection != null) {
			long time = System.currentTimeMillis();
			if ((readOnlyTimestamp == 0) || (time > readOnlyTimestamp + STATUS_CACHING_TIMEOUT)) {
				try (Table table = hConnection.getTable(tableName)) {
					readOnly = table.getDescriptor().isReadOnly();
					readOnlyTimestamp = time;
				} catch (IOException ex) {
					throw new SailException(ex);
				}
			}
		}
        return !readOnly;
    }

    @Override
	public SailConnection getConnection() throws SailException {
		if (!isInitialized()) {
			throw new IllegalStateException("Sail is not initialized or has been shut down");
		}
		try {
			return connFactory.createConnection(this);
		} catch (IOException ioe) {
			throw new SailException(ioe);
		}
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
