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
import com.msd.gin.halyard.common.SSLSettings;
import com.msd.gin.halyard.common.StatementIndices;
import com.msd.gin.halyard.function.DynamicFunctionRegistry;
import com.msd.gin.halyard.optimizers.HalyardEvaluationStatistics;
import com.msd.gin.halyard.optimizers.StatementPatternCardinalityCalculator;
import com.msd.gin.halyard.spin.SpinFunctionInterpreter;
import com.msd.gin.halyard.spin.SpinMagicPropertyInterpreter;
import com.msd.gin.halyard.spin.SpinParser;
import com.msd.gin.halyard.spin.SpinParser.Input;
import com.msd.gin.halyard.vocab.HALYARD;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.net.ssl.SSLContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.eclipse.rdf4j.common.transaction.IsolationLevel;
import org.eclipse.rdf4j.common.transaction.IsolationLevels;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryContextInitializer;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.AbstractFederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;

import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

/**
 * HBaseSail is the RDF Storage And Inference Layer (SAIL) implementation on top of Apache HBase.
 * It implements the interfaces - {@code Sail, SailConnection} and {@code FederatedServiceResolver}. Currently federated queries are
 * only supported for queries across multiple graphs in one Halyard database.
 * @author Adam Sotona (MSD)
 */
public class HBaseSail implements Sail, HBaseSailMXBean {

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
		HBaseSailConnection createConnection(HBaseSail sail) throws IOException;
	}

	public static final class ElasticSettings {
		String protocol;
		String host;
		int port;
		String username;
		String password;
		String indexName;
		SSLSettings sslSettings;

		public String getProtocol() {
			return protocol;
		}

		public String getHost() {
			return host;
		}

		public int getPort() {
			return port;
		}

		public String getIndexName() {
			return indexName;
		}

		static ElasticSettings from(URL esIndexUrl) {
			if (esIndexUrl == null) {
				return null;
			}
			ElasticSettings settings = new ElasticSettings();
			settings.protocol = esIndexUrl.getProtocol();
			settings.host = esIndexUrl.getHost();
			settings.port = esIndexUrl.getPort();
			settings.indexName = esIndexUrl.getPath().substring(1);
			return settings;
		}
	}

	public static final class ScanSettings {
		long minTimestamp = 0;
		long maxTimestamp = Long.MAX_VALUE;
		int maxVersions = 1;

		public long getMinTimestamp() {
			return minTimestamp;
		}

		public long getMaxTimestamp() {
			return maxTimestamp;
		}

		public int getMaxVersions() {
			return maxVersions;
		}
	}

	public static final class QueryInfo {
		private final long timestamp = System.currentTimeMillis();
		private final String queryString;
		private final TupleExpr queryExpr;
		private final TupleExpr optimizedExpr;

		public QueryInfo(String queryString, TupleExpr queryExpr, TupleExpr optimizedExpr) {
			this.queryString = queryString;
			this.queryExpr = queryExpr;
			this.optimizedExpr = optimizedExpr;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public String getQueryString() {
			return queryString;
		}

		public String getQueryTree() {
			return queryExpr.toString();
		}

		public String getOptimizedQueryTree() {
			return optimizedExpr.toString();
		}

		@Override
		public String toString() {
			return "Query: " + queryString + "\nTree:\n" + queryExpr + "\nOptimized:\n" + optimizedExpr;
		}
	}

	private static final long STATUS_CACHING_TIMEOUT = 60000l;

    private final Configuration config; //the configuration of the HBase database
	final TableName tableName;
	final String snapshotName;
	final Path snapshotRestorePath;
	final boolean create;
    final boolean pushStrategy;
	final int splitBits;
	protected HalyardEvaluationStatistics statistics;
	final int evaluationTimeoutSecs;
	private boolean readOnly = true;
	private long readOnlyTimestamp = 0L;
	final ElasticSettings esSettings;
	ElasticsearchTransport esTransport;
	boolean includeNamespaces = false;
	private boolean trackResultSize;
	private boolean trackResultTime;
    final Ticker ticker;
	private FederatedServiceResolver federatedServiceResolver;
	private RDFFactory rdfFactory;
	private StatementIndices stmtIndices;
	private ValueFactory valueFactory;
	private final FunctionRegistry functionRegistry = new DynamicFunctionRegistry();
	private final TupleFunctionRegistry tupleFunctionRegistry = TupleFunctionRegistry.getInstance();
	private final SpinParser spinParser = new SpinParser(Input.TEXT_FIRST, functionRegistry, tupleFunctionRegistry);
	private final List<QueryContextInitializer> queryContextInitializers = new ArrayList<>();
	private final ScanSettings scanSettings = new ScanSettings();
	final SailConnectionFactory connFactory;
	Connection hConnection;
	final boolean hConnectionIsShared; //whether a Connection is provided or we need to create our own
	Keyspace keyspace;
	String owner;
	ObjectInstance mxInst;
	private final Queue<QueryInfo> queryHistory = new ArrayBlockingQueue<>(10, true);


	HBaseSail(Configuration config, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, ElasticSettings elasticSettings) {
		this(null, config, tableName, create, splitBits, pushStrategy, evaluationTimeout, elasticSettings, null, HBaseSailConnection.Factory.INSTANCE,
				new HBaseFederatedServiceResolver(null, config, tableName, evaluationTimeout, null));
	}

	/**
	 * Construct HBaseSail for a table.
	 * 
	 * @param conn
	 * @param config
	 * @param tableName
	 * @param create
	 * @param splitBits
	 * @param pushStrategy
	 * @param evaluationTimeout
	 * @param elasticSettings
	 * @param ticker
	 * @param connFactory
	 * @param fsr
	 */
	HBaseSail(@Nullable Connection conn, Configuration config, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, ElasticSettings elasticSettings, Ticker ticker, SailConnectionFactory connFactory,
			FederatedServiceResolver fsr) {
		this.hConnection = conn;
		this.hConnectionIsShared = (conn != null);
		this.config = Objects.requireNonNull(config);
		this.tableName = TableName.valueOf(tableName);
		this.create = create;
		this.splitBits = splitBits;
		this.snapshotName = null;
		this.snapshotRestorePath = null;
		this.pushStrategy = pushStrategy;
		this.evaluationTimeoutSecs = evaluationTimeout;
		this.esSettings = elasticSettings;
		this.ticker = ticker;
		this.connFactory = connFactory;
		this.federatedServiceResolver = fsr;
	}

	public HBaseSail(Configuration config, String snapshotName, String snapshotRestorePath, boolean pushStrategy, int evaluationTimeout, URL elasticIndexURL) {
		this(config, snapshotName, snapshotRestorePath, pushStrategy, evaluationTimeout, ElasticSettings.from(elasticIndexURL));
	}

	HBaseSail(Configuration config, String snapshotName, String snapshotRestorePath, boolean pushStrategy, int evaluationTimeout, ElasticSettings elasticSettings) {
		this(config, snapshotName, snapshotRestorePath, pushStrategy, evaluationTimeout, elasticSettings, null, HBaseSailConnection.Factory.INSTANCE, new HBaseFederatedServiceResolver(null, config, null, evaluationTimeout, null));
	}

	/**
	 * Construct HBaseSail for a snapshot.
	 * 
	 * @param conn
	 * @param config
	 * @param tableName
	 * @param create
	 * @param splitBits
	 * @param pushStrategy
	 * @param evaluationTimeout
	 * @param elasticSettings
	 * @param ticker
	 * @param connFactory
	 * @param fsr
	 */
	HBaseSail(Configuration config, String snapshotName, String snapshotRestorePath, boolean pushStrategy, int evaluationTimeout, ElasticSettings elasticSettings, Ticker ticker, SailConnectionFactory connFactory,
			FederatedServiceResolver fsr) {
		this.hConnection = null;
		this.hConnectionIsShared = false;
		this.config = Objects.requireNonNull(config);
		this.tableName = null;
		this.create = false;
		this.splitBits = -1;
		this.snapshotName = snapshotName;
		this.snapshotRestorePath = new Path(snapshotRestorePath);
		this.pushStrategy = pushStrategy;
		this.evaluationTimeoutSecs = evaluationTimeout;
		this.esSettings = elasticSettings;
		this.ticker = ticker;
		this.connFactory = connFactory;
		this.federatedServiceResolver = fsr;
	}

	private HBaseSail(@Nullable Connection conn, Configuration config, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, URL elasticIndexURL, Ticker ticker, SailConnectionFactory connFactory) {
		this(conn, config, tableName, create, splitBits, pushStrategy, evaluationTimeout, ElasticSettings.from(elasticIndexURL), ticker, connFactory,
				new HBaseFederatedServiceResolver(conn, config, tableName, evaluationTimeout, null));
	}

	public HBaseSail(@Nonnull Connection conn, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, URL elasticIndexURL, Ticker ticker) {
		this(conn, conn.getConfiguration(), tableName, create, splitBits, pushStrategy, evaluationTimeout, elasticIndexURL, ticker, HBaseSailConnection.Factory.INSTANCE);
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
    public HBaseSail(Configuration config, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, URL elasticIndexURL, Ticker ticker, SailConnectionFactory connFactory) {
		this(null, config, tableName, create, splitBits, pushStrategy, evaluationTimeout, elasticIndexURL, ticker, connFactory);
    }

	public HBaseSail(Configuration config, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, URL elasticIndexURL, Ticker ticker) {
		this(config, tableName, create, splitBits, pushStrategy, evaluationTimeout, elasticIndexURL, ticker, HBaseSailConnection.Factory.INSTANCE);
	}

	@Override
	public boolean isPushStrategyEnabled() {
		return pushStrategy;
	}

	@Override
	public int getEvaluationTimeout() {
		return evaluationTimeoutSecs;
	}

	@Override
	public ElasticSettings getElasticSettings() {
		return esSettings;
	}

	@Override
	public int getValueIdentifierSize() {
		return rdfFactory.getIdSize();
	}

	@Override
	public String getValueIdentifierAlgorithm() {
		return rdfFactory.getIdAlgorithm();
	}

	@Override
	public ScanSettings getScanSettings() {
		return scanSettings;
	}

	@Override
	public boolean isTrackResultSize() {
		return trackResultSize;
	}

	@Override
	public void setTrackResultSize(boolean f) {
		trackResultSize = f;
	}

	@Override
	public boolean isTrackResultTime() {
		return trackResultTime;
	}

	@Override
	public void setTrackResultTime(boolean f) {
		trackResultTime = f;
	}

	@Override
	public QueryInfo[] getRecentQueries() {
		return queryHistory.toArray(new QueryInfo[queryHistory.size()]);
	}

	String getFederatedServiceResolverName() {
		return federatedServiceResolver.getClass().getSimpleName() + "@" + Integer.toHexString(federatedServiceResolver.hashCode());
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

	void trackQuery(String sourceString, TupleExpr rawExpr, TupleExpr optimizedExpr) {
		QueryInfo query = new QueryInfo(sourceString, rawExpr, optimizedExpr);
		while (!queryHistory.offer(query)) {
			queryHistory.remove();
		}
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

	@Override
	public void init() throws SailException {
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
		stmtIndices = new StatementIndices(config, rdfFactory);
		valueFactory = IdValueFactory.INSTANCE;

		if (includeNamespaces) {
			addNamespaces();
		}

		StatementPatternCardinalityCalculator.Factory spcalcFactory = () -> new HalyardStatsBasedStatementPatternCardinalityCalculator(
				new HBaseTripleSource(keyspace.getConnection(), valueFactory, stmtIndices, evaluationTimeoutSecs), rdfFactory);
		HalyardEvaluationStatistics.ServiceStatsProvider srvStatsProvider = service -> {
			HalyardEvaluationStatistics fedStats = null;
			FederatedService fedServ = federatedServiceResolver.getService(service);
			if (fedServ instanceof SailFederatedService) {
				Sail sail = ((SailFederatedService) fedServ).getSail();
				if (sail instanceof HBaseSail) {
					fedStats = ((HBaseSail) sail).statistics;
				}
			}
			return fedStats;
		};
		statistics = new HalyardEvaluationStatistics(spcalcFactory, srvStatsProvider);

		SpinFunctionInterpreter.registerSpinParsingFunctions(spinParser, functionRegistry, pushStrategy ? tupleFunctionRegistry : TupleFunctionRegistry.getInstance());
		SpinMagicPropertyInterpreter.registerSpinParsingTupleFunctions(spinParser, tupleFunctionRegistry);

		if (esSettings != null) {
			RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(esSettings.host, esSettings.port != -1 ? esSettings.port : 9200, esSettings.protocol));
			CredentialsProvider esCredentialsProvider;
			if (esSettings.password != null) {
				esCredentialsProvider = new BasicCredentialsProvider();
				esCredentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(esSettings.username, esSettings.password));
			} else {
				esCredentialsProvider = null;
			}
			SSLContext sslContext;
			if (esSettings.sslSettings != null) {
				try {
					sslContext = esSettings.sslSettings.createSSLContext();
				} catch (IOException | GeneralSecurityException e) {
					throw new SailException(e);
				}
			} else {
				sslContext = null;
			}
			restClientBuilder.setHttpClientConfigCallback(new HttpClientConfigCallback() {
				@Override
				public HttpAsyncClientBuilder customizeHttpClient(
						HttpAsyncClientBuilder httpClientBuilder) {
					if (esCredentialsProvider != null) {
						httpClientBuilder.setDefaultCredentialsProvider(esCredentialsProvider);
					}
					if (sslContext != null) {
						httpClientBuilder.setSSLContext(sslContext);
					}
					return httpClientBuilder;
				}
			});
			RestClient restClient = restClientBuilder.build();
			esTransport = new RestClientTransport(restClient, new JacksonJsonpMapper());
		}

		Hashtable<String, String> attrs = new Hashtable<>();
		attrs.put("type", getClass().getName());
		attrs.put("id", Integer.toString(hashCode()));
		if (tableName != null) {
			attrs.put("table", tableName.getNameAsString());
		} else {
			attrs.put("snapshot", snapshotName);
		}
		if (owner != null) {
			attrs.put("owner", owner);
		}
		attrs.put("federatedServiceResolver", getFederatedServiceResolverName());
		try {
			mxInst = ManagementFactory.getPlatformMBeanServer().registerMBean(this, ObjectName.getInstance("com.msd.gin.halyard", attrs));
		} catch (InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException | MalformedObjectNameException e) {
			throw new AssertionError(e);
		}
	}

	private boolean isInitialized() {
		return (keyspace != null) && (rdfFactory != null) && (stmtIndices != null);
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

	public Configuration getConfiguration() {
		return config;
	}

	public FunctionRegistry getFunctionRegistry() {
		return functionRegistry;
	}

	public TupleFunctionRegistry getTupleFunctionRegistry() {
		return tupleFunctionRegistry;
	}

	public FederatedServiceResolver getFederatedServiceResolver() {
		return federatedServiceResolver;
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

	public RDFFactory getRDFFactory() {
		if (rdfFactory == null) {
			throw new IllegalStateException("Sail is not initialized");
		}
		return rdfFactory;
	}

	public StatementIndices getStatementIndices() {
		if (stmtIndices == null) {
			throw new IllegalStateException("Sail is not initialized");
		}
		return stmtIndices;
	}

    @Override
    public void shutDown() throws SailException { //release resources
		if (mxInst != null) {
			try {
				ManagementFactory.getPlatformMBeanServer().unregisterMBean(mxInst.getObjectName());
			} catch (MBeanRegistrationException | InstanceNotFoundException e) {
			}
			mxInst = null;
		}

		if (esTransport != null) {
			try {
				esTransport.close();
			} catch (IOException ignore) {
			}
			esTransport = null;
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
		HBaseSailConnection conn;
		try {
			conn = connFactory.createConnection(this);
		} catch (IOException ioe) {
			throw new SailException(ioe);
		}
		conn.trackResultSize = trackResultSize;
		conn.trackResultTime = trackResultTime;
		return conn;
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
