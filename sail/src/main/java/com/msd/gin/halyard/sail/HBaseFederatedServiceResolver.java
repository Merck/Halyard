package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.federation.SailFederatedService;
import com.msd.gin.halyard.repository.HBaseRepositoryManager;
import com.msd.gin.halyard.sail.HBaseSail.Ticker;
import com.msd.gin.halyard.vocab.HALYARD;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.eclipse.rdf4j.model.util.Literals;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.repository.sparql.federation.RepositoryFederatedService;
import org.eclipse.rdf4j.repository.sparql.federation.SPARQLServiceResolver;

public class HBaseFederatedServiceResolver extends SPARQLServiceResolver
{
	private static final String MIN_TIMESTAMP_QUERY_PARAM = "minTimestamp";
	private static final String MAX_TIMESTAMP_QUERY_PARAM = "maxTimestamp";
	private static final String MAX_VERSIONS_QUERY_PARAM = "maxVersions";
	private static final String ENDPOINT_QUERY = "PREFIX halyard: <" + HALYARD.NAMESPACE + ">\n" + "SELECT ?user ?pass WHERE { GRAPH halyard:endpoints {?url halyard:username ?user; halyard:password ?pass} }";

	private static final HBaseRepositoryManager REPO_MANAGER = new HBaseRepositoryManager();

	private final Connection hConnection;
	private final Configuration config;
	private final String defaultTableName;
	private final boolean usePush;
	private final int evaluationTimeout;
	private final Ticker ticker;

	static String getName(FederatedServiceResolver federatedServiceResolver) {
		return federatedServiceResolver.getClass().getSimpleName() + "@" + Integer.toHexString(federatedServiceResolver.hashCode());
	}

	/**
	 * Federated service resolver that supports querying other HBase tables.
	 * 
	 * @param conn
	 * @param config
	 * @param defaultTableName default table name to use (if any) if not specified in SERVICE URL.
	 * @param usePush
	 * @param evaluationTimeout
	 * @param ticker
	 */
	public HBaseFederatedServiceResolver(@Nullable Connection conn, Configuration config, @Nullable String defaultTableName, boolean usePush, int evaluationTimeout, @Nullable Ticker ticker) {
		this.hConnection = conn;
		this.config = config;
		this.defaultTableName = defaultTableName;
		this.usePush = usePush;
		this.evaluationTimeout = evaluationTimeout;
		this.ticker = ticker;
		this.REPO_MANAGER.init();
	}

	@Override
	protected FederatedService createService(String serviceUrl) throws QueryEvaluationException {
		FederatedService federatedService;
		if (serviceUrl.startsWith(HALYARD.NAMESPACE)) {
			String path;
			List<NameValuePair> queryParams;
			int queryParamsPos = serviceUrl.lastIndexOf('?');
			if (queryParamsPos != -1) {
				path = serviceUrl.substring(HALYARD.NAMESPACE.length(), queryParamsPos);
				queryParams = URLEncodedUtils.parse(serviceUrl.substring(queryParamsPos + 1), StandardCharsets.UTF_8);
			} else {
				path = serviceUrl.substring(HALYARD.NAMESPACE.length());
				queryParams = Collections.emptyList();
			}

			final String federatedTable = !path.isEmpty() ? path : defaultTableName;
			if (federatedTable == null) {
				throw new QueryEvaluationException(String.format("Invalid SERVICE URL: %s", serviceUrl));
			}

			HBaseSail sail = new HBaseSail(hConnection, config, federatedTable, false, 0, usePush, evaluationTimeout, null, ticker);
			sail.owner = getName(this);
			HBaseSail.ScanSettings scanSettings = sail.getScanSettings();
			for (NameValuePair nvp : queryParams) {
				switch (nvp.getName()) {
					case MIN_TIMESTAMP_QUERY_PARAM:
						scanSettings.minTimestamp = HalyardTableUtils.toHalyardTimestamp(Long.parseLong(nvp.getValue()), false);
						break;
					case MAX_TIMESTAMP_QUERY_PARAM:
						scanSettings.maxTimestamp = HalyardTableUtils.toHalyardTimestamp(Long.parseLong(nvp.getValue()), false);
						break;
					case MAX_VERSIONS_QUERY_PARAM:
						scanSettings.maxVersions = Integer.parseInt(nvp.getValue());
						break;
				}
			}
			federatedService = new SailFederatedService(sail);
		} else {
			SPARQLRepository sparqlRepo = new SPARQLRepository(serviceUrl);
			sparqlRepo.setHttpClientSessionManager(getHttpClientSessionManager());
			try {
				URL url = new URL(serviceUrl);
				if (url.getUserInfo() == null) {
					// check for stored authentication info
					Repository systemRepo = REPO_MANAGER.getSystemRepository();
					try (RepositoryConnection conn = systemRepo.getConnection()) {
						TupleQuery query = conn.prepareTupleQuery(ENDPOINT_QUERY);
						query.setBinding("url", systemRepo.getValueFactory().createIRI(serviceUrl));
						try (TupleQueryResult res = query.evaluate()) {
							for (BindingSet bs : res) {
								String user = Literals.getLabel(bs.getValue("user"), null);
								if (user != null) {
									String pass = Literals.getLabel(bs.getValue("pass"), null);
									sparqlRepo.setUsernameAndPassword(user, pass);
								}
							}
						}
					}
				}
			} catch (MalformedURLException ioe) {
				// ignore
			}
			federatedService = new RepositoryFederatedService(sparqlRepo);
		}
		return federatedService;
	}

	@Override
	public void shutDown() {
		super.shutDown();
		REPO_MANAGER.shutDown();
	}
}
