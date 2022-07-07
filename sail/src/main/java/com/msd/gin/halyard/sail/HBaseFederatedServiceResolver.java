package com.msd.gin.halyard.sail;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.sail.HBaseSail.Ticker;
import com.msd.gin.halyard.vocab.HALYARD;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.repository.sparql.federation.SPARQLServiceResolver;

public class HBaseFederatedServiceResolver extends SPARQLServiceResolver
{
	private static final String MIN_TIMESTAMP_QUERY_PARAM = "minTimestamp";
	private static final String MAX_TIMESTAMP_QUERY_PARAM = "maxTimestamp";
	private static final String MAX_VERSIONS_QUERY_PARAM = "maxVersions";

	private final Connection hConnection;
	private final Configuration config;
	private final String defaultTableName;
	private final int evaluationTimeout;
	private final Ticker ticker;

	private final Cache<String, SailFederatedService> federatedServices = CacheBuilder.newBuilder().maximumSize(10).removalListener((evt) -> ((SailFederatedService) evt.getValue()).shutdown()).build();

	/**
	 * Federated service resolver that supports querying other HBase tables.
	 * 
	 * @param conn
	 * @param config
	 * @param defaultTableName default table name to use (if any) if not specified in SERVICE URL.
	 * @param evaluationTimeout
	 * @param ticker
	 */
	public HBaseFederatedServiceResolver(@Nullable Connection conn, Configuration config, @Nullable String defaultTableName, int evaluationTimeout, @Nullable Ticker ticker) {
		this.hConnection = conn;
		this.config = config;
		this.defaultTableName = defaultTableName;
		this.evaluationTimeout = evaluationTimeout;
		this.ticker = ticker;
	}

	@Override
	public FederatedService getService(String serviceUrl) throws QueryEvaluationException {
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

			SailFederatedService federatedService;
			try {
				federatedService = federatedServices.get(serviceUrl, () -> {
					HBaseSail sail = new HBaseSail(hConnection, config, federatedTable, false, 0, true, evaluationTimeout, null, ticker, HBaseSailConnection.Factory.INSTANCE, this);
					for (NameValuePair nvp : queryParams) {
						switch (nvp.getName()) {
							case MIN_TIMESTAMP_QUERY_PARAM:
								sail.scanSettings.minTimestamp = HalyardTableUtils.toHalyardTimestamp(Long.parseLong(nvp.getValue()), false);
								break;
							case MAX_TIMESTAMP_QUERY_PARAM:
								sail.scanSettings.maxTimestamp = HalyardTableUtils.toHalyardTimestamp(Long.parseLong(nvp.getValue()), false);
								break;
							case MAX_VERSIONS_QUERY_PARAM:
								sail.scanSettings.maxVersions = Integer.parseInt(nvp.getValue());
								break;
						}
					}
					SailFederatedService sailFedService = new SailFederatedService(sail);
					sailFedService.initialize();
					return sailFedService;
				});
			} catch (ExecutionException e) {
				if (e.getCause() instanceof RuntimeException) {
					throw (RuntimeException) e.getCause();
				} else {
					throw new AssertionError(e.getClass());
				}
			}
			return federatedService;
		} else {
			return super.getService(serviceUrl);
		}
	}

	@Override
	public void shutDown() {
		super.shutDown();
		federatedServices.invalidateAll(); // release the references to the services
	}
}
