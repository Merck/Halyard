/*
 * Copyright 2019 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
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
package com.msd.gin.halyard.repository;

import com.msd.gin.halyard.common.Config;
import com.msd.gin.halyard.sail.HBaseSail;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.http.client.HttpClient;
import org.eclipse.rdf4j.http.client.HttpClientDependent;
import org.eclipse.rdf4j.http.client.SessionManagerDependent;
import org.eclipse.rdf4j.http.client.SharedHttpClientSessionManager;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolverClient;
import org.eclipse.rdf4j.repository.DelegatingRepository;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryResolverClient;
import org.eclipse.rdf4j.repository.config.DelegatingRepositoryImplConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.repository.config.RepositoryConfigUtil;
import org.eclipse.rdf4j.repository.config.RepositoryFactory;
import org.eclipse.rdf4j.repository.config.RepositoryImplConfig;
import org.eclipse.rdf4j.repository.config.RepositoryRegistry;
import org.eclipse.rdf4j.repository.manager.RepositoryInfo;
import org.eclipse.rdf4j.repository.manager.RepositoryManager;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sparql.federation.SPARQLServiceResolver;

/**
 *
 * @author Adam Sotona (MSD)
 */
public final class HBaseRepositoryManager extends RepositoryManager {
    private static final String SYSTEM_REPO_ID = "RDF4JSYSTEM";
	private static final String SYSTEM_ID = "SYSTEM";
    private volatile SharedHttpClientSessionManager client;
    private volatile SPARQLServiceResolver serviceResolver;
    private volatile Configuration config = HBaseConfiguration.create();

    public HBaseRepositoryManager(Object...anyArgs) {
    }

    void overrideConfiguration(Configuration config) {
        this.config = config;
    }

    protected Repository createSystemRepository() throws RepositoryException {
		config.set(Config.ID_HASH, "Murmur3-128");
        SailRepository repo = new SailRepository(new HBaseSail(config, SYSTEM_REPO_ID, true, 0, true, 180, null, null));
        repo.init();
        return repo;
    }

    @Override
    public URL getLocation() throws MalformedURLException {
        throw new MalformedURLException();
    }

    private SharedHttpClientSessionManager getSesameClient() {
        SharedHttpClientSessionManager result = client;
        if (result == null) {
            synchronized (this) {
                result = client;
                if (result == null) {
                    result = client = new SharedHttpClientSessionManager();
                }
            }
        }
        return result;
    }

    @Override
    public HttpClient getHttpClient() {
        SharedHttpClientSessionManager nextClient = client;
        if (nextClient == null) {
            return null;
        } else {
            return nextClient.getHttpClient();
        }
    }

    @Override
    public void setHttpClient(HttpClient httpClient) {
        getSesameClient().setHttpClient(httpClient);
    }

    private FederatedServiceResolver getFederatedServiceResolver() {
        SPARQLServiceResolver result = serviceResolver;
        if (result == null) {
            synchronized (this) {
                result = serviceResolver;
                if (result == null) {
                    result = serviceResolver = new SPARQLServiceResolver();
                    result.setHttpClientSessionManager(getSesameClient());
                }
            }
        }
        return result;
    }

    @Override
    public void shutDown() {
        try {
            super.shutDown();
        } finally {
            try {
                SPARQLServiceResolver toCloseServiceResolver = serviceResolver;
                serviceResolver = null;
                if (toCloseServiceResolver != null) {
                    toCloseServiceResolver.shutDown();
                }
            } finally {
                SharedHttpClientSessionManager toCloseClient = client;
                client = null;
                if (toCloseClient != null) {
                    toCloseClient.shutDown();
                }
            }
        }
    }

    @Override
    protected Repository createRepository(String id) throws RepositoryConfigException, RepositoryException {
        Repository repository = null;
        RepositoryConfig repConfig = getRepositoryConfig(id);
        if (repConfig != null) {
            repConfig.validate();
            repository = createRepositoryStack(repConfig.getRepositoryImplConfig());
            repository.init();
        }
        return repository;
    }

    private Repository createRepositoryStack(RepositoryImplConfig config) throws RepositoryConfigException {
        RepositoryFactory factory = RepositoryRegistry.getInstance()
            .get(config.getType())
            .orElseThrow(() -> new RepositoryConfigException("Unsupported repository type: " + config.getType()));
        Repository repository = factory.getRepository(config);
        if (repository instanceof RepositoryResolverClient) {
            ((RepositoryResolverClient) repository).setRepositoryResolver(this);
        }
        if (repository instanceof FederatedServiceResolverClient) {
            ((FederatedServiceResolverClient) repository).setFederatedServiceResolver(getFederatedServiceResolver());
        }
        if (repository instanceof SessionManagerDependent) {
            ((SessionManagerDependent) repository).setHttpClientSessionManager(client);
        } else if (repository instanceof HttpClientDependent) {
            ((HttpClientDependent) repository).setHttpClient(getHttpClient());
        }
        if (config instanceof DelegatingRepositoryImplConfig) {
            RepositoryImplConfig delegateConfig = ((DelegatingRepositoryImplConfig) config).getDelegate();
            Repository delegate = createRepositoryStack(delegateConfig);
            try {
                ((DelegatingRepository) repository).setDelegate(delegate);
            } catch (ClassCastException e) {
                throw new RepositoryConfigException("Delegate specified for repository that is not a DelegatingRepository: " + delegate.getClass(), e);
            }
        }
        return repository;
    }

    @Override
    public RepositoryInfo getRepositoryInfo(String id) {
        RepositoryConfig config = getRepositoryConfig(id);
        if (config == null) {
            return null;
        }
        RepositoryInfo repInfo = new RepositoryInfo();
        repInfo.setId(config.getID());
        repInfo.setDescription(config.getTitle());
        repInfo.setReadable(true);
        repInfo.setWritable(true);
        return repInfo;
    }

    @Override
    @SuppressWarnings("deprecation")
    public synchronized List<RepositoryInfo> getAllRepositoryInfos(boolean skipSystemRepo) throws RepositoryException {
        List<RepositoryInfo> result = new ArrayList<>();
        for (String name : RepositoryConfigUtil.getRepositoryIDs(getSystemRepository())) {
            RepositoryInfo repInfo = getRepositoryInfo(name);
            if (!skipSystemRepo || !repInfo.getId().equals(SYSTEM_REPO_ID)) {
                result.add(repInfo);
            }
        }
        return result;
    }

    @Override
    public RepositoryConfig getRepositoryConfig(String repositoryID) throws RepositoryConfigException, RepositoryException {
		Repository systemRepository = getSystemRepository();
		if (systemRepository == null) {
			return null;
		} else {
			return RepositoryConfigUtil.getRepositoryConfig(systemRepository, repositoryID);
		}
	}

	public Repository getSystemRepository() {
		if (!isInitialized()) {
			throw new IllegalStateException("Repository Manager is not initialized");
		}
		synchronized (initializedRepositories) {
			Repository systemRepository = initializedRepositories.get(SYSTEM_ID);
			if (systemRepository != null && systemRepository.isInitialized()) {
				return systemRepository;
			}
			systemRepository = createSystemRepository();
			if (systemRepository != null) {
				initializedRepositories.put(SYSTEM_ID, systemRepository);
			}
			return systemRepository;
		}
	}

	@Override
	public void addRepositoryConfig(RepositoryConfig config) throws RepositoryException, RepositoryConfigException {
		Repository systemRepository = getSystemRepository();
		if (systemRepository != null && !SYSTEM_ID.equals(config.getID())) {
			RepositoryConfigUtil.updateRepositoryConfigs(systemRepository, config);
		}
	}

	@Override
	public Collection<RepositoryInfo> getAllRepositoryInfos() throws RepositoryException {
		return getAllRepositoryInfos(false);
	}
}
