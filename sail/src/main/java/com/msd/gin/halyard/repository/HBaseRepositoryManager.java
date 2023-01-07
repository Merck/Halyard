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

import com.msd.gin.halyard.common.TableConfig;
import com.msd.gin.halyard.sail.HBaseSail;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
	private static final String SYSTEM_TABLE = "RDF4JSYSTEM";
	// ensure different from RepositoryConfigRepository.ID
	private static final String SYSTEM_ID = "system";
    private volatile SharedHttpClientSessionManager client;
    private volatile SPARQLServiceResolver serviceResolver;
    private volatile Configuration config = HBaseConfiguration.create();

	public static Repository createSystemRepository(Configuration config) throws RepositoryException {
		config.set(TableConfig.ID_HASH, "Murmur3-128");
		SailRepository repo = new SailRepository(new HBaseSail(config, SYSTEM_TABLE, true, 0, true, 180, null, null));
		repo.init();
		return repo;
	}

    public HBaseRepositoryManager(Object...anyArgs) {
    }

    void overrideConfiguration(Configuration config) {
        this.config = config;
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
		Repository repository;
		if (SYSTEM_ID.equals(id)) {
			repository = createSystemRepository(config);
		} else {
			RepositoryConfig repConfig = getRepositoryConfig(id);
			if (repConfig != null) {
				repConfig.validate();
				repository = createRepositoryStack(repConfig.getRepositoryImplConfig());
				repository.init();
			} else {
				repository = null;
			}
		}
		return repository;
    }

    private Repository createRepositoryStack(RepositoryImplConfig implConfig) throws RepositoryConfigException {
        RepositoryFactory factory = RepositoryRegistry.getInstance()
            .get(implConfig.getType())
            .orElseThrow(() -> new RepositoryConfigException("Unsupported repository type: " + implConfig.getType()));
        Repository repository = factory.getRepository(implConfig);
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
        if (implConfig instanceof DelegatingRepositoryImplConfig) {
            RepositoryImplConfig delegateConfig = ((DelegatingRepositoryImplConfig) implConfig).getDelegate();
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
		RepositoryInfo repInfo;
		RepositoryConfig config = getRepositoryConfig(id);
		if (config != null) {
			repInfo = new RepositoryInfo();
			repInfo.setId(config.getID());
			repInfo.setDescription(config.getTitle());
			repInfo.setReadable(true);
			repInfo.setWritable(true);
		} else {
			repInfo = null;
		}
        return repInfo;
    }

    @Override
    public RepositoryConfig getRepositoryConfig(String repositoryID) throws RepositoryConfigException, RepositoryException {
		if (SYSTEM_ID.equals(repositoryID)) {
			return new RepositoryConfig(SYSTEM_ID, "System repository");
		} else {
			Repository systemRepository = getSystemRepository();
			return RepositoryConfigUtil.getRepositoryConfig(systemRepository, repositoryID);
		}
	}

	@Override
	public boolean hasRepositoryConfig(String repositoryID) throws RepositoryException, RepositoryConfigException {
		return getRepositoryConfig(repositoryID) != null;
	}

	public Repository getSystemRepository() {
		if (!isInitialized()) {
			throw new IllegalStateException("Repository Manager is not initialized");
		}
		return getRepository(SYSTEM_ID);
	}

	@Override
	public void addRepositoryConfig(RepositoryConfig repoConfig) throws RepositoryException, RepositoryConfigException {
		if (!SYSTEM_ID.equals(repoConfig.getID())) {
			Repository systemRepository = getSystemRepository();
			RepositoryConfigUtil.updateRepositoryConfigs(systemRepository, repoConfig);
		}
	}

	@Override
	public boolean removeRepository(String repositoryID) throws RepositoryException, RepositoryConfigException {
		boolean removed = false;
		if (!SYSTEM_ID.equals(repositoryID)) {
			removed = super.removeRepository(repositoryID);
			Repository systemRepository = getSystemRepository();
			RepositoryConfigUtil.removeRepositoryConfigs(systemRepository, repositoryID);
		}
		return removed;
	}

	@Override
	public Collection<RepositoryInfo> getAllRepositoryInfos() throws RepositoryException {
		List<RepositoryInfo> result = new ArrayList<>();
		result.add(getRepositoryInfo(SYSTEM_ID));
		for (String id : RepositoryConfigUtil.getRepositoryIDs(getSystemRepository())) {
			RepositoryInfo repInfo = getRepositoryInfo(id);
			result.add(repInfo);
		}
		Collections.sort(result);
		return result;
	}
}
