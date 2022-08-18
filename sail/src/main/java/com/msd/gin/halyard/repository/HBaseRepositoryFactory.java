package com.msd.gin.halyard.repository;

import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.sail.HBaseSailFactory;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.repository.config.RepositoryFactory;
import org.eclipse.rdf4j.repository.config.RepositoryImplConfig;
import org.eclipse.rdf4j.sail.config.SailConfigException;

public class HBaseRepositoryFactory implements RepositoryFactory {
	public static final String REPOSITORY_TYPE = "openrdf:HBaseRepository";

	@Override
	public String getRepositoryType() {
		return REPOSITORY_TYPE;
	}

	@Override
	public RepositoryImplConfig getConfig() {
		return new HBaseRepositoryConfig();
	}

	@Override
	public Repository getRepository(RepositoryImplConfig config) throws RepositoryConfigException {
		if (config instanceof HBaseRepositoryConfig) {
			HBaseRepositoryConfig repoConfig = (HBaseRepositoryConfig) config;

			try {
				HBaseSail sail = (HBaseSail) new HBaseSailFactory().getSail(repoConfig.getSailImplConfig());
				return new HBaseRepository(sail);
			} catch (SailConfigException e) {
				throw new RepositoryConfigException(e.getMessage(), e);
			}
		}

		throw new RepositoryConfigException("Invalid configuration class: " + config.getClass());
	}
}
