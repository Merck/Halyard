package com.msd.gin.halyard.repository;

import com.msd.gin.halyard.sail.HBaseSailConfig;
import com.msd.gin.halyard.sail.HBaseSailFactory;

import java.util.Optional;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.ModelException;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.repository.config.AbstractRepositoryImplConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.sail.config.SailConfigException;

import static org.eclipse.rdf4j.repository.sail.config.SailRepositorySchema.*;
import static org.eclipse.rdf4j.repository.sail.config.SailRepositorySchema.NAMESPACE;
import static org.eclipse.rdf4j.sail.config.SailConfigSchema.*;

public class HBaseRepositoryConfig extends AbstractRepositoryImplConfig {
	private HBaseSailConfig sailImplConfig;

	public HBaseRepositoryConfig() {
		super(HBaseRepositoryFactory.REPOSITORY_TYPE);
	}

	public HBaseRepositoryConfig(HBaseSailConfig cfg) {
		super(HBaseRepositoryFactory.REPOSITORY_TYPE);
		setSailImplConfig(cfg);
	}

	public HBaseSailConfig getSailImplConfig() {
		return sailImplConfig;
	}

	public void setSailImplConfig(HBaseSailConfig cfg) {
		sailImplConfig = cfg;
	}

	@Override
	public void validate() throws RepositoryConfigException {
		super.validate();
		if (sailImplConfig == null) {
			throw new RepositoryConfigException("No Sail implementation specified for Sail repository");
		}

		try {
			sailImplConfig.validate();
		} catch (SailConfigException e) {
			throw new RepositoryConfigException(e.getMessage(), e);
		}
	}

	@Override
	public Resource export(Model model) {
		Resource repImplNode = super.export(model);

		if (sailImplConfig != null) {
			model.setNamespace("sr", NAMESPACE);
			Resource sailImplNode = sailImplConfig.export(model);
			model.add(repImplNode, SAILIMPL, sailImplNode);
		}

		return repImplNode;
	}

	@Override
	public void parse(Model model, Resource repImplNode) throws RepositoryConfigException {
		try {
			Optional<Resource> sailImplNode = Models.objectResource(model.getStatements(repImplNode, SAILIMPL, null));
			if (sailImplNode.isPresent()) {
				if (model.contains(sailImplNode.get(), SAILTYPE, SimpleValueFactory.getInstance().createLiteral(HBaseSailFactory.SAIL_TYPE))) {
					sailImplConfig = (HBaseSailConfig) new HBaseSailFactory().getConfig();
					sailImplConfig.parse(model, sailImplNode.get());
				} else {
					throw new RepositoryConfigException("Missing/invalid sail type");
				}
			}
		} catch (ModelException | SailConfigException e) {
			throw new RepositoryConfigException(e.getMessage(), e);
		}
	}
}
