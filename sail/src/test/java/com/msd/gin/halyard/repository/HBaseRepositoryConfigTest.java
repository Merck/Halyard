package com.msd.gin.halyard.repository;

import com.msd.gin.halyard.sail.HBaseSailConfig;

import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class HBaseRepositoryConfigTest {

	@Test
	public void testExportAndParse() throws Exception {
		HBaseSailConfig sailConfig = new HBaseSailConfig();
		sailConfig.setTableName("test");
		HBaseRepositoryConfig cfg = new HBaseRepositoryConfig();
		cfg.setSailImplConfig(sailConfig);
		cfg.validate();
		TreeModel g = new TreeModel();
		cfg.export(g);
		cfg = new HBaseRepositoryConfig();
		cfg.parse(g, null);
		assertEquals("test", cfg.getSailImplConfig().getTableName());
	}

	@Test
	public void testValidate_noSailConfig() throws Exception {
		HBaseRepositoryConfig cfg = new HBaseRepositoryConfig();
		assertThrows(RepositoryConfigException.class, () -> cfg.validate());
    }
}
