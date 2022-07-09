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

import com.msd.gin.halyard.vocab.HALYARD;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.repository.config.RepositoryConfigSchema;
import org.eclipse.rdf4j.repository.sail.config.SailRepositorySchema;
import org.eclipse.rdf4j.sail.config.SailConfigException;
import org.eclipse.rdf4j.sail.config.SailConfigSchema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.net.MalformedURLException;
import java.net.URL;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HBaseSailConfigTest {

    @Test
    public void testTableName() {
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.setTableName("whatevertable");
		assertEquals("whatevertable", cfg.getTableName());
    }

    @Test
    public void testSplitBits() {
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.setSplitBits(7);
        assertEquals(7, cfg.getSplitBits());
    }

    @Test
    public void testCreate() {
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.setCreate(false);
        assertFalse(cfg.isCreate());
        cfg.setCreate(true);
        assertTrue(cfg.isCreate());
    }

    @Test
	public void testSnapshotName() {
		HBaseSailConfig cfg = new HBaseSailConfig();
		cfg.setSnapshotName("whateversnapshot");
		assertEquals("whateversnapshot", cfg.getSnapshotName());
	}

	@Test
	public void testSnapshotRestorePath() {
		HBaseSailConfig cfg = new HBaseSailConfig();
		cfg.setSnapshotRestorePath("/path");
		assertEquals("/path", cfg.getSnapshotRestorePath());
	}

	@Test
    public void testPush() {
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.setPush(false);
        assertFalse(cfg.isPush());
        cfg.setPush(true);
        assertTrue(cfg.isPush());
    }

    @Test
    public void testEvaluationTimeout() {
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.setEvaluationTimeout(360);
        assertEquals(360, cfg.getEvaluationTimeout());
    }

    @Test
    public void testElasticIndex() throws MalformedURLException {
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.setElasticIndexURL(new URL("http://localhost:12345/index"));
		cfg.setElasticUsername("elastic");
		cfg.setElasticPassword("espass");
		cfg.setElasticKeystoreLocation("/home/keystore");
		cfg.setElasticKeystorePassword("keypass");
		cfg.setElasticTruststoreLocation("/home/truststore");
		cfg.setElasticTruststorePassword("trustpass");
        assertEquals("http://localhost:12345/index", cfg.getElasticIndexURL().toString());
		assertEquals("elastic", cfg.getElasticUsername());
		assertEquals("espass", cfg.getElasticPassword());
		assertEquals("/home/keystore", cfg.getElasticKeystoreLocation());
		assertEquals("keypass", cfg.getElasticKeystorePassword());
		assertEquals("/home/truststore", cfg.getElasticTruststoreLocation());
		assertEquals("trustpass", cfg.getElasticTruststorePassword());
    }

    @Test
    public void testExportAndParse() throws Exception {
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.setTableName("whatevertable");
        cfg.setSplitBits(7);
        cfg.setCreate(false);
        cfg.setPush(false);
        cfg.setElasticIndexURL(new URL("http://whateverURL/index"));
        TreeModel g = new TreeModel();
        cfg.export(g);
        cfg = new HBaseSailConfig();
        cfg.parse(g, null);
        assertEquals("whatevertable", cfg.getTableName());
        assertEquals(7, cfg.getSplitBits());
        assertFalse(cfg.isCreate());
        assertFalse(cfg.isPush());
        assertEquals("http://whateverURL/index", cfg.getElasticIndexURL().toString());
    }

    @Test
    public void testExportAndParse2() throws Exception {
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.setTableName(null);
        cfg.setSplitBits(5);
        cfg.setCreate(true);
        cfg.setPush(true);
        TreeModel g = new TreeModel();
        cfg.export(g);
        cfg = new HBaseSailConfig();
        cfg.parse(g, null);
        assertNull(cfg.getTableName());
        assertEquals(5, cfg.getSplitBits());
        assertTrue(cfg.isCreate());
        assertTrue(cfg.isPush());
        assertNull(cfg.getElasticIndexURL());
    }

    @Test
	public void testExportAndParse3() throws Exception {
		HBaseSailConfig cfg = new HBaseSailConfig();
		cfg.setSnapshotName("snapshot");
		cfg.setSnapshotRestorePath("/path");
		cfg.setPush(true);
		TreeModel g = new TreeModel();
		cfg.export(g);
		cfg = new HBaseSailConfig();
		cfg.parse(g, null);
		assertNull(cfg.getTableName());
		assertEquals("snapshot", cfg.getSnapshotName());
		assertEquals("/path", cfg.getSnapshotRestorePath());
		assertTrue(cfg.isPush());
		assertNull(cfg.getElasticIndexURL());
	}

	@Test
	public void testExportAndParse4() throws Exception {
		HBaseSailConfig cfg = new HBaseSailConfig();
		cfg.setElasticIndexURL(new URL("http://localhost:12345/index"));
		cfg.setElasticUsername("elastic");
		cfg.setElasticPassword("espass");
		cfg.setElasticKeystoreLocation("/home/keystore");
		cfg.setElasticKeystorePassword("keypass");
		cfg.setElasticTruststoreLocation("/home/truststore");
		cfg.setElasticTruststorePassword("trustpass");
		TreeModel g = new TreeModel();
		cfg.export(g);
		cfg = new HBaseSailConfig();
		cfg.parse(g, null);
		assertEquals("http://localhost:12345/index", cfg.getElasticIndexURL().toString());
		assertEquals("elastic", cfg.getElasticUsername());
		assertEquals("espass", cfg.getElasticPassword());
		assertEquals("/home/keystore", cfg.getElasticKeystoreLocation());
		assertEquals("keypass", cfg.getElasticKeystorePassword());
		assertEquals("/home/truststore", cfg.getElasticTruststoreLocation());
		assertEquals("trustpass", cfg.getElasticTruststorePassword());
	}

	@Test
    public void testParseEmpty() throws Exception {
        TreeModel g = new TreeModel();
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.parse(g, null);
        assertNull(cfg.getTableName());
        assertEquals(0, cfg.getSplitBits());
        assertTrue(cfg.isCreate());
        assertTrue(cfg.isPush());
        assertNull(cfg.getElasticIndexURL());
    }

    @Test
    public void testEmptyTableName() throws Exception {
        TreeModel g = new TreeModel();
        IRI node = SimpleValueFactory.getInstance().createIRI("http://node");
        g.add(node, HALYARD.TABLE_NAME_PROPERTY, SimpleValueFactory.getInstance().createLiteral(""));
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.parse(g, null);
        assertNull(cfg.getTableName());
    }

    @Test
    public void testDefaultTableNameFromRepositoryId() throws Exception {
        TreeModel g = new TreeModel();
        IRI node = SimpleValueFactory.getInstance().createIRI("http://node");
        Literal id =  SimpleValueFactory.getInstance().createLiteral("testId");
        g.add(node, SailRepositorySchema.SAILIMPL, node);
        g.add(node, SailConfigSchema.DELEGATE, node);
        g.add(node, RepositoryConfigSchema.REPOSITORYIMPL, node);
        g.add(node, RepositoryConfigSchema.REPOSITORYID, id);
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.parse(g, null);
        assertEquals(id.stringValue(), cfg.getTableName());
    }

    @Test
    public void testDefaultTableNameFromMissingRepositoryId() throws Exception {
        TreeModel g = new TreeModel();
        IRI node = SimpleValueFactory.getInstance().createIRI("http://node");
        g.add(node, SailRepositorySchema.SAILIMPL, node);
        g.add(node, RepositoryConfigSchema.REPOSITORYIMPL, node);
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.parse(g, null);
        assertNull(cfg.getTableName());
    }

    @Test
    public void testDefaultTableNameFromMissingRepoImpl() throws Exception {
        TreeModel g = new TreeModel();
        IRI node = SimpleValueFactory.getInstance().createIRI("http://node");
        g.add(node, SailRepositorySchema.SAILIMPL, node);
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.parse(g, null);
        assertNull(cfg.getTableName());
    }

	@Test
    public void testSplitbitsFail() throws Exception {
        TreeModel g = new TreeModel();
        g.add(SimpleValueFactory.getInstance().createIRI("http://node"), HALYARD.SPLITBITS_PROPERTY, SimpleValueFactory.getInstance().createLiteral("not a number"));
        HBaseSailConfig cfg = new HBaseSailConfig();
		assertThrows(SailConfigException.class, () -> cfg.parse(g, null));
    }

	@Test
    public void testCreateTableFail() throws Exception {
        TreeModel g = new TreeModel();
        g.add(SimpleValueFactory.getInstance().createIRI("http://node"), HALYARD.CREATE_TABLE_PROPERTY, SimpleValueFactory.getInstance().createLiteral("not a boolean"));
        HBaseSailConfig cfg = new HBaseSailConfig();
		assertThrows(SailConfigException.class, () -> cfg.parse(g, null));
    }

	@Test
    public void testPushStrategyFail() throws Exception {
        TreeModel g = new TreeModel();
        g.add(SimpleValueFactory.getInstance().createIRI("http://node"), HALYARD.PUSH_STRATEGY_PROPERTY, SimpleValueFactory.getInstance().createLiteral("not a boolean"));
        HBaseSailConfig cfg = new HBaseSailConfig();
		assertThrows(SailConfigException.class, () -> cfg.parse(g, null));
    }

	@Test
    public void testTimeoutFail() throws Exception {
        TreeModel g = new TreeModel();
        g.add(SimpleValueFactory.getInstance().createIRI("http://node"), HALYARD.EVALUATION_TIMEOUT_PROPERTY, SimpleValueFactory.getInstance().createLiteral("not a number"));
        HBaseSailConfig cfg = new HBaseSailConfig();
		assertThrows(SailConfigException.class, () -> cfg.parse(g, null));
    }
}
