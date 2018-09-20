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

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.repository.config.RepositoryConfigSchema;
import org.eclipse.rdf4j.repository.sail.config.SailRepositorySchema;
import org.eclipse.rdf4j.sail.config.AbstractSailImplConfig;
import org.eclipse.rdf4j.sail.config.SailConfigException;
import org.eclipse.rdf4j.sail.config.SailConfigSchema;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HBaseSailConfigTest {

    @Test
    public void testTablespace() {
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.setTablespace("whatevertable");
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
    public void testElasticIndex() {
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.setElasticIndexURL("http://localhost:12345/index");
        assertEquals("http://localhost:12345/index", cfg.getElasticIndexURL());
    }

    @Test
    public void testExportAndParse() throws Exception {
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.setTablespace("whatevertable");
        cfg.setSplitBits(7);
        cfg.setCreate(false);
        cfg.setPush(false);
        cfg.setElasticIndexURL("http://whateverURL/index");
        TreeModel g = new TreeModel();
        cfg.export(g);
        cfg = new HBaseSailConfig();
        cfg.parse(g, null);
        assertEquals("whatevertable", cfg.getTablespace());
        assertEquals(7, cfg.getSplitBits());
        assertFalse(cfg.isCreate());
        assertFalse(cfg.isPush());
        assertEquals("http://whateverURL/index", cfg.getElasticIndexURL());
    }

    @Test
    public void testExportAndParse2() throws Exception {
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.setTablespace(null);
        cfg.setSplitBits(5);
        cfg.setCreate(true);
        cfg.setPush(true);
        TreeModel g = new TreeModel();
        cfg.export(g);
        cfg = new HBaseSailConfig();
        cfg.parse(g, null);
        assertNull(cfg.getTablespace());
        assertEquals(5, cfg.getSplitBits());
        assertTrue(cfg.isCreate());
        assertTrue(cfg.isPush());
        assertEquals("", cfg.getElasticIndexURL());
    }

    @Test
    public void testParseEmpty() throws Exception {
        TreeModel g = new TreeModel();
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.parse(g, null);
        assertNull(cfg.getTablespace());
        assertEquals(0, cfg.getSplitBits());
        assertTrue(cfg.isCreate());
        assertTrue(cfg.isPush());
        assertEquals("", cfg.getElasticIndexURL());
    }

    @Test
    public void testEmptyTableSpace() throws Exception {
        TreeModel g = new TreeModel();
        IRI node = SimpleValueFactory.getInstance().createIRI("http://node");
        g.add(node, HALYARD.TABLE_NAME_PROPERTY, SimpleValueFactory.getInstance().createLiteral(""));
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.parse(g, null);
        assertNull(cfg.getTablespace());
    }

    @Test
    public void testDefaultTableSpaceFromRepositoryId() throws Exception {
        TreeModel g = new TreeModel();
        IRI node = SimpleValueFactory.getInstance().createIRI("http://node");
        Literal id =  SimpleValueFactory.getInstance().createLiteral("testId");
        g.add(node, SailRepositorySchema.SAILIMPL, node);
        g.add(node, SailConfigSchema.DELEGATE, node);
        g.add(node, RepositoryConfigSchema.REPOSITORYIMPL, node);
        g.add(node, RepositoryConfigSchema.REPOSITORYID, id);
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.parse(g, null);
        assertEquals(id.stringValue(), cfg.getTablespace());
    }

    @Test
    public void testDefaultTableSpaceFromMissingRepositoryId() throws Exception {
        TreeModel g = new TreeModel();
        IRI node = SimpleValueFactory.getInstance().createIRI("http://node");
        g.add(node, SailRepositorySchema.SAILIMPL, node);
        g.add(node, RepositoryConfigSchema.REPOSITORYIMPL, node);
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.parse(g, null);
        assertNull(cfg.getTablespace());
    }

    @Test
    public void testDefaultTableSpaceFromMissingRepoImpl() throws Exception {
        TreeModel g = new TreeModel();
        IRI node = SimpleValueFactory.getInstance().createIRI("http://node");
        g.add(node, SailRepositorySchema.SAILIMPL, node);
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.parse(g, null);
        assertNull(cfg.getTablespace());
    }

    @Test(expected = SailConfigException.class)
    public void testSplitbitsFail() throws Exception {
        TreeModel g = new TreeModel();
        g.add(SimpleValueFactory.getInstance().createIRI("http://node"), HALYARD.SPLITBITS_PROPERTY, SimpleValueFactory.getInstance().createLiteral("not a number"));
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.parse(g, null);
    }

    @Test(expected = SailConfigException.class)
    public void testCreateTableFail() throws Exception {
        TreeModel g = new TreeModel();
        g.add(SimpleValueFactory.getInstance().createIRI("http://node"), HALYARD.CREATE_TABLE_PROPERTY, SimpleValueFactory.getInstance().createLiteral("not a boolean"));
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.parse(g, null);
    }

    @Test(expected = SailConfigException.class)
    public void testPushStrategyFail() throws Exception {
        TreeModel g = new TreeModel();
        g.add(SimpleValueFactory.getInstance().createIRI("http://node"), HALYARD.PUSH_STRATEGY_PROPERTY, SimpleValueFactory.getInstance().createLiteral("not a boolean"));
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.parse(g, null);
    }

    @Test(expected = SailConfigException.class)
    public void testTimeoutFail() throws Exception {
        TreeModel g = new TreeModel();
        g.add(SimpleValueFactory.getInstance().createIRI("http://node"), HALYARD.EVALUATION_TIMEOUT_PROPERTY, SimpleValueFactory.getInstance().createLiteral("not a number"));
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.parse(g, null);
    }

    @Test
    public void testInstantiateConstants() {
        new HALYARD();
        new VOID_EXT();
    }
}
