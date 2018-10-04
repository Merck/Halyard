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

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.common.HalyardTableUtils;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.eclipse.rdf4j.IsolationLevel;
import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.UnknownSailTransactionStateException;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HBaseSailTest {

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDataDir() throws Exception {
        new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null, null).getDataDir();
    }

    @Test
    public void testInitializeAndShutDown() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null, null);
        assertFalse(sail.isOpen());
        sail.initialize();
        assertTrue(sail.isOpen());
        sail.shutDown();
        assertFalse(sail.isOpen());
    }

    @Test
    public void testIsWritable() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertableRW", true, 0, true, 0, null, null);
        sail.initialize();
        HTableDescriptor desc = sail.table.getTableDescriptor();
        assertTrue(sail.isWritable());
        sail.shutDown();
        try (Connection con = ConnectionFactory.createConnection(HBaseServerTestInstance.getInstanceConfig())) {
            try (Admin ha = con.getAdmin()) {
                desc = new HTableDescriptor(desc);
                desc.setReadOnly(true);
                ha.modifyTable(desc.getTableName(), desc);
            }
        }
        sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), desc.getNameAsString(), true, 0, true, 0, null, null);
        sail.initialize();
        assertFalse(sail.isWritable());
        sail.shutDown();
    }

    @Test(expected = SailException.class)
    public void testWriteToReadOnly() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertableRO", true, 0, true, 0, null, null);
        sail.initialize();
        try {
            HTableDescriptor desc = sail.table.getTableDescriptor();
            try (Connection con = ConnectionFactory.createConnection(HBaseServerTestInstance.getInstanceConfig())) {
                try (Admin ha = con.getAdmin()) {
                    desc = new HTableDescriptor(desc);
                    desc.setReadOnly(true);
                    ha.modifyTable(desc.getTableName(), desc);
                }
            }
            ValueFactory vf = SimpleValueFactory.getInstance();
            sail.addStatement(vf.createIRI("http://whatever/subj"), vf.createIRI("http://whatever/pred"), vf.createLiteral("whatever"));
        } finally {
            sail.shutDown();
        }
    }

    @Test
    public void testGetConnection() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null, null);
        assertSame(sail, sail.getConnection());
    }

    @Test
    public void testGetValueFactory() throws Exception {
        assertSame(SimpleValueFactory.getInstance(), new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null, null).getValueFactory());
    }

    @Test
    public void testGetSupportedIsolationLevels() throws Exception {
        List<IsolationLevel> il = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null, null).getSupportedIsolationLevels();
        assertEquals(1, il.size());
        assertTrue(il.contains(IsolationLevels.NONE));
    }

    @Test
    public void testGetDefaultIsolationLevel() throws Exception {
        assertSame(IsolationLevels.NONE, new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null, null).getDefaultIsolationLevel());
    }

    @Test
    public void testGetContextIDs() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Configuration cfg = HBaseServerTestInstance.getInstanceConfig();
        HBaseSail sail = new HBaseSail(cfg, "whatevertablectx", true, 0, true, 0, null, null);
        sail.initialize();
        sail.addStatement(HALYARD.STATS_ROOT_NODE, SD.NAMED_GRAPH_PROPERTY, vf.createIRI("http://whatever/ctx"), HALYARD.STATS_GRAPH_CONTEXT);
        sail.commit();
        try (CloseableIteration<? extends Resource, SailException> ctxIt = sail.getContextIDs()) {
            assertTrue(ctxIt.hasNext());
            assertEquals("http://whatever/ctx", ctxIt.next().stringValue());
        }
    }

    @Test
    public void testSize() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Configuration cfg = HBaseServerTestInstance.getInstanceConfig();
        HBaseSail sail = new HBaseSail(cfg, "whatevertablesize", true, 0, true, 180, null, null);
        sail.initialize();
        assertEquals(0, sail.size());
        assertEquals(0, sail.size(HALYARD.STATS_ROOT_NODE));
        IRI iri = vf.createIRI("http://whatever/");
        sail.addStatement(iri, iri, iri);
        sail.commit();
        assertEquals(1, sail.size());
        sail.addStatement(HALYARD.STATS_ROOT_NODE, VOID.TRIPLES, vf.createLiteral(567), HALYARD.STATS_GRAPH_CONTEXT);
        sail.commit();
        assertEquals(567, sail.size());
        assertEquals(567, sail.size(HALYARD.STATS_ROOT_NODE));
        sail.addStatement(HALYARD.STATS_ROOT_NODE, VOID.TRIPLES, vf.createLiteral(568), HALYARD.STATS_GRAPH_CONTEXT);
        sail.commit();
        try {
            sail.size();
            fail("Expected SailException");
        } catch (SailException se) {}
        try {
            sail.size(HALYARD.STATS_ROOT_NODE);
            fail("Expected SailException");
        } catch (SailException se) {}
    }

    @Test(expected = UnknownSailTransactionStateException.class)
    public void testBegin() throws Exception {
        new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null, null).begin(IsolationLevels.READ_COMMITTED);
    }

    @Test
    public void testRollback() throws Exception {
        new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null, null).rollback();
    }

    @Test
    public void testIsActive() throws Exception {
        assertTrue(new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null, null).isActive());
    }

    @Test
    public void testNamespaces() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null, null);
        sail.initialize();
        assertFalse(sail.getNamespaces().hasNext());
        sail.setNamespace("prefix", "http://whatever/namespace/");
        sail.shutDown();
        sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", false, 0, true, 0, null, null);
        sail.initialize();
        assertTrue(sail.getNamespaces().hasNext());
        sail.removeNamespace("prefix");
        sail.shutDown();
        sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", false, 0, true, 0, null, null);
        sail.initialize();
        assertFalse(sail.getNamespaces().hasNext());
        sail.setNamespace("prefix", "http://whatever/namespace/");
        sail.setNamespace("prefix", "http://whatever/namespace2/");
        sail.shutDown();
        sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", false, 0, true, 0, null, null);
        sail.initialize();
        assertEquals("http://whatever/namespace2/", sail.getNamespace("prefix"));
        sail.clearNamespaces();
        sail.shutDown();
        sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", false, 0, true, 0, null, null);
        sail.initialize();
        assertFalse(sail.getNamespaces().hasNext());
        sail.shutDown();
    }

    @Test
    public void testClear() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Resource subj = vf.createIRI("http://whatever/subj/");
        IRI pred = vf.createIRI("http://whatever/pred/");
        Value obj = vf.createLiteral("whatever");
        IRI context = vf.createIRI("http://whatever/context/");
        CloseableIteration<? extends Statement, SailException> iter;
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertableClear", true, 0, true, 0, null, null);
        sail.initialize();
        sail.addStatement(subj, pred, obj, context);
        sail.addStatement(subj, pred, obj);
        sail.commit();
        iter = sail.getStatements(subj, pred, obj, true);
        assertTrue(iter.hasNext());
        iter.close();
        sail.clear(context);
        iter = sail.getStatements(subj, pred, obj, true);
        assertTrue(iter.hasNext());
        iter.close();
        iter = sail.getStatements(subj, pred, obj, true, context);
        assertFalse(iter.hasNext());
        iter.close();
        sail.clear();
        iter = sail.getStatements(subj, pred, obj, true);
        assertFalse(iter.hasNext());
        iter.close();
    }

    @Test
    public void testEvaluate() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Resource subj = vf.createIRI("http://whatever/subj/");
        IRI pred = vf.createIRI("http://whatever/pred/");
        Value obj = vf.createLiteral("whatever");
        CloseableIteration<? extends Statement, SailException> iter;
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null, null);
        SailRepository rep = new SailRepository(sail);
        rep.initialize();
        sail.addStatement(subj, pred, obj);
        sail.commit();
        TupleQuery q = rep.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, "select ?s ?p ?o where {<http://whatever/subj/> <http://whatever/pred/> \"whatever\"}");
        TupleQueryResult res = q.evaluate();
        assertTrue(res.hasNext());
        rep.shutDown();
    }

    @Test
    public void testEvaluateWithContext() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Resource subj = vf.createIRI("http://whatever/subj/");
        IRI pred = vf.createIRI("http://whatever/pred/");
        Value obj = vf.createLiteral("whatever");
        IRI context = vf.createIRI("http://whatever/context/");
        CloseableIteration<? extends Statement, SailException> iter;
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null, null);
        SailRepository rep = new SailRepository(sail);
        rep.initialize();
        sail.addStatement(subj, pred, obj, context);
        sail.commit();
        TupleQuery q = rep.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, "select ?s ?p ?o from named <http://whatever/context/> where {<http://whatever/subj/> <http://whatever/pred/> \"whatever\"}");
        TupleQueryResult res = q.evaluate();
        assertFalse(res.hasNext());
        rep.shutDown();
    }

    @Test
    public void testEvaluateService() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Resource subj = vf.createIRI("http://whatever/subj/");
        IRI pred = vf.createIRI("http://whatever/pred/");
        Value obj = vf.createLiteral("whatever");
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whateverservice", true, 0, true, 0, null, null);
        SailRepository rep = new SailRepository(sail);
        rep.initialize();
        sail.addStatement(subj, pred, obj);
        sail.commit();
        rep.shutDown();

        sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whateverparent", true, 0, true, 0, null, null);
        rep = new SailRepository(sail);
        rep.initialize();
        TupleQuery q = rep.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, "select * {SERVICE <" + HALYARD.NAMESPACE +"whateverservice> {?s ?p ?o}}");
        TupleQueryResult res = q.evaluate();
        assertTrue(res.hasNext());
        rep.shutDown();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testStatementsIteratorRemove1() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null, null);
        try {
            sail.initialize();
            sail.getStatements(null, null, null, true).remove();
        } finally {
            sail.shutDown();
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testStatementsIteratorRemove2() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null, null);
        try {
            sail.initialize();
            ValueFactory vf = SimpleValueFactory.getInstance();
            sail.getStatements(vf.createIRI("http://whatever/subj/"), vf.createIRI("http://whatever/pred/"), vf.createIRI("http://whatever/obj/"), true).remove();
        } finally {
            sail.shutDown();
        }
    }

    @Test
    public void testEmptyMethodsThatShouldDoNothing() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null, null);
        sail.setDataDir(null);
        sail.prepare();
        sail.begin();
        sail.flush();
        sail.startUpdate(null);
        sail.endUpdate(null);
        sail.close();
    }

    @Test(expected = SailException.class)
    public void testTimeoutGetStatements() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 1, null, null);
        sail.initialize();
        try {
            sail.commit();
            CloseableIteration<? extends Statement, SailException> it = sail.getStatements(null, null, null, true);
            Thread.sleep(2000);
            it.hasNext();
        } finally {
            sail.shutDown();
        }
    }

    @Test
    public void testCardinalityCalculator() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "cardinalitytable", true, 0, true, 0, null, null);
        sail.initialize();
        SimpleValueFactory f = SimpleValueFactory.getInstance();
        TupleExpr q1 = QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL, "select * where {?s a ?o}", "http://whatever/").getTupleExpr();
        TupleExpr q2 = QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL, "select * where {graph <http://whatevercontext> {?s a ?o}}", "http://whatever/").getTupleExpr();
        TupleExpr q3 = QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL, "select * where {?s <http://whatever/> ?o}", "http://whatever/").getTupleExpr();
        TupleExpr q4 = QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL, "select * where {?s ?p \"whatever\"^^<" + HALYARD.SEARCH_TYPE.stringValue() + ">}", "http://whatever/").getTupleExpr();
        assertEquals(100.0, sail.statistics.getCardinality(q1), 0.01);
        assertEquals(100.0, sail.statistics.getCardinality(q2), 0.01);
        assertEquals(100.0, sail.statistics.getCardinality(q3), 0.01);
        assertEquals(1.0, sail.statistics.getCardinality(q4), 0.01);
        sail.addStatement(HALYARD.STATS_ROOT_NODE, VOID.TRIPLES, f.createLiteral(10000l), HALYARD.STATS_GRAPH_CONTEXT);
        sail.addStatement(f.createIRI(HALYARD.STATS_ROOT_NODE.stringValue() + "_property_" + HalyardTableUtils.encode(HalyardTableUtils.hashKey(RDF.TYPE))), VOID.TRIPLES, f.createLiteral(5000l), HALYARD.STATS_GRAPH_CONTEXT);
        sail.addStatement(f.createIRI("http://whatevercontext"), VOID.TRIPLES, f.createLiteral(10000l), HALYARD.STATS_GRAPH_CONTEXT);
        sail.addStatement(f.createIRI("http://whatevercontext_property_" + HalyardTableUtils.encode(HalyardTableUtils.hashKey(RDF.TYPE))), VOID.TRIPLES, f.createLiteral(20l), HALYARD.STATS_GRAPH_CONTEXT);
        sail.commit();
        assertEquals(5000.0, sail.statistics.getCardinality(q1), 0.01);
        assertEquals(20.0, sail.statistics.getCardinality(q2), 0.01);
        assertEquals(100.0, sail.statistics.getCardinality(q3), 0.01);
        assertEquals(1.0, sail.statistics.getCardinality(q4), 0.01);
        sail.shutDown();
    }

    @Test
    public void testEvaluateServices() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whateverservice2", true, 0, true, 0, null, null);
        SailRepository rep = new SailRepository(sail);
        rep.initialize();
        Random r = new Random(333);
        IRI pred = vf.createIRI("http://whatever/pred");
        IRI meta = vf.createIRI("http://whatever/meta");
        for (int i = 0; i < 1000; i++) {
            IRI subj = vf.createIRI("http://whatever/subj#" + r.nextLong());
            IRI graph = vf.createIRI("http://whatever/grp#" + r.nextLong());
            sail.addStatement(subj, pred, graph, meta);
            for (int j = 0; j < 10; j++) {
                IRI s = vf.createIRI("http://whatever/s#" + r.nextLong());
                IRI p = vf.createIRI("http://whatever/p#" + r.nextLong());
                IRI o = vf.createIRI("http://whatever/o#" + r.nextLong());
                sail.addStatement(s, p, o, graph);
            }
        }
        sail.commit();
        rep.shutDown();

        sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whateverparent2", true, 0, true, 0, null, null);
        rep = new SailRepository(sail);
        rep.initialize();
        TupleQuery q = rep.getConnection().prepareTupleQuery(QueryLanguage.SPARQL,
            "select * where {"
            + "  SERVICE <" + HALYARD.NAMESPACE + "whateverservice2> {"
            + "    graph <http://whatever/meta> {"
            + "      ?subj <http://whatever/pred> ?graph"
            + "    }"
            + "  }"
            + "  SERVICE <" + HALYARD.NAMESPACE + "whateverservice2> {"
            + "    graph ?graph {"
            + "      ?s ?p ?o"
            + "    }"
            + "  }"
            + "}");
        TupleQueryResult res = q.evaluate();
        int count = 0;
        while (res.hasNext()) {
            count++;
            res.next();
        }
        rep.shutDown();
        assertEquals(10000, count);
    }
}
