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
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.eclipse.rdf4j.IsolationLevel;
import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.UnknownSailTransactionStateException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HBaseSailTest {

	private Connection hconn;

    @Before
    public void setup() throws Exception {
		hconn = HalyardTableUtils.getConnection(HBaseServerTestInstance.getInstanceConfig());
    }

    @After
    public void teardown() throws Exception {
		hconn.close();
    }

	@Test(expected = UnsupportedOperationException.class)
    public void testGetDataDir() throws Exception {
        new HBaseSail(hconn, "whatevertable", true, 0, true, 0, null, null).getDataDir();
    }

    @Test
    public void testInitializeAndShutDown() throws Exception {
        HBaseSail sail = new HBaseSail(hconn, "whatevertable", true, 0, true, 0, null, null);
        sail.initialize();
        sail.shutDown();
    }

    @Test
    public void testInitializeAndShutDownWithNoSharedConnection() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null, null);
        sail.initialize();
        sail.shutDown();
    }

    @Test
    public void testIsWritable() throws Exception {
        HBaseSail sail = new HBaseSail(hconn, "whatevertableRW", true, 0, true, 0, null, null);
        sail.initialize();
        HTableDescriptor desc = sail.getTable().getTableDescriptor();
        assertTrue(sail.isWritable());
        sail.shutDown();
        try (Admin ha = hconn.getAdmin()) {
            desc = new HTableDescriptor(desc);
            desc.setReadOnly(true);
            ha.modifyTable(desc.getTableName(), desc);
        }
        sail = new HBaseSail(hconn, desc.getNameAsString(), true, 0, true, 0, null, null);
        sail.initialize();
        assertFalse(sail.isWritable());
        sail.shutDown();
    }

    @Test(expected = SailException.class)
    public void testWriteToReadOnly() throws Exception {
        HBaseSail sail = new HBaseSail(hconn, "whatevertableRO", true, 0, true, 0, null, null);
        sail.initialize();
        try {
            HTableDescriptor desc = sail.getTable().getTableDescriptor();
            try (Admin ha = hconn.getAdmin()) {
                desc = new HTableDescriptor(desc);
                desc.setReadOnly(true);
                ha.modifyTable(desc.getTableName(), desc);
            }
            ValueFactory vf = SimpleValueFactory.getInstance();
			try (SailConnection conn = sail.getConnection()) {
            	conn.addStatement(vf.createIRI("http://whatever/subj"), vf.createIRI("http://whatever/pred"), vf.createLiteral("whatever"));
            }
        } finally {
            sail.shutDown();
        }
    }

    @Test
    public void testGetConnection() throws Exception {
        HBaseSail sail = new HBaseSail(hconn, "whatevertable", true, 0, true, 0, null, null);
		try {
			sail.initialize();
			try (SailConnection conn = sail.getConnection()) {
				assertTrue(conn.isOpen());
			}
		} finally {
			sail.shutDown();
		}
    }

    @Test
    public void testGetValueFactory() throws Exception {
        assertSame(SimpleValueFactory.getInstance(), new HBaseSail(hconn, "whatevertable", true, 0, true, 0, null, null).getValueFactory());
    }

    @Test
    public void testGetSupportedIsolationLevels() throws Exception {
        List<IsolationLevel> il = new HBaseSail(hconn, "whatevertable", true, 0, true, 0, null, null).getSupportedIsolationLevels();
        assertEquals(1, il.size());
        assertTrue(il.contains(IsolationLevels.NONE));
    }

    @Test
    public void testGetDefaultIsolationLevel() throws Exception {
        assertSame(IsolationLevels.NONE, new HBaseSail(hconn, "whatevertable", true, 0, true, 0, null, null).getDefaultIsolationLevel());
    }

    @Test
    public void testGetContextIDs() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
		HBaseSail sail = new HBaseSail(hconn, "whatevertablectx", true, 0, true,
				0, null, null);
        sail.initialize();
		try (SailConnection conn = sail.getConnection()) {
            conn.addStatement(HALYARD.STATS_ROOT_NODE, SD.NAMED_GRAPH_PROPERTY, vf.createIRI("http://whatever/ctx"), HALYARD.STATS_GRAPH_CONTEXT);
            conn.commit();
            try (CloseableIteration<? extends Resource, SailException> ctxIt = conn.getContextIDs()) {
                assertTrue(ctxIt.hasNext());
                assertEquals("http://whatever/ctx", ctxIt.next().stringValue());
            }
        }
    }

    @Test
    public void testSize() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
		HBaseSail sail = new HBaseSail(hconn, "whatevertablesize", true, 0, true,
				180, null, null);
        sail.initialize();
		try (SailConnection conn = sail.getConnection()) {
            assertEquals(0, conn.size());
            assertEquals(0, conn.size(HALYARD.STATS_ROOT_NODE));
            IRI iri = vf.createIRI("http://whatever/");
            conn.addStatement(iri, iri, iri);
            conn.commit();
            assertEquals(1, conn.size());
            conn.addStatement(HALYARD.STATS_ROOT_NODE, VOID.TRIPLES, vf.createLiteral(567), HALYARD.STATS_GRAPH_CONTEXT);
            conn.commit();
            assertEquals(567, conn.size());
            assertEquals(567, conn.size(HALYARD.STATS_ROOT_NODE));
            conn.addStatement(HALYARD.STATS_ROOT_NODE, VOID.TRIPLES, vf.createLiteral(568), HALYARD.STATS_GRAPH_CONTEXT);
            conn.commit();
            try {
            	conn.size();
                fail("Expected SailException");
            } catch (SailException se) {}
            try {
            	conn.size(HALYARD.STATS_ROOT_NODE);
                fail("Expected SailException");
            } catch (SailException se) {}
        }
    }

    @Test(expected = UnknownSailTransactionStateException.class)
    public void testBegin() throws Exception {
        HBaseSail sail = new HBaseSail(hconn, "whatevertable", true, 0, true, 0, null, null);
		try {
			sail.initialize();
			try (SailConnection conn = sail.getConnection()) {
				conn.begin(IsolationLevels.READ_COMMITTED);
			}
		} finally {
			sail.shutDown();
		}
    }

    @Test
    public void testRollback() throws Exception {
        HBaseSail sail = new HBaseSail(hconn, "whatevertable", true, 0, true, 0, null, null);
		try {
			sail.initialize();
			try (SailConnection conn = sail.getConnection()) {
				conn.rollback();
			}
		} finally {
			sail.shutDown();
		}
    }

    @Test
    public void testIsActive() throws Exception {
    	HBaseSail sail = new HBaseSail(hconn, "whatevertable", true, 0, true, 0, null, null);
		try {
			sail.initialize();
			try (SailConnection conn = sail.getConnection()) {
				assertTrue(conn.isActive());
			}
		} finally {
			sail.shutDown();
		}
    }

    @Test
    public void testNamespaces() throws Exception {
        HBaseSail sail = new HBaseSail(hconn, "whatevertable", true, 0, true, 0, null, null);
        sail.initialize();
		try (SailConnection conn = sail.getConnection()) {
        	assertFalse(conn.getNamespaces().hasNext());
        	conn.setNamespace("prefix", "http://whatever/namespace/");
        }
        sail.shutDown();
        sail = new HBaseSail(hconn, "whatevertable", false, 0, true, 0, null, null);
        sail.initialize();
		try (SailConnection conn = sail.getConnection()) {
	        assertTrue(conn.getNamespaces().hasNext());
	        conn.removeNamespace("prefix");
        }
        sail.shutDown();
        sail = new HBaseSail(hconn, "whatevertable", false, 0, true, 0, null, null);
        sail.initialize();
		try (SailConnection conn = sail.getConnection()) {
	        assertFalse(conn.getNamespaces().hasNext());
	        conn.setNamespace("prefix", "http://whatever/namespace/");
	        conn.setNamespace("prefix", "http://whatever/namespace2/");
        }
        sail.shutDown();
        sail = new HBaseSail(hconn, "whatevertable", false, 0, true, 0, null, null);
        sail.initialize();
		try (SailConnection conn = sail.getConnection()) {
        	assertEquals("http://whatever/namespace2/", conn.getNamespace("prefix"));
        	conn.clearNamespaces();
        }
        sail.shutDown();
        sail = new HBaseSail(hconn, "whatevertable", false, 0, true, 0, null, null);
        sail.initialize();
		try (SailConnection conn = sail.getConnection()) {
        	assertFalse(conn.getNamespaces().hasNext());
        }
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
        HBaseSail sail = new HBaseSail(hconn, "whatevertableClear", true, 0, true, 0, null, null);
        sail.initialize();
		try (SailConnection conn = sail.getConnection()) {
	        conn.addStatement(subj, pred, obj, context);
	        conn.addStatement(subj, pred, obj);
	        conn.commit();
	        iter = conn.getStatements(subj, pred, obj, true);
	        assertTrue(iter.hasNext());
	        iter.close();
	        conn.clear(context);
	        iter = conn.getStatements(subj, pred, obj, true);
	        assertTrue(iter.hasNext());
	        iter.close();
	        iter = conn.getStatements(subj, pred, obj, true, context);
	        assertFalse(iter.hasNext());
	        iter.close();
	        conn.clear();
	        iter = conn.getStatements(subj, pred, obj, true);
	        assertFalse(iter.hasNext());
	        iter.close();
        }
    }

    @Test
    public void testEvaluate() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Resource subj = vf.createIRI("http://whatever/subj/");
        IRI pred = vf.createIRI("http://whatever/pred/");
        Value obj = vf.createLiteral("whatever");
        HBaseSail sail = new HBaseSail(hconn, "whatevertable", true, 0, true, 0, null, null);
        SailRepository rep = new SailRepository(sail);
        rep.initialize();
		try (RepositoryConnection conn = rep.getConnection()) {
			conn.add(subj, pred, obj);
			conn.commit();
		}
		try (RepositoryConnection conn = rep.getConnection()) {
			TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL,
					"select ?s ?p ?o where {<http://whatever/subj/> <http://whatever/pred/> \"whatever\"}");
			try (TupleQueryResult res = q.evaluate()) {
				assertTrue(res.hasNext());
			}
		}
        rep.shutDown();
    }

    @Test
	public void testEvaluateConstruct() throws Exception {
		ValueFactory vf = SimpleValueFactory.getInstance();
		Resource subj = vf.createIRI("http://whatever/subj/");
		IRI pred = vf.createIRI("http://whatever/pred/");
		Value obj = vf.createLiteral("whatever");
		HBaseSail sail = new HBaseSail(hconn, "whatevertable", true, 0, true, 0, null, null);
		SailRepository rep = new SailRepository(sail);
		rep.initialize();
		try (RepositoryConnection conn = rep.getConnection()) {
			conn.add(subj, pred, obj);
			conn.commit();
		}
		try (RepositoryConnection conn = rep.getConnection()) {
			GraphQuery q = conn.prepareGraphQuery(QueryLanguage.SPARQL, "construct {?s ?p ?o} where {?s ?p ?o}");
			try (GraphQueryResult res = q.evaluate()) {
				assertTrue(res.hasNext());
			}
		}
		rep.shutDown();
	}

	@Test
    public void testEvaluateWithContext() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Resource subj = vf.createIRI("http://whatever/subj/");
        IRI pred = vf.createIRI("http://whatever/pred/");
        Value obj = vf.createLiteral("whatever");
        IRI context = vf.createIRI("http://whatever/context/");
        HBaseSail sail = new HBaseSail(hconn, "whatevertable", true, 0, true, 0, null, null);
        SailRepository rep = new SailRepository(sail);
        rep.initialize();
		try (RepositoryConnection conn = rep.getConnection()) {
			conn.add(subj, pred, obj, context);
			conn.commit();
		}
		try (RepositoryConnection conn = rep.getConnection()) {
			TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL,
					"select ?s ?p ?o from named <http://whatever/context/> where {<http://whatever/subj/> <http://whatever/pred/> \"whatever\"}");
			try (TupleQueryResult res = q.evaluate()) {
				assertFalse(res.hasNext());
			}
		}
        rep.shutDown();
    }

    @Test
	public void testEvaluateSelectService() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        HBaseSail sail = new HBaseSail(hconn, "whateverservice", true, 0, true, 0, null, null);
        SailRepository rep = new SailRepository(sail);
        rep.initialize();
        Random r = new Random(333);
        IRI pred = vf.createIRI("http://whatever/pred");
        IRI meta = vf.createIRI("http://whatever/meta");
		try (RepositoryConnection conn = rep.getConnection()) {
			for (int i = 0; i < 1000; i++) {
				IRI subj = vf.createIRI("http://whatever/subj#" + r.nextLong());
				IRI graph = vf.createIRI("http://whatever/grp#" + r.nextLong());
				conn.add(subj, pred, graph, meta);
				for (int j = 0; j < 10; j++) {
					IRI s = vf.createIRI("http://whatever/s#" + r.nextLong());
					IRI p = vf.createIRI("http://whatever/p#" + r.nextLong());
					IRI o = vf.createIRI("http://whatever/o#" + r.nextLong());
					conn.add(s, p, o, graph);
				}
			}
			conn.commit();
		}
        rep.shutDown();

        sail = new HBaseSail(hconn, "whateverparent", true, 0, true, 0, null, null);
        rep = new SailRepository(sail);
        rep.initialize();
		try (RepositoryConnection conn = rep.getConnection()) {
			TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL,
					"select * where {" + "  SERVICE <" + HALYARD.NAMESPACE + "whateverservice> {"
							+ "    graph <http://whatever/meta> {" + "      ?subj <http://whatever/pred> ?graph"
							+ "    }" + "    graph ?graph {" + "      ?s ?p ?o" + "    }" + "  }" + "}");
			int count = 0;
			try (TupleQueryResult res = q.evaluate()) {
				while (res.hasNext()) {
					count++;
					res.next();
				}
			}
			assertEquals(10000, count);
		}
        rep.shutDown();
    }

	/**
	 * Tests FederatedService.ask().
	 */
	@Test
	public void testEvaluateAskService() throws Exception {
		ValueFactory vf = SimpleValueFactory.getInstance();
		HBaseSail sail = new HBaseSail(hconn, "whateverservice", true, 0, true, 0, null, null);
		SailRepository rep = new SailRepository(sail);
		rep.initialize();
		try (RepositoryConnection conn = rep.getConnection()) {
			conn.add(vf.createIRI("http://whatever/subj"), vf.createIRI("http://whatever/pred"), vf.createIRI("http://whatever/obj"));
			conn.commit();
		}
		rep.shutDown();

		sail = new HBaseSail(hconn, "whateverparent", true, 0, true, 0, null, null);
		rep = new SailRepository(sail);
		rep.initialize();
		try (RepositoryConnection conn = rep.getConnection()) {
			conn.add(vf.createIRI("http://whatever/subj"), vf.createIRI("http://whatever/pred"), vf.createIRI("http://whatever/obj"));
			conn.commit();
			BooleanQuery q = conn.prepareBooleanQuery(QueryLanguage.SPARQL, "ask where {" + "    ?s ?p ?o" + "  SERVICE <" + HALYARD.NAMESPACE + "whateverservice> {" + "    ?s ?p ?o" + "  }" + "}");
			assertTrue(q.evaluate());
		}
		rep.shutDown();
	}

    @Test(expected = UnsupportedOperationException.class)
    public void testStatementsIteratorRemove1() throws Exception {
        HBaseSail sail = new HBaseSail(hconn, "whatevertable", true, 0, true, 0, null, null);
        try {
            sail.initialize();
			try (SailConnection conn = sail.getConnection()) {
				conn.getStatements(null, null, null, true).remove();
			}
        } finally {
            sail.shutDown();
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testStatementsIteratorRemove2() throws Exception {
        HBaseSail sail = new HBaseSail(hconn, "whatevertable", true, 0, true, 0, null, null);
        try {
            sail.initialize();
            ValueFactory vf = SimpleValueFactory.getInstance();
			try (SailConnection conn = sail.getConnection()) {
				conn.getStatements(vf.createIRI("http://whatever/subj/"), vf.createIRI("http://whatever/pred/"),
						vf.createIRI("http://whatever/obj/"), true).remove();
			}
        } finally {
            sail.shutDown();
        }
    }

    @Test
    public void testEmptyMethodsThatShouldDoNothing() throws Exception {
        HBaseSail sail = new HBaseSail(hconn, "whatevertable", true, 0, true, 0, null, null);
		try {
			sail.setDataDir(null);
			sail.initialize();
			try (SailConnection conn = sail.getConnection()) {
				conn.prepare();
				conn.begin();
				conn.flush();
				assertFalse(conn.pendingRemovals());
				conn.startUpdate(null);
				conn.endUpdate(null);
			}
		} finally {
			sail.shutDown();
		}
    }

    @Test(expected = SailException.class)
    public void testTimeoutGetStatements() throws Exception {
        HBaseSail sail = new HBaseSail(hconn, "whatevertable", true, 0, true, 1, null, null);
        sail.initialize();
        try {
			try (SailConnection conn = sail.getConnection()) {
				conn.commit();
				try (CloseableIteration<? extends Statement, SailException> it = conn.getStatements(null, null, null,
						true)) {
					Thread.sleep(2000);
					it.hasNext();
				}
			}
        } finally {
            sail.shutDown();
        }
    }

    @Test
    public void testCardinalityCalculator() throws Exception {
        HBaseSail sail = new HBaseSail(hconn, "cardinalitytable", true, 0, true, 0, null, null);
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
		try (SailConnection conn = sail.getConnection()) {
	        conn.addStatement(HALYARD.STATS_ROOT_NODE, VOID.TRIPLES, f.createLiteral(10000l), HALYARD.STATS_GRAPH_CONTEXT);
	        conn.addStatement(f.createIRI(HALYARD.STATS_ROOT_NODE.stringValue() + "_property_" + HalyardTableUtils.encode(HalyardTableUtils.hashKey(RDF.TYPE))), VOID.TRIPLES, f.createLiteral(5000l), HALYARD.STATS_GRAPH_CONTEXT);
	        conn.addStatement(f.createIRI("http://whatevercontext"), VOID.TRIPLES, f.createLiteral(10000l), HALYARD.STATS_GRAPH_CONTEXT);
	        conn.addStatement(f.createIRI("http://whatevercontext_property_" + HalyardTableUtils.encode(HalyardTableUtils.hashKey(RDF.TYPE))), VOID.TRIPLES, f.createLiteral(20l), HALYARD.STATS_GRAPH_CONTEXT);
	        conn.commit();
		}
        assertEquals(5000.0, sail.statistics.getCardinality(q1), 0.01);
        assertEquals(20.0, sail.statistics.getCardinality(q2), 0.01);
        assertEquals(100.0, sail.statistics.getCardinality(q3), 0.01);
        assertEquals(1.0, sail.statistics.getCardinality(q4), 0.01);
        sail.shutDown();
    }

    @Test
	public void testEvaluateSelectServiceWithBindings() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        HBaseSail sail = new HBaseSail(hconn, "whateverservice2", true, 0, true, 0, null, null);
        SailRepository rep = new SailRepository(sail);
        rep.initialize();
		try (RepositoryConnection conn = rep.getConnection()) {
			conn.add(vf.createIRI("http://whatever/subj"), vf.createIRI("http://whatever/pred"),
					vf.createIRI("http://whatever/obj"));
			conn.commit();
		}
		try (RepositoryConnection conn = rep.getConnection()) {
			TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL, "select * where {" + "  bind (\"a\" as ?a)\n"
					+ "  SERVICE <" + HALYARD.NAMESPACE + "whateverservice2> {" + "    ?s ?p ?o" + "  }" + "}");
			try (TupleQueryResult res = q.evaluate()) {
				assertTrue(res.hasNext());
				assertNotNull(res.next().getValue("a"));
			}
		}
        rep.shutDown();
    }

	/**
	 * Tests FederatedService.ask().
	 */
	@Test
	public void testEvaluateAskServiceWithBindings() throws Exception {
		ValueFactory vf = SimpleValueFactory.getInstance();
		HBaseSail sail = new HBaseSail(hconn, "whateverservice2", true, 0, true, 0, null, null);
		SailRepository rep = new SailRepository(sail);
		rep.initialize();
		try (RepositoryConnection conn = rep.getConnection()) {
			conn.add(vf.createIRI("http://whatever/subj"), vf.createIRI("http://whatever/pred"), vf.createIRI("http://whatever/obj"));
			conn.commit();
		}
		try (RepositoryConnection conn = rep.getConnection()) {
			BooleanQuery q = conn.prepareBooleanQuery(QueryLanguage.SPARQL, "ask where {" + "    ?s ?p ?o" + "  bind (\"a\" as ?a)\n" + "  SERVICE <" + HALYARD.NAMESPACE + "whateverservice2> {" + "    ?s ?p ?o" + "  }" + "}");
			assertTrue(q.evaluate());
		}
		rep.shutDown();
	}
}
