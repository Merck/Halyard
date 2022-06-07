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
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.vocab.HALYARD;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.eclipse.rdf4j.IsolationLevel;
import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
@RunWith(Parameterized.class)
public class HBaseSailTest {

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[] { true }, new Object[] { false });
	}

	private Set<String> usedTableNames = new HashSet<>();
	private Connection hconn;
	private boolean usePushStrategy;

	public HBaseSailTest(boolean usePushStrategy) {
		this.usePushStrategy = usePushStrategy;
	}

	@Before
    public void setup() throws Exception {
		hconn = HalyardTableUtils.getConnection(HBaseServerTestInstance.getInstanceConfig());
    }

	private String useTable(String tableName) {
		usedTableNames.add(tableName);
		return tableName;
	}

	@After
    public void teardown() throws Exception {
		Iterator<String> iter = usedTableNames.iterator();
		while (iter.hasNext()) {
			HalyardTableUtils.deleteTable(hconn, TableName.valueOf(iter.next()));
			iter.remove();
		}
		hconn.close();
    }

	@Test(expected = UnsupportedOperationException.class)
    public void testGetDataDir() throws Exception {
		HBaseSail sail = new HBaseSail(hconn, "whatevertable", true, 0, usePushStrategy, 0, null, null);
		sail.getDataDir();
    }

    @Test
    public void testInitializeAndShutDown() throws Exception {
		HBaseSail sail = new HBaseSail(hconn, useTable("whatevertable"), true, 0, usePushStrategy, 0, null, null);
        sail.initialize();
        sail.shutDown();
    }

    @Test
    public void testInitializeAndShutDownWithNoSharedConnection() throws Exception {
		HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), useTable("whatevertable"), true, 0, usePushStrategy, 0, null, null);
        sail.initialize();
        sail.shutDown();
    }

    @Test
    public void testIsWritable() throws Exception {
		HBaseSail sail = new HBaseSail(hconn, useTable("whatevertableRW"), true, 0, usePushStrategy, 0, null, null);
		sail.initialize();
		TableDescriptor desc;
		try {
			desc = hconn.getTable(sail.tableName).getDescriptor();
			assertTrue(sail.isWritable());
		} finally {
			sail.shutDown();
        }

		try (Admin ha = hconn.getAdmin()) {
			desc = TableDescriptorBuilder.newBuilder(desc).setReadOnly(true).build();
			ha.modifyTable(desc);
		}

		sail = new HBaseSail(hconn, desc.getTableName().getNameAsString(), true, 0, usePushStrategy, 0, null, null);
		sail.initialize();
		try {
			assertFalse(sail.isWritable());
		} finally {
			sail.shutDown();
        }
    }

    @Test(expected = SailException.class)
    public void testWriteToReadOnly() throws Exception {
		HBaseSail sail = new HBaseSail(hconn, useTable("whatevertableRO"), true, 0, usePushStrategy, 0, null, null);
        sail.initialize();
        try {
			TableDescriptor desc = hconn.getTable(sail.tableName).getDescriptor();
            try (Admin ha = hconn.getAdmin()) {
				desc = TableDescriptorBuilder.newBuilder(desc).setReadOnly(true).build();
				ha.modifyTable(desc);
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
		HBaseSail sail = new HBaseSail(hconn, useTable("whatevertable"), true, 0, usePushStrategy, 0, null, null);
		sail.initialize();
		try {
			try (SailConnection conn = sail.getConnection()) {
				assertTrue(conn.isOpen());
			}
		} finally {
			sail.shutDown();
		}
    }

    @Test
    public void testGetValueFactory() throws Exception {
		HBaseSail sail = new HBaseSail(hconn, useTable("whatevertable"), true, 0, usePushStrategy, 0, null, null);
		sail.initialize();
		try {
			assertNotNull(sail.getValueFactory());
		} finally {
			sail.shutDown();
		}
    }

    @Test
    public void testGetSupportedIsolationLevels() throws Exception {
		HBaseSail sail = new HBaseSail(hconn, "whatevertable", true, 0, usePushStrategy, 0, null, null);
		List<IsolationLevel> il = sail.getSupportedIsolationLevels();
        assertEquals(1, il.size());
        assertTrue(il.contains(IsolationLevels.NONE));
    }

    @Test
    public void testGetDefaultIsolationLevel() throws Exception {
		HBaseSail sail = new HBaseSail(hconn, "whatevertable", true, 0, usePushStrategy, 0, null, null);
		assertSame(IsolationLevels.NONE, sail.getDefaultIsolationLevel());
    }

    @Test
    public void testGetContextIDs() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
		HBaseSail sail = new HBaseSail(hconn, useTable("whatevertablectx"), true, 0, usePushStrategy,
				0, null, null);
        sail.initialize();
		try (SailConnection conn = sail.getConnection()) {
            conn.addStatement(HALYARD.STATS_ROOT_NODE, SD.NAMED_GRAPH_PROPERTY, vf.createIRI("http://whatever/ctx"), HALYARD.STATS_GRAPH_CONTEXT);
            try (CloseableIteration<? extends Resource, SailException> ctxIt = conn.getContextIDs()) {
                assertTrue(ctxIt.hasNext());
                assertEquals("http://whatever/ctx", ctxIt.next().stringValue());
            }
		} finally {
			sail.shutDown();
        }
    }

    @Test
    public void testSize() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
		HBaseSail sail = new HBaseSail(hconn, useTable("whatevertablesize"), true, 0, usePushStrategy,
				180, null, null);
        sail.initialize();
		try (SailConnection conn = sail.getConnection()) {
            assertEquals(0, conn.size());
            assertEquals(0, conn.size(HALYARD.STATS_ROOT_NODE));
            IRI iri = vf.createIRI("http://whatever/");
            conn.addStatement(iri, iri, iri);
            assertEquals(1, conn.size());
            conn.addStatement(HALYARD.STATS_ROOT_NODE, VOID.TRIPLES, vf.createLiteral(567), HALYARD.STATS_GRAPH_CONTEXT);
            assertEquals(567, conn.size());
            assertEquals(567, conn.size(HALYARD.STATS_ROOT_NODE));
            conn.addStatement(HALYARD.STATS_ROOT_NODE, VOID.TRIPLES, vf.createLiteral(568), HALYARD.STATS_GRAPH_CONTEXT);
            try {
            	conn.size();
                fail("Expected SailException");
            } catch (SailException se) {}
            try {
            	conn.size(HALYARD.STATS_ROOT_NODE);
                fail("Expected SailException");
            } catch (SailException se) {}
		} finally {
			sail.shutDown();
        }
    }

    @Test(expected = UnknownSailTransactionStateException.class)
    public void testBegin() throws Exception {
		HBaseSail sail = new HBaseSail(hconn, useTable("whatevertable"), true, 0, usePushStrategy, 0, null, null);
		sail.initialize();
		try {
			try (SailConnection conn = sail.getConnection()) {
				conn.begin(IsolationLevels.READ_COMMITTED);
			}
		} finally {
			sail.shutDown();
		}
    }

    @Test
    public void testRollback() throws Exception {
		HBaseSail sail = new HBaseSail(hconn, useTable("whatevertable"), true, 0, usePushStrategy, 0, null, null);
		sail.initialize();
		try (SailConnection conn = sail.getConnection()) {
			conn.rollback();
		} finally {
			sail.shutDown();
		}
    }

    @Test
    public void testIsActive() throws Exception {
		HBaseSail sail = new HBaseSail(hconn, useTable("whatevertable"), true, 0, usePushStrategy, 0, null, null);
		sail.initialize();
		try (SailConnection conn = sail.getConnection()) {
			assertTrue(conn.isActive());
		} finally {
			sail.shutDown();
		}
    }

    @Test
    public void testNamespaces() throws Exception {
		String tableName = useTable("whatevertable");
		HBaseSail sail = new HBaseSail(hconn, tableName, true, 0, usePushStrategy, 0, null, null);
        sail.initialize();
		try (SailConnection conn = sail.getConnection()) {
        	assertEquals(0, countNamespaces(conn));
        	conn.setNamespace("prefix", "http://whatever/namespace/");
        	assertEquals("http://whatever/namespace/", conn.getNamespace("prefix"));
        	assertEquals(1, countNamespaces(conn));
		} finally {
			sail.shutDown();
        }
		sail = new HBaseSail(hconn, tableName, false, 0, usePushStrategy, 0, null, null);
        sail.initialize();
		try (SailConnection conn = sail.getConnection()) {
        	assertEquals(1, countNamespaces(conn));
	        conn.removeNamespace("prefix");
        	assertNull(conn.getNamespace("prefix"));
        	assertEquals(0, countNamespaces(conn));
		} finally {
			sail.shutDown();
        }
		sail = new HBaseSail(hconn, tableName, false, 0, usePushStrategy, 0, null, null);
        sail.initialize();
		try (SailConnection conn = sail.getConnection()) {
        	assertEquals(0, countNamespaces(conn));
	        conn.setNamespace("prefix", "http://whatever/namespace/");
	        conn.setNamespace("prefix", "http://whatever/namespace2/");
        	assertEquals("http://whatever/namespace2/", conn.getNamespace("prefix"));
        	assertEquals(1, countNamespaces(conn));
		} finally {
			sail.shutDown();
        }
		sail = new HBaseSail(hconn, tableName, false, 0, usePushStrategy, 0, null, null);
        sail.initialize();
		try (SailConnection conn = sail.getConnection()) {
        	assertEquals(1, countNamespaces(conn));
        	assertEquals("http://whatever/namespace2/", conn.getNamespace("prefix"));
        	conn.clearNamespaces();
        	assertEquals(0, countNamespaces(conn));
		} finally {
			sail.shutDown();
        }
		sail = new HBaseSail(hconn, tableName, false, 0, usePushStrategy, 0, null, null);
        sail.initialize();
		try (SailConnection conn = sail.getConnection()) {
        	assertEquals(0, countNamespaces(conn));
		} finally {
			sail.shutDown();
        }
    }

    private int countNamespaces(SailConnection conn) {
    	int count ;
    	try (CloseableIteration<? extends Namespace, SailException> iter = conn.getNamespaces()) {
    		for(count=0; iter.hasNext(); iter.next()) {
    			count++;
    		}
    	}
    	return count;
    }

    @Test
    public void testClear() throws Exception {
		String tableName = useTable("whatevertableClear");
        ValueFactory vf = SimpleValueFactory.getInstance();
        Resource subj = vf.createIRI("http://whatever/subj/");
        IRI pred = vf.createIRI("http://whatever/pred/");
        Value obj = vf.createLiteral("whatever");
        IRI context = vf.createIRI("http://whatever/context/");
        CloseableIteration<? extends Statement, SailException> iter;
		HBaseSail sail = new HBaseSail(hconn, tableName, true, 0, usePushStrategy, 0, null, null);
        sail.initialize();
		try (SailConnection conn = sail.getConnection()) {
	        conn.addStatement(subj, pred, obj, context);
	        conn.addStatement(subj, pred, obj);
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
		} finally {
			sail.shutDown();
        }
		// check sail can be re-initialized after clear()
		sail = new HBaseSail(hconn, tableName, false, 0, usePushStrategy, 0, null, null);
		sail.initialize();
		sail.shutDown();
    }

    @Test
    public void testEvaluate() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Resource subj = vf.createIRI("http://whatever/subj/");
        IRI pred = vf.createIRI("http://whatever/pred/");
        Value obj = vf.createLiteral("whatever");
		HBaseSail sail = new HBaseSail(hconn, useTable("whatevertable"), true, 0, usePushStrategy, 0, null, null);
        SailRepository rep = new SailRepository(sail);
        rep.init();
		try (RepositoryConnection conn = rep.getConnection()) {
			conn.add(subj, pred, obj);
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
		HBaseSail sail = new HBaseSail(hconn, useTable("whatevertable"), true, 0, usePushStrategy, 0, null, null);
		SailRepository rep = new SailRepository(sail);
		rep.init();
		try (RepositoryConnection conn = rep.getConnection()) {
			conn.add(subj, pred, obj);
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
		HBaseSail sail = new HBaseSail(hconn, useTable("whatevertable"), true, 0, usePushStrategy, 0, null, null);
        SailRepository rep = new SailRepository(sail);
        rep.init();
		try (RepositoryConnection conn = rep.getConnection()) {
			conn.add(subj, pred, obj, context);
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
		HBaseSail sail = new HBaseSail(hconn, useTable("whateverservice"), true, 0, usePushStrategy, 0, null, null);
        SailRepository rep = new SailRepository(sail);
        rep.init();
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
		}
        rep.shutDown();

		sail = new HBaseSail(hconn, useTable("whateverparent"), true, 0, usePushStrategy, 0, null, null);
        rep = new SailRepository(sail);
        rep.init();
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

	@Test
	public void testEvaluateServiceSameTable() throws Exception {
		String tableName = useTable("whateverservicesametable");
		ValueFactory vf = SimpleValueFactory.getInstance();
		HBaseSail sail = new HBaseSail(hconn, tableName, true, 0, usePushStrategy, 0, null, null);
		SailRepository rep = new SailRepository(sail);
		rep.init();
		Random r = new Random(458);
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
		}
		rep.shutDown();

		sail = new HBaseSail(hconn, tableName, true, 0, usePushStrategy, 0, null, null);
		rep = new SailRepository(sail);
		rep.init();
		try (RepositoryConnection conn = rep.getConnection()) {
			TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL, "select * where {" + "  SERVICE <" + HALYARD.NAMESPACE + "> {" + "    graph <http://whatever/meta> {" + "      ?subj <http://whatever/pred> ?graph"
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
		HBaseSail sail = new HBaseSail(hconn, useTable("whateverservice"), true, 0, usePushStrategy, 0, null, null);
		SailRepository rep = new SailRepository(sail);
		rep.init();
		try (RepositoryConnection conn = rep.getConnection()) {
			conn.add(vf.createIRI("http://whatever/subj"), vf.createIRI("http://whatever/pred"), vf.createIRI("http://whatever/obj"));
		}
		rep.shutDown();

		sail = new HBaseSail(hconn, useTable("whateverparent"), true, 0, usePushStrategy, 0, null, null);
		rep = new SailRepository(sail);
		rep.init();
		try (RepositoryConnection conn = rep.getConnection()) {
			conn.add(vf.createIRI("http://whatever/subj"), vf.createIRI("http://whatever/pred"), vf.createIRI("http://whatever/obj"));
			BooleanQuery q = conn.prepareBooleanQuery(QueryLanguage.SPARQL, "ask where {" + "    ?s ?p ?o" + "  SERVICE <" + HALYARD.NAMESPACE + "whateverservice> {" + "    ?s ?p ?o" + "  }" + "}");
			assertTrue(q.evaluate());
		}
		rep.shutDown();
	}

    @Test(expected = UnsupportedOperationException.class)
    public void testStatementsIteratorRemove1() throws Exception {
		HBaseSail sail = new HBaseSail(hconn, useTable("whatevertable"), true, 0, usePushStrategy, 0, null, null);
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
		HBaseSail sail = new HBaseSail(hconn, useTable("whatevertable"), true, 0, usePushStrategy, 0, null, null);
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
		HBaseSail sail = new HBaseSail(hconn, useTable("whatevertable"), true, 0, usePushStrategy, 0, null, null);
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
		HBaseSail sail = new HBaseSail(hconn, useTable("whatevertable"), true, 0, usePushStrategy, 1, null, null);
        sail.initialize();
        try {
			try (SailConnection conn = sail.getConnection()) {
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
		HBaseSail sail = new HBaseSail(hconn, useTable("cardinalitytable"), true, 0, usePushStrategy, 0, null, null);
        sail.initialize();
		RDFFactory rdfFactory = sail.getRDFFactory();
        SimpleValueFactory f = SimpleValueFactory.getInstance();
        TupleExpr q1 = QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL, "select * where {?s a ?o}", "http://whatever/").getTupleExpr();
        TupleExpr q2 = QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL, "select * where {graph <http://whatevercontext> {?s a ?o}}", "http://whatever/").getTupleExpr();
        TupleExpr q3 = QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL, "select * where {?s <http://whatever/> ?o}", "http://whatever/").getTupleExpr();
        TupleExpr q4 = QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL, "select * where {?s ?p \"whatever\"^^<" + HALYARD.SEARCH_TYPE.stringValue() + ">}", "http://whatever/").getTupleExpr();
        assertEquals(100.0, sail.statistics.getCardinality(q1), 0.01);
        assertEquals(100.0, sail.statistics.getCardinality(q2), 0.01);
        assertEquals(100.0, sail.statistics.getCardinality(q3), 0.01);
        assertEquals(0.0001, sail.statistics.getCardinality(q4), 0.00001);
		try (SailConnection conn = sail.getConnection()) {
	        conn.addStatement(HALYARD.STATS_ROOT_NODE, VOID.TRIPLES, f.createLiteral(10000l), HALYARD.STATS_GRAPH_CONTEXT);
			conn.addStatement(f.createIRI(HALYARD.STATS_ROOT_NODE.stringValue() + "_property_" + rdfFactory.id(RDF.TYPE)), VOID.TRIPLES, f.createLiteral(5000l), HALYARD.STATS_GRAPH_CONTEXT);
	        conn.addStatement(f.createIRI("http://whatevercontext"), VOID.TRIPLES, f.createLiteral(10000l), HALYARD.STATS_GRAPH_CONTEXT);
			conn.addStatement(f.createIRI("http://whatevercontext_property_" + rdfFactory.id(RDF.TYPE)), VOID.TRIPLES, f.createLiteral(20l), HALYARD.STATS_GRAPH_CONTEXT);
		}
        assertEquals(5000.0, sail.statistics.getCardinality(q1), 0.01);
        assertEquals(20.0, sail.statistics.getCardinality(q2), 0.01);
        assertEquals(100.0, sail.statistics.getCardinality(q3), 0.01);
        assertEquals(0.0001, sail.statistics.getCardinality(q4), 0.00001);
        sail.shutDown();
    }

    @Test
	public void testEvaluateSelectServiceWithBindings() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
		HBaseSail sail = new HBaseSail(hconn, useTable("whateverservice2"), true, 0, usePushStrategy, 0, null, null);
        SailRepository rep = new SailRepository(sail);
        rep.init();
		try (RepositoryConnection conn = rep.getConnection()) {
			conn.add(vf.createIRI("http://whatever/subj"), vf.createIRI("http://whatever/pred"),
					vf.createIRI("http://whatever/obj"));
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
		HBaseSail sail = new HBaseSail(hconn, useTable("whateverservice2"), true, 0, usePushStrategy, 0, null, null);
		SailRepository rep = new SailRepository(sail);
		rep.init();
		try (RepositoryConnection conn = rep.getConnection()) {
			conn.add(vf.createIRI("http://whatever/subj"), vf.createIRI("http://whatever/pred"), vf.createIRI("http://whatever/obj"));
		}
		try (RepositoryConnection conn = rep.getConnection()) {
			BooleanQuery q = conn.prepareBooleanQuery(QueryLanguage.SPARQL, "ask where {" + "    ?s ?p ?o" + "  bind (\"a\" as ?a)\n" + "  SERVICE <" + HALYARD.NAMESPACE + "whateverservice2> {" + "    ?s ?p ?o" + "  }" + "}");
			assertTrue(q.evaluate());
		}
		rep.shutDown();
	}

    @Test
    public void testBindWithFilter() throws Exception {
		HBaseSail sail = new HBaseSail(hconn, useTable("empty"), true, 0, usePushStrategy, 0, null, null);
        SailRepository rep = new SailRepository(sail);
        rep.init();
		try (RepositoryConnection conn = rep.getConnection()) {
			TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT ?x WHERE {BIND (\"x\" AS ?x)\n  FILTER (?x = \"x\")}");
			try (TupleQueryResult res = q.evaluate()) {
				assertTrue(res.hasNext());
				assertEquals("x", res.next().getBinding("x").getValue().stringValue());
			}
		}
        rep.shutDown();
    }

	@Test
	public void testAddDeleteTriple() throws Exception {
		String table = "whatevertable";
		HBaseSail sail = new HBaseSail(hconn, useTable(table), true, 0, usePushStrategy, 0, null, null);
		sail.initialize();
		ValueFactory vf = sail.getValueFactory();
		Triple t = vf.createTriple(vf.createIRI("http://whatever/subj"), vf.createIRI("http://whatever/pred"), vf.createLiteral("whatever"));
		Triple t1 = vf.createTriple(vf.createIRI("http://whatever/subj1"), vf.createIRI("http://whatever/pred1"), t);
		Triple t2 = vf.createTriple(vf.createIRI("http://whatever/subj2"), vf.createIRI("http://whatever/pred2"), t);
		try (SailConnection conn = sail.getConnection()) {
			conn.addStatement(t1, RDFS.COMMENT, vf.createLiteral(1));
			conn.addStatement(t2, RDFS.COMMENT, vf.createLiteral(2));
			assertCount(conn, 2);
			assertTripleCount(conn, 3);
			conn.removeStatements(t2, RDFS.COMMENT, vf.createLiteral(2));
			assertCount(conn, 1);
			assertTripleCount(conn, 2);
			conn.removeStatements(t1, RDFS.COMMENT, vf.createLiteral(1));
			assertCount(conn, 0);
			assertTripleCount(conn, 0);
		}
		sail.shutDown();
	}

	@Test
    public void testSnapshot() throws Exception {
		String table = "whatevertable";
		HBaseSail sail = new HBaseSail(hconn, useTable(table), true, 0, usePushStrategy, 0, null, null);
		sail.initialize();
		ValueFactory vf = sail.getValueFactory();
		try (SailConnection conn = sail.getConnection()) {
			conn.addStatement(vf.createIRI("http://whatever/subj"), vf.createIRI("http://whatever/pred"), vf.createLiteral("whatever"));
		}
		sail.shutDown();

		String snapshot = table + "Snapshot";
		try (Admin admin = hconn.getAdmin()) {
			admin.snapshot(snapshot, TableName.valueOf(table));
		}

		File restorePath = File.createTempFile("snapshot", "");
		restorePath.delete();
		restorePath.deleteOnExit();

		sail = new HBaseSail(hconn.getConfiguration(), snapshot, restorePath.toURI().toURL().toString(), usePushStrategy, 0, null, null);
		sail.initialize();
		try (SailConnection conn = sail.getConnection()) {
			try (CloseableIteration<? extends Statement, SailException> iter = conn.getStatements(null, null, null, false)) {
				assertTrue(iter.hasNext());
			}
		}
		sail.shutDown();

		try (Admin admin = hconn.getAdmin()) {
			admin.deleteSnapshot(snapshot);
		}
	}

	private static void assertCount(SailConnection conn, int expected) throws Exception {
		int count = 0;
		try (CloseableIteration<? extends Statement, SailException> iter = conn.getStatements(null, null, null, true)) {
			while (iter.hasNext()) {
				iter.next();
				count++;
			}
		}
		Assert.assertEquals(expected, count);
	}

	private static void assertTripleCount(SailConnection conn, int expected) throws Exception {
		int count = 0;
		try (CloseableIteration<? extends Statement, SailException> iter = conn.getStatements(null, null, null, true, HALYARD.TRIPLE_GRAPH_CONTEXT)) {
			while (iter.hasNext()) {
				iter.next();
				count++;
			}
		}
		Assert.assertEquals(expected, count);
	}
}
