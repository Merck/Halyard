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
package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.sail.HBaseSail;

import java.io.File;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Random;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.LinkedHashModelFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardBulkUpdateTest extends AbstractHalyardToolTest {
    private static final String TABLE = "bulkupdatetesttable";

	@Override
	protected AbstractHalyardTool createTool() {
		return new HalyardBulkUpdate();
	}

    @Test
    public void testBulkUpdate() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Configuration conf = HBaseServerTestInstance.getInstanceConfig();
        HBaseSail sail = new HBaseSail(conf, TABLE, true, -1, true, 0, null, null);
        sail.init();
		try (SailConnection conn = sail.getConnection()) {
			for (int i = 0; i < 5; i++) {
				for (int j = 0; j < 5; j++) {
					conn.addStatement(vf.createIRI("http://whatever/subj" + i), vf.createIRI("http://whatever/pred"), vf.createIRI("http://whatever/obj" + j));
				}
			}
		}
        sail.shutDown();
        File queries = createTempDir("test_update_queries");
        File q = new File(queries, "test_update_query.sparql");
        try (PrintStream qs = new PrintStream(q)) {
            qs.println("PREFIX halyard: <http://merck.github.io/Halyard/ns#>\n"
                + "delete {?s <http://whatever/pred> ?o}\n"
                + "insert {?o <http://whatever/reverse> ?s}\n"
                + "where {?s <http://whatever/pred> ?o . FILTER (halyard:forkAndFilterBy(2, ?s, ?o))};"
                + "insert {?s <http://whatever/another> ?o}\n"
                + "where {?s <http://whatever/reverse> ?o . FILTER (halyard:forkAndFilterBy(3, ?s, ?o))}");
        }
        File htableDir = getTempHTableDir("test_htable");

        assertEquals(0, run(new String[]{ "-q", queries.toURI().toURL().toString(), "-w", htableDir.toURI().toURL().toString(), "-s", TABLE}));

        q.delete();
        queries.delete();

        sail = new HBaseSail(conf, TABLE, false, 0, true, 0, null, null);
        sail.init();
        try {
			try (SailConnection conn = sail.getConnection()) {
				int count;
				try (CloseableIteration<? extends Statement, SailException> iter = conn.getStatements(null, null, null, true)) {
					count = 0;
					while (iter.hasNext()) {
						iter.next();
						count++;
					}
				}
				Assert.assertEquals(50, count);
				try (CloseableIteration<? extends Statement, SailException> iter = conn.getStatements(null, vf.createIRI("http://whatever/pred"), null, true)) {
					Assert.assertFalse(iter.hasNext());
				}
			}
        } finally {
            sail.shutDown();
        }
    }

    private static class Change {
        int timestamp;
        HashSet<Statement> inserts = new HashSet<>();
        HashSet<Statement> deletes = new HashSet<>();
    }

    @Test
    public void testTimeAwareBulkUpdate() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        //generate inserts and deletes and create reference model
        TreeMap<Integer, Change> changes = new TreeMap<>();
        IRI targetGraph = vf.createIRI("http://whatever/targetGraph");
        IRI subj[] = new IRI[5];
        for (int i=0; i<subj.length; i++) {
            subj[i] = vf.createIRI("http://whtever/subj#" + i);
        }
        IRI pred[] = new IRI[5];
        for (int i=0; i<pred.length; i++) {
            pred[i] = vf.createIRI("http://whtever/pred#" + i);
        }
        LinkedHashModel referenceModel = new LinkedHashModelFactory().createEmptyModel();
        Random r = new Random(1234);
        for (int i=0; i<1000; i++) {
            int ds = referenceModel.size();
            Change ch = new Change();
            ch.timestamp = i;
            if (ds > 0) for (Statement s : referenceModel) {
                if (r.nextInt(ds) > 20) {
                    ch.deletes.add(s);
                }
            }
            for (Statement s : ch.deletes) {
                referenceModel.remove(s);
            }
            for (int j=0; j<r.nextInt(40); j++) {
                Statement s = vf.createStatement(subj[r.nextInt(subj.length)], pred[r.nextInt(pred.length)], vf.createLiteral(r.nextInt(100)), targetGraph);
                ch.inserts.add(s);
                referenceModel.add(s);
            }
            //shuffle the changes
            changes.put(r.nextInt(), ch);
        }

        //fill the change graph with change events
        Configuration conf = HBaseServerTestInstance.getInstanceConfig();
        HBaseSail sail = new HBaseSail(conf, "timebulkupdatetesttable", true, -1, true, 0, null, null);
        sail.init();
        int i=0;
        IRI timestamp = vf.createIRI("http://whatever/timestamp");
        IRI deleteGraph = vf.createIRI("http://whatever/deleteGraph");
        IRI insertGraph = vf.createIRI("http://whatever/insertGraph");
        IRI context = vf.createIRI("http://whatever/context");
		try (SailConnection conn = sail.getConnection()) {
			for (Change c : changes.values()) {
				IRI chSubj = vf.createIRI("http://whatever/change#" + i);
				IRI delGr = vf.createIRI("http://whatever/graph#" + i + "d");
				IRI insGr = vf.createIRI("http://whatever/graph#" + i + "i");
				conn.addStatement(chSubj, timestamp, vf.createLiteral(c.timestamp));
				conn.addStatement(chSubj, context, targetGraph);
				conn.addStatement(chSubj, deleteGraph, delGr);
				conn.addStatement(chSubj, insertGraph, insGr);
				for (Statement s : c.deletes) {
					conn.addStatement(s.getSubject(), s.getPredicate(), s.getObject(), delGr);
				}
				for (Statement s : c.inserts) {
					conn.addStatement(s.getSubject(), s.getPredicate(), s.getObject(), insGr);
				}
				i++;
			}
		}
        sail.shutDown();

        //prepare the update queries
        File queries = createTempDir("test_time_update_queries");
        File q1 = new File(queries, "test_time_update_query_delete.sparql");
        File q2 = new File(queries, "test_time_update_query_insert.sparql");
        try (PrintStream qs = new PrintStream(q1)) {
            qs.println( "PREFIX : <http://whatever/> " +
                        "PREFIX halyard: <http://merck.github.io/Halyard/ns#> " +
                        "DELETE {" +
                        "  GRAPH ?targetGraph {" +
                        "    ?deleteSubj ?deletePred ?deleteObj ." +
                        "  }" +
                        "}" +
                        "WHERE {" +
                        "  ?change :context   ?targetGraph ;" +
                        "          :timestamp ?t ." +
                        "  OPTIONAL {" +
                        "    ?change :deleteGraph ?delGr ." +
                        "    GRAPH ?delGr {" +
                        "      ?deleteSubj ?deletePred ?deleteObj ." +
                        "    }" +
                        "    (?deleteSubj ?deletePred ?deleteObj ?targetGraph) halyard:timestamp ?t ." +
                        "  }" +
                        "}");
        }
        try (PrintStream qs = new PrintStream(q2)) {
            qs.println( "PREFIX : <http://whatever/> " +
                        "PREFIX halyard: <http://merck.github.io/Halyard/ns#> " +
                        "INSERT {" +
                        "  GRAPH ?targetGraph {" +
                        "    ?insertSubj ?insertPred ?insertObj ." +
                        "  }" +
                        "  (?insertSubj ?insertPred ?insertObj ?targetGraph) halyard:timestamp ?t ." +
                        "}" +
                        "WHERE {" +
                        "  ?change :context   ?targetGraph ;" +
                        "          :timestamp ?t ." +
                        "  FILTER (halyard:forkAndFilterBy(2, ?change))" +
                        "  OPTIONAL {" +
                        "    ?change :insertGraph ?insGr ." +
                        "    GRAPH ?insGr {" +
                        "      ?insertSubj ?insertPred ?insertObj ." +
                        "    }" +
                        "  }" +
                        "}");
        }
        File htableDir = getTempHTableDir("test_htable");

        //execute BulkUpdate
        assertEquals(0, run(new String[]{ "-q", queries.toURI().toURL().toString(), "-w", htableDir.toURI().toURL().toString(), "-s", "timebulkupdatetesttable"}));

        q1.delete();
        q2.delete();
        queries.delete();

        //read transformed data into model
        LinkedHashModel resultModel = new LinkedHashModelFactory().createEmptyModel();
        sail = new HBaseSail(conf, "timebulkupdatetesttable", false, 0, true, 0, null, null);
        sail.init();
        try {
			try (SailConnection conn = sail.getConnection()) {
				try (CloseableIteration<? extends Statement, SailException> iter = conn.getStatements(null, null, null, true, targetGraph)) {
					while (iter.hasNext()) {
						resultModel.add(iter.next());
					}
				}
			}
        } finally {
            sail.shutDown();
        }

        //compare the models
        TreeMap<String,String> fail = new TreeMap<>();
        for (Statement st : (Iterable<Statement>) referenceModel) {
            if (!resultModel.contains(st)) {
                fail.put(st.toString(), "-" + st.toString());
            }
        }
        for (Statement st : (Iterable<Statement>) resultModel) {
            if (!referenceModel.contains(st)) {
                fail.put(st.toString(), "+" + st.toString());
            }
        }
        if (fail.size() > 0) {
            StringBuilder sb = new StringBuilder();
            sb.append('\n');
            for (String line : fail.values()) {
                sb.append(line).append('\n');
            }
            Assert.fail(sb.toString());
        }
    }
}
