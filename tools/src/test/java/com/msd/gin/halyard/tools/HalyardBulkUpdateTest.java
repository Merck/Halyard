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
import java.util.Iterator;
import java.util.Random;
import java.util.TreeMap;
import org.apache.commons.cli.MissingOptionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.LinkedHashModelFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.SailException;
import org.junit.Assert;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardBulkUpdateTest {

    private static final String TABLE = "bulkupdatetesttable";

    @Test
    public void testBulkUpdate() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Configuration conf = HBaseServerTestInstance.getInstanceConfig();
        HBaseSail sail = new HBaseSail(conf, TABLE, true, -1, true, 0, null, null);
        sail.initialize();
        for (int i=0; i<5; i++) {
            for (int j=0; j<5; j++) {
                sail.addStatement(vf.createIRI("http://whatever/subj" + i), vf.createIRI("http://whatever/pred"), vf.createIRI("http://whatever/obj" + j));
            }
        }
        sail.commit();
        sail.shutDown();
        File queries = File.createTempFile("test_update_queries", "");
        queries.delete();
        queries.mkdir();
        File q = new File(queries, "test_update_query.sparql");
        q.deleteOnExit();
        try (PrintStream qs = new PrintStream(q)) {
            qs.println("PREFIX halyard: <http://merck.github.io/Halyard/ns#>\n"
                + "delete {?s <http://whatever/pred> ?o}\n"
                + "insert {?o <http://whatever/reverse> ?s}\n"
                + "where {?s <http://whatever/pred> ?o . FILTER (halyard:forkAndFilterBy(2, ?s, ?o))};"
                + "insert {?s <http://whatever/another> ?o}\n"
                + "where {?s <http://whatever/reverse> ?o . FILTER (halyard:forkAndFilterBy(3, ?s, ?o))}");
        }
        File htableDir = File.createTempFile("test_htable", "");
        htableDir.delete();

        assertEquals(0, ToolRunner.run(conf, new HalyardBulkUpdate(), new String[]{ "-q", queries.toURI().toURL().toString(), "-w", htableDir.toURI().toURL().toString(), "-s", TABLE}));

        q.delete();
        queries.delete();

        sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), TABLE, false, 0, true, 0, null, null);
        sail.initialize();
        try {
            int count;
            try (CloseableIteration<? extends Statement, SailException> iter = sail.getStatements(null, null, null, true)) {
                count = 0;
                while (iter.hasNext()) {
                    iter.next();
                    count++;
                }
            }
            Assert.assertEquals(50, count);
            Assert.assertFalse(sail.getStatements(null, SimpleValueFactory.getInstance().createIRI("http://whatever/pred"), null, true).hasNext());
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
        //generate inserts and deletes and create reference model
        TreeMap<Integer, Change> changes = new TreeMap<>();
        IRI targetGraph = SimpleValueFactory.getInstance().createIRI("http://whatever/targetGraph");
        IRI subj[] = new IRI[5];
        for (int i=0; i<subj.length; i++) {
            subj[i] = SimpleValueFactory.getInstance().createIRI("http://whtever/subj#" + i);
        }
        IRI pred[] = new IRI[5];
        for (int i=0; i<pred.length; i++) {
            pred[i] = SimpleValueFactory.getInstance().createIRI("http://whtever/pred#" + i);
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
                Statement s = SimpleValueFactory.getInstance().createStatement(subj[r.nextInt(subj.length)], pred[r.nextInt(pred.length)], SimpleValueFactory.getInstance().createLiteral(r.nextInt(100)), targetGraph);
                ch.inserts.add(s);
                referenceModel.add(s);
            }
            //shuffle the changes
            changes.put(r.nextInt(), ch);
        }

        //fill the change graph with change events
        Configuration conf = HBaseServerTestInstance.getInstanceConfig();
        HBaseSail sail = new HBaseSail(conf, "timebulkupdatetesttable", true, -1, true, 0, null, null);
        sail.initialize();
        int i=0;
        IRI timstamp = SimpleValueFactory.getInstance().createIRI("http://whatever/timestamp");
        IRI deleteGraph = SimpleValueFactory.getInstance().createIRI("http://whatever/deleteGraph");
        IRI insertGraph = SimpleValueFactory.getInstance().createIRI("http://whatever/insertGraph");
        IRI context = SimpleValueFactory.getInstance().createIRI("http://whatever/context");
        for (Change c : changes.values()) {
            IRI chSubj = SimpleValueFactory.getInstance().createIRI("http://whatever/change#" + i);
            IRI delGr = SimpleValueFactory.getInstance().createIRI("http://whatever/graph#" + i + "d");
            IRI insGr = SimpleValueFactory.getInstance().createIRI("http://whatever/graph#" + i + "i");
            sail.addStatement(chSubj, timstamp, SimpleValueFactory.getInstance().createLiteral(c.timestamp));
            sail.addStatement(chSubj, context, targetGraph);
            sail.addStatement(chSubj, deleteGraph, delGr);
            sail.addStatement(chSubj, insertGraph, insGr);
            for (Statement s : c.deletes) {
                sail.addStatement(s.getSubject(), s.getPredicate(), s.getObject(), delGr);
            }
            for (Statement s : c.inserts) {
                sail.addStatement(s.getSubject(), s.getPredicate(), s.getObject(), insGr);
            }
            i++;
        }
        sail.commit();
        sail.shutDown();

        //prepare the update queries
        File queries = File.createTempFile("test_time_update_queries", "");
        queries.delete();
        queries.mkdir();
        File q1 = new File(queries, "test_time_update_query_delete.sparql");
        File q2 = new File(queries, "test_time_update_query_insert.sparql");
        q1.deleteOnExit();
        q2.deleteOnExit();
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
                        "          :timestamp ?HALYARD_TIMESTAMP_SPECIAL_VARIABLE ." +
                        "  OPTIONAL {" +
                        "    ?change :deleteGraph ?delGr ." +
                        "    GRAPH ?delGr {" +
                        "      ?deleteSubj ?deletePred ?deleteObj ." +
                        "    }" +
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
                        "}" +
                        "WHERE {" +
                        "  ?change :context   ?targetGraph ;" +
                        "          :timestamp ?HALYARD_TIMESTAMP_SPECIAL_VARIABLE ." +
                        "  FILTER (halyard:forkAndFilterBy(2, ?change))" +
                        "  OPTIONAL {" +
                        "    ?change :insertGraph ?insGr ." +
                        "    GRAPH ?insGr {" +
                        "      ?insertSubj ?insertPred ?insertObj ." +
                        "    }" +
                        "  }" +
                        "}");
        }
        File htableDir = File.createTempFile("test_htable", "");
        htableDir.delete();

        //execute BulkUpdate
        assertEquals(0, ToolRunner.run(conf, new HalyardBulkUpdate(), new String[]{ "-q", queries.toURI().toURL().toString(), "-w", htableDir.toURI().toURL().toString(), "-s", "timebulkupdatetesttable"}));

        q1.delete();
        q2.delete();
        queries.delete();

        //read transformed data into model
        LinkedHashModel resultModel = new LinkedHashModelFactory().createEmptyModel();
        sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "timebulkupdatetesttable", false, 0, true, 0, null, null);
        sail.initialize();
        try {
            try (CloseableIteration<? extends Statement, SailException> iter = sail.getStatements(null, null, null, true, targetGraph)) {
                while (iter.hasNext()) {
                    resultModel.add(iter.next());
                }
            }
        } finally {
            sail.shutDown();
        }

        //compare the models
        TreeMap<String,String> fail = new TreeMap<>();
        Iterator<Statement> it = referenceModel.iterator();
        while (it.hasNext()) {
            Statement st = it.next();
            if (!resultModel.contains(st)) {
                fail.put(st.toString(), "-" + st.toString());
            }
        }
        it = resultModel.iterator();
        while (it.hasNext()) {
            Statement st = it.next();
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

    @Test
    public void testHelp() throws Exception {
        assertEquals(-1, new HalyardBulkUpdate().run(new String[]{"-h"}));
    }

    @Test(expected = MissingOptionException.class)
    public void testRunNoArgs() throws Exception {
        assertEquals(-1, new HalyardBulkUpdate().run(new String[]{}));
    }
}
