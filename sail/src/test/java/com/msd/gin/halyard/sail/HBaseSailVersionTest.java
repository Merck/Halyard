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
import com.msd.gin.halyard.repository.HBaseRepository;

import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.LinkedHashModelFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class HBaseSailVersionTest {

	private Connection hconn;

    @Before
    public void setup() throws Exception {
		hconn = HalyardTableUtils.getConnection(HBaseServerTestInstance.getInstanceConfig());
    }

    @After
    public void teardown() throws Exception {
		hconn.close();
    }

    @Test
	public void testEvaluate1() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
		Literal startDate = vf.createLiteral(new Date());
        Resource subj = vf.createIRI("http://whatever/subj/");
        IRI pred = vf.createIRI("http://whatever/pred/");
        Value obj = vf.createLiteral("whatever");
        HBaseSail sail = new HBaseSail(hconn, "whatevertable", true, 0, true, 0, null, null);
        SailRepository rep = new SailRepository(sail);
        rep.init();
		try (RepositoryConnection conn = rep.getConnection()) {
			conn.add(subj, pred, obj);
		}
		try (RepositoryConnection conn = rep.getConnection()) {
			TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL,
					"prefix halyard: <http://merck.github.io/Halyard/ns#>\nselect ?s ?p ?t where {?s ?p \"whatever\". (?s ?p \"whatever\") halyard:timestamp ?t}");
			try (TupleQueryResult res = q.evaluate()) {
				assertTrue(res.hasNext());
				BindingSet bs = res.next();
				Literal t = (Literal) bs.getValue("t");
				assertNotNull("Missing binding", t);
				assertTrue(String.format("Expected %s > %s", t, startDate), t.calendarValue().compare(startDate.calendarValue()) > 0);
			}
		}
        rep.shutDown();
    }

    @Test
	public void testEvaluate2() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
		Literal startDate = vf.createLiteral(new Date());
        Resource subj = vf.createIRI("http://whatever/subj/");
        IRI pred = vf.createIRI("http://whatever/pred/");
        Value obj = vf.createLiteral("whatever");
        HBaseSail sail = new HBaseSail(hconn, "whatevertable", true, 0, true, 0, null, null);
        SailRepository rep = new SailRepository(sail);
        rep.init();
		try (RepositoryConnection conn = rep.getConnection()) {
			conn.add(subj, pred, obj);
		}
		try (RepositoryConnection conn = rep.getConnection()) {
			TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL,
					"prefix halyard: <http://merck.github.io/Halyard/ns#>\nselect ?t where {(<http://whatever/subj/> <http://whatever/pred/> \"whatever\") halyard:timestamp ?t}");
			try (TupleQueryResult res = q.evaluate()) {
				assertTrue(res.hasNext());
				BindingSet bs = res.next();
				assertTrue(((Literal) bs.getValue("t")).calendarValue().compare(startDate.calendarValue()) > 0);
			}
		}
        rep.shutDown();
    }

    @Test
    public void testModify() throws Exception {
		TableName htableName = TableName.valueOf("timestamptable");
		try (Admin admin = hconn.getAdmin()) {
			ColumnFamilyDescriptor cd = ColumnFamilyDescriptorBuilder.newBuilder("e".getBytes()).setMaxVersions(5).build();
			TableDescriptor td = TableDescriptorBuilder.newBuilder(htableName).setColumnFamily(cd).build();
			admin.createTable(td, null);
        }

		HBaseSail sail = new HBaseSail(hconn, "timestamptable", false, 0, true, 0, null, null);
		HBaseRepository rep = new HBaseRepository(sail);
        rep.init();
        try(SailRepositoryConnection con = rep.getConnection()) {
            // insert a stmt in the past
            update(con,
					"prefix halyard: <http://merck.github.io/Halyard/ns#>\ninsert {<http://whatever> <http://whatever> <http://whatever>. (<http://whatever> <http://whatever> <http://whatever>) halyard:timestamp ?t} where {bind(\"2002-05-30T09:30:10.2\"^^<http://www.w3.org/2001/XMLSchema#dateTime> as ?t)}");
            assertEquals(toTimestamp("2002-05-30T09:30:10.2"), selectLatest(con));

            // delete a stmt further in the past
            update(con,
					"prefix halyard: <http://merck.github.io/Halyard/ns#>\ndelete {<http://whatever> <http://whatever> <http://whatever>} where {<http://whatever> <http://whatever> <http://whatever>. (<http://whatever> <http://whatever> <http://whatever>) halyard:timestamp \"2002-05-30T09:30:10.1\"^^<http://www.w3.org/2001/XMLSchema#dateTime> }");
            assertEquals(toTimestamp("2002-05-30T09:30:10.2"), selectLatest(con));

            // delete a more recent stmt
            update(con,
					"prefix halyard: <http://merck.github.io/Halyard/ns#>\ndelete {<http://whatever> <http://whatever> <http://whatever>} where {<http://whatever> <http://whatever> <http://whatever>. (<http://whatever> <http://whatever> <http://whatever>) halyard:timestamp \"2002-05-30T09:30:10.4\"^^<http://www.w3.org/2001/XMLSchema#dateTime> }");
            assertEquals(-1L, selectLatest(con));

            // insert an older stmt
            update(con,
					"prefix halyard: <http://merck.github.io/Halyard/ns#>\ninsert {<http://whatever> <http://whatever> <http://whatever>. (<http://whatever> <http://whatever> <http://whatever>) halyard:timestamp ?t} where {bind(\"2002-05-30T09:30:10.3\"^^<http://www.w3.org/2001/XMLSchema#dateTime> as ?t)}");
            assertEquals(-1L, selectLatest(con));

            // insert a recent stmt
            update(con,
					"prefix halyard: <http://merck.github.io/Halyard/ns#>\ninsert {<http://whatever> <http://whatever> <http://whatever>. (<http://whatever> <http://whatever> <http://whatever>) halyard:timestamp ?t} where {bind(\"2002-05-30T09:30:10.4\"^^<http://www.w3.org/2001/XMLSchema#dateTime> as ?t)}");
            assertEquals(toTimestamp("2002-05-30T09:30:10.4"), selectLatest(con));

            // insert a stmt now
            update(con,
            		"insert data {<http://whatever> <http://whatever> <http://whatever>}");
            assertNotEquals(-1L, selectLatest(con));
            assertEquals(2, selectAllVersions(con).size());
        }
        rep.shutDown();
    }

    private static long toTimestamp(String datetime) {
		return SimpleValueFactory.getInstance().createLiteral(datetime, XSD.DATETIME).calendarValue().toGregorianCalendar().getTimeInMillis();
    }

    private void update(SailRepositoryConnection con, String update) {
        con.prepareUpdate(update).execute();
    }

    private long selectLatest(SailRepositoryConnection con) {
    	Set<Long> results = selectTimestamps(con,
    			"prefix halyard: <http://merck.github.io/Halyard/ns#>\nselect ?t where {<http://whatever> ?p ?o. (<http://whatever> ?p ?o) halyard:timestamp ?t}");
    	return results.size() == 1 ? results.iterator().next() : -1L;
    }


    private Set<Long> selectAllVersions(SailRepositoryConnection con) {
    	return selectTimestamps(con,
                "prefix halyard: <http://merck.github.io/Halyard/ns#>\nselect ?t where {service <http://merck.github.io/Halyard/ns#timestamptable?maxVersions=5> {<http://whatever> ?p ?o. (<http://whatever> ?p ?o) halyard:timestamp ?t}}");
    }

    private Set<Long> selectTimestamps(SailRepositoryConnection con, String query) {
        Set<Long> results = new HashSet<>();
        try(TupleQueryResult iter = con.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate()) {
                while(iter.hasNext()) {
                        BindingSet bs = iter.next();
                        Literal t = (Literal) bs.getValue("t");
                        results.add(t.calendarValue().toGregorianCalendar().getTimeInMillis());
                }
        }
        return results;
    }



    @Test
    public void testTimeAwareModify() throws Exception {
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
		HBaseRepository rep = new HBaseRepository(sail);
        rep.init();
        int i=0;
        IRI timestamp = vf.createIRI("http://whatever/timestamp");
        IRI deleteGraph = vf.createIRI("http://whatever/deleteGraph");
        IRI insertGraph = vf.createIRI("http://whatever/insertGraph");
        IRI context = vf.createIRI("http://whatever/context");
		try (RepositoryConnection conn = rep.getConnection()) {
			for (Change c : changes.values()) {
				IRI chSubj = vf.createIRI("http://whatever/change#" + i);
				IRI delGr = vf.createIRI("http://whatever/graph#" + i + "d");
				IRI insGr = vf.createIRI("http://whatever/graph#" + i + "i");
				conn.add(chSubj, timestamp, vf.createLiteral(c.timestamp));
				conn.add(chSubj, context, targetGraph);
				conn.add(chSubj, deleteGraph, delGr);
				conn.add(chSubj, insertGraph, insGr);
				for (Statement s : c.deletes) {
					conn.add(s.getSubject(), s.getPredicate(), s.getObject(), delGr);
				}
				for (Statement s : c.inserts) {
					conn.add(s.getSubject(), s.getPredicate(), s.getObject(), insGr);
				}
				i++;
			}
		}

        //execute the update queries
		try (RepositoryConnection conn = rep.getConnection()) {
			conn.prepareUpdate(
						"PREFIX : <http://whatever/> " +
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
                        "}").execute();

            conn.prepareUpdate(
            			"PREFIX : <http://whatever/> " +
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
                        "  OPTIONAL {" +
                        "    ?change :insertGraph ?insGr ." +
                        "    GRAPH ?insGr {" +
                        "      ?insertSubj ?insertPred ?insertObj ." +
                        "    }" +
                        "  }" +
                        "}").execute();
		}

        //read transformed data into model
        LinkedHashModel resultModel = new LinkedHashModelFactory().createEmptyModel();
		try (RepositoryConnection conn = rep.getConnection()) {
			try (CloseableIteration<? extends Statement, RepositoryException> iter = conn.getStatements(null, null, null, true, targetGraph)) {
				while (iter.hasNext()) {
					Statement stmt = iter.next();
					resultModel.add(stmt);
				}
			}
		}

        rep.shutDown();

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

    private static class Change {
        int timestamp;
        HashSet<Statement> inserts = new HashSet<>();
        HashSet<Statement> deletes = new HashSet<>();
    }
}
