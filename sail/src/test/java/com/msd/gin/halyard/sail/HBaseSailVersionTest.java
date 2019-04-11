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
import java.util.Set;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.junit.After;
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
        rep.initialize();
		try (RepositoryConnection conn = rep.getConnection()) {
			conn.add(subj, pred, obj);
			conn.commit();
		}
		try (RepositoryConnection conn = rep.getConnection()) {
			TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL,
					"prefix halyard: <http://merck.github.io/Halyard/ns#>\nselect ?s ?p ?t where {?s ?p \"whatever\". (?s ?p \"whatever\") halyard:timestamp ?t}");
			try (TupleQueryResult res = q.evaluate()) {
				assertTrue(res.hasNext());
				BindingSet bs = res.next();
				assertTrue(((Literal) bs.getValue("t")).calendarValue().compare(startDate.calendarValue()) > 0);
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
        rep.initialize();
		try (RepositoryConnection conn = rep.getConnection()) {
			conn.add(subj, pred, obj);
			conn.commit();
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
			HTableDescriptor td = new HTableDescriptor(htableName);
			td.addFamily(new HColumnDescriptor("e".getBytes()).setMaxVersions(5));
			admin.createTable(td, null);
        }

		HBaseSail sail = new HBaseSail(hconn, "timestamptable", true, 0, true, 0, null, null);
		HBaseRepository rep = new HBaseRepository(sail);
        rep.initialize();
        try(SailRepositoryConnection con = rep.getConnection()) {
            // insert a stmt in the past
            update(con,
            		"prefix halyard: <http://merck.github.io/Halyard/ns#>\ninsert {<http://whatever> <http://whatever> <http://whatever>. (<http://whatever> <http://whatever> <http://whatever>) halyard:timestamp ?t} where {bind(\"2002-05-30T09:30:10.2\"^^<http://www.w3.org/2001/XMLSchema#dateTime> as ?t)}");
            assertEquals(toTimestamp("2002-05-30T09:30:10.2"), selectLatest(con));

            // delete a stmt further in the past
            update(con,
            		"prefix halyard: <http://merck.github.io/Halyard/ns#>\ndelete {<http://whatever> <http://whatever> <http://whatever>} where { (<http://whatever> <http://whatever> <http://whatever>) halyard:timestamp \"2002-05-30T09:30:10.1\"^^<http://www.w3.org/2001/XMLSchema#dateTime> }");
            assertEquals(toTimestamp("2002-05-30T09:30:10.2"), selectLatest(con));

            // delete a more recent stmt
            update(con,
            		"prefix halyard: <http://merck.github.io/Halyard/ns#>\ndelete {<http://whatever> <http://whatever> <http://whatever>} where { (<http://whatever> <http://whatever> <http://whatever>) halyard:timestamp \"2002-05-30T09:30:10.4\"^^<http://www.w3.org/2001/XMLSchema#dateTime> }");
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
    	return SimpleValueFactory.getInstance().createLiteral(datetime, XMLSchema.DATETIME).calendarValue().toGregorianCalendar().getTimeInMillis();
    }

    private void update(SailRepositoryConnection con, String update) {
        con.prepareUpdate(update).execute();
        con.commit();
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
}
