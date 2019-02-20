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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Date;

import org.apache.hadoop.hbase.client.Connection;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
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

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.repository.HBaseRepository;

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
		HBaseSail sail = new HBaseSail(hconn, "timestamptable", true, 0, true, 0, null, null);
		HBaseRepository rep = new HBaseRepository(sail);
        rep.initialize();
        try(SailRepositoryConnection con = rep.getConnection()) {
			assertTrue(testUpdate(con,
					"prefix halyard: <http://merck.github.io/Halyard/ns#>\ninsert {<http://whatever> <http://whatever> <http://whatever>. (<http://whatever> <http://whatever> <http://whatever>) halyard:timestamp ?t} where {bind(\"2002-05-30T09:30:10.2\"^^<http://www.w3.org/2001/XMLSchema#dateTime> as ?t)}"));
			assertTrue(testUpdate(con,
					"prefix halyard: <http://merck.github.io/Halyard/ns#>\ndelete {<http://whatever> <http://whatever> <http://whatever>. ?ls rdf:first <http://whatever>. ?ls rdf:rest ?lp. ?lp rdf:first <http://whatever>. ?lp rdf:rest ?lo. ?lo rdf:first <http://whatever>. ?lo rdf:rest rdf:nil. ?ls halyard:timestamp ?t } where { ?ls rdf:first <http://whatever>. ?ls rdf:rest ?lp. ?lp rdf:first <http://whatever>. ?lp rdf:rest ?lo. ?lo rdf:first <http://whatever>. ?lo rdf:rest rdf:nil. bind(\"2002-05-30T09:30:10.1\"^^<http://www.w3.org/2001/XMLSchema#dateTime> as ?t)}"));
			assertFalse(testUpdate(con,
					"prefix halyard: <http://merck.github.io/Halyard/ns#>\ndelete {<http://whatever> <http://whatever> <http://whatever>. ?ls rdf:first <http://whatever>. ?ls rdf:rest ?lp. ?lp rdf:first <http://whatever>. ?lp rdf:rest ?lo. ?lo rdf:first <http://whatever>. ?lo rdf:rest rdf:nil. ?ls halyard:timestamp ?t } where { ?ls rdf:first <http://whatever>. ?ls rdf:rest ?lp. ?lp rdf:first <http://whatever>. ?lp rdf:rest ?lo. ?lo rdf:first <http://whatever>. ?lo rdf:rest rdf:nil. bind(\"2002-05-30T09:30:10.4\"^^<http://www.w3.org/2001/XMLSchema#dateTime> as ?t)}"));
			assertFalse(testUpdate(con,
					"prefix halyard: <http://merck.github.io/Halyard/ns#>\ninsert {<http://whatever> <http://whatever> <http://whatever>. (<http://whatever> <http://whatever> <http://whatever>) halyard:timestamp ?t} where {bind(\"2002-05-30T09:30:10.3\"^^<http://www.w3.org/2001/XMLSchema#dateTime> as ?t)}"));
			assertTrue(testUpdate(con,
					"prefix halyard: <http://merck.github.io/Halyard/ns#>\ninsert {<http://whatever> <http://whatever> <http://whatever>. (<http://whatever> <http://whatever> <http://whatever>) halyard:timestamp ?t} where {bind(\"2002-05-30T09:30:10.4\"^^<http://www.w3.org/2001/XMLSchema#dateTime> as ?t)}"));
        }
        rep.shutDown();
    }

    private boolean testUpdate(SailRepositoryConnection con, String update) {
        con.prepareUpdate(update).execute();
        con.commit();
        try(TupleQueryResult iter = con.prepareTupleQuery(QueryLanguage.SPARQL, "select * where {<http://whatever> ?p ?o}").evaluate()) {
        	return iter.hasNext();
        }
    }
}
