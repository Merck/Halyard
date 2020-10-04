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

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Connection;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.resultio.helpers.QueryResultCollector;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class HBaseSailReificationTest {

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
    public void testModify() throws Exception {
		HBaseSail sail = new HBaseSail(hconn, "reiftable", true, 0, true, 0, null, null);
		HBaseRepository rep = new HBaseRepository(sail);
        rep.init();
        try(SailRepositoryConnection con = rep.getConnection()) {
			// insert a stmt
            update(con,
					"prefix halyard: <http://merck.github.io/Halyard/ns#>\ninsert {<http://s> <http://p> <http://o>. ?t <:from> \"source 1\"} where {(<http://s> <http://p> <http://o>) halyard:identifier ?t. ?t halyard:triple (<http://s> <http://p> <http://o>)}");

			List<BindingSet> result = select(con,
					"prefix halyard: <http://merck.github.io/Halyard/ns#>\nselect ?s {(<http://s> <http://p> <http://o>) halyard:identifier/<:from> ?s}");
			assertEquals("source 1", ((Literal) result.get(0).getValue("s")).getLabel());

			result = select(con, "prefix halyard: <http://merck.github.io/Halyard/ns#>\nselect ?s {[halyard:triple (<http://s> <http://p> <http://o>); <:from> ?s]}");
			assertEquals("source 1", ((Literal) result.get(0).getValue("s")).getLabel());

			result = select(con, "prefix halyard: <http://merck.github.io/Halyard/ns#>\nselect ?s {(<http://other> <http://other> <http://other>) halyard:identifier/<:from> ?s}");
			assertEquals(Collections.emptyList(), result);

			result = select(con, "prefix halyard: <http://merck.github.io/Halyard/ns#>\nselect ?s {[halyard:triple (<http://other> <http://other> <http://other>); <:from> ?s]}");
			assertEquals(Collections.emptyList(), result);

			result = select(con,
					"prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> prefix halyard: <http://merck.github.io/Halyard/ns#>\nselect ?s ?p ?o {(<http://s> <http://p> <http://o>) halyard:identifier [rdf:subject ?s; rdf:predicate ?p; rdf:object ?o]}");
			assertEquals("http://s", result.get(0).getValue("s").stringValue());
			assertEquals("http://p", result.get(0).getValue("p").stringValue());
			assertEquals("http://o", result.get(0).getValue("o").stringValue());

			result = select(con, "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> prefix halyard: <http://merck.github.io/Halyard/ns#>\nselect ?s {<http://s> halyard:identifier/rdf:subject ?s}");
			assertEquals("http://s", result.get(0).getValue("s").stringValue());
			result = select(con, "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> prefix halyard: <http://merck.github.io/Halyard/ns#>\nselect ?p {<http://p> halyard:identifier/rdf:predicate ?p}");
			assertEquals("http://p", result.get(0).getValue("p").stringValue());
			result = select(con, "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> prefix halyard: <http://merck.github.io/Halyard/ns#>\nselect ?o {<http://o> halyard:identifier/rdf:object ?o}");
			assertEquals("http://o", result.get(0).getValue("o").stringValue());

			// delete the stmt
            update(con,
					"prefix halyard: <http://merck.github.io/Halyard/ns#>\ndelete {<http://s> <http://p> <http://o>. ?t <:from> \"source 1\"} where {(<http://s> <http://p> <http://o>) halyard:identifier ?t. ?t halyard:triple (<http://s> <http://p> <http://o>) }");

			result = select(con, "prefix halyard: <http://merck.github.io/Halyard/ns#>\nselect ?s {(<http://s> <http://p> <http://o>) halyard:identifier/<:from> ?s}");
			assertEquals(Collections.emptyList(), result);

			result = select(con, "prefix halyard: <http://merck.github.io/Halyard/ns#>\nselect ?s {[halyard:triple (<http://s> <http://p> <http://o>); <:from> ?s]}");
			assertEquals(Collections.emptyList(), result);
        }
        rep.shutDown();
    }

    private void update(SailRepositoryConnection con, String update) {
        con.prepareUpdate(update).execute();
    }

	private List<BindingSet> select(SailRepositoryConnection con, String query) {
		QueryResultCollector results = new QueryResultCollector();
		con.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate(results);
		return results.getBindingSets();
	}
}
