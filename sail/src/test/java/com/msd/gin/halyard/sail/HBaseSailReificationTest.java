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
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.client.Connection;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResult;
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
					"prefix halyard: <http://merck.github.io/Halyard/ns#>\ninsert {?stmt <:from> \"source 1\"} where {(<http://whatever> <http://whatever> <http://whatever>) halyard:statement ?stmt }");

			Set<String> result = select(con,
					"prefix halyard: <http://merck.github.io/Halyard/ns#>\nselect ?s {(<http://whatever> <http://whatever> <http://whatever>) halyard:statement/<:from> ?s}");
			assertEquals(Collections.singleton("source 1"), result);

			result = select(con, "prefix halyard: <http://merck.github.io/Halyard/ns#>\nselect ?s {(<http://other> <http://other> <http://other>) halyard:statement/<:from> ?s}");
			assertEquals(Collections.emptySet(), result);

			// delete the stmt
            update(con,
					"prefix halyard: <http://merck.github.io/Halyard/ns#>\ndelete {?stmt <:from> \"source 1\"} where {(<http://whatever> <http://whatever> <http://whatever>) halyard:statement ?stmt }");

			result = select(con, "prefix halyard: <http://merck.github.io/Halyard/ns#>\nselect ?s {(<http://whatever> <http://whatever> <http://whatever>) halyard:statement/<:from> ?s}");
			assertEquals(Collections.emptySet(), result);
        }
        rep.shutDown();
    }

	@Test
	public void testModifyWithContext() throws Exception {
		HBaseSail sail = new HBaseSail(hconn, "reifctxtable", true, 0, true, 0, null, null);
		HBaseRepository rep = new HBaseRepository(sail);
		rep.init();
		try (SailRepositoryConnection con = rep.getConnection()) {
			// insert a stmt
			update(con, "prefix halyard: <http://merck.github.io/Halyard/ns#>\ninsert {?stmt <:from> \"source 1\"} where {(<http://whatever> <http://whatever> <http://whatever> <:graph>) halyard:statement ?stmt }");

			Set<String> result = select(con, "prefix halyard: <http://merck.github.io/Halyard/ns#>\nselect ?s {(<http://whatever> <http://whatever> <http://whatever> <:graph>) halyard:statement/<:from> ?s}");
			assertEquals(Collections.singleton("source 1"), result);

			result = select(con, "prefix halyard: <http://merck.github.io/Halyard/ns#>\nselect ?s {(<http://other> <http://other> <http://other> <:graph>) halyard:statement/<:from> ?s}");
			assertEquals(Collections.emptySet(), result);

			// delete the stmt
			update(con, "prefix halyard: <http://merck.github.io/Halyard/ns#>\ndelete {?stmt <:from> \"source 1\"} where {(<http://whatever> <http://whatever> <http://whatever> <:graph>) halyard:statement ?stmt }");

			result = select(con, "prefix halyard: <http://merck.github.io/Halyard/ns#>\nselect ?s {(<http://whatever> <http://whatever> <http://whatever> <:graph>) halyard:statement/<:from> ?s}");
			assertEquals(Collections.emptySet(), result);
		}
		rep.shutDown();
	}

    private void update(SailRepositoryConnection con, String update) {
        con.prepareUpdate(update).execute();
    }

	private Set<String> select(SailRepositoryConnection con, String query) {
		Set<String> results = new HashSet<>();
		try (TupleQueryResult iter = con.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate()) {
			while (iter.hasNext()) {
				BindingSet bs = iter.next();
				Literal t = (Literal) bs.getValue("s");
				results.add(t.getLabel());
			}
		}
		return results;
	}
}
