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

import com.msd.gin.halyard.vocab.HALYARD;

import java.net.ServerSocket;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class SearchTest extends AbstractSearchTest {

    @Test
    public void statementLiteralSearchTest() throws Exception {
		ValueFactory vf = SimpleValueFactory.getInstance();
		Literal val1 = vf.createLiteral("Whatever Text");
		Literal val2 = vf.createLiteral("Whatever Text", "en");
		Literal val3 = vf.createLiteral("Que sea", "es");
		try (ServerSocket server = startElasticsearch(val1, val2)) {
			IRI whatever = vf.createIRI("http://whatever");
			Repository hbaseRepo = createRepo("testSimpleLiteralSearch", server);
			try (RepositoryConnection conn = hbaseRepo.getConnection()) {
				conn.add(whatever, whatever, val1);
				conn.add(whatever, whatever, val2);
				conn.add(whatever, whatever, val3);
				try (RepositoryResult<Statement> iter = conn.getStatements(null, null, vf.createLiteral("what", HALYARD.SEARCH))) {
					assertTrue(iter.hasNext());
					iter.next();
					assertTrue(iter.hasNext());
					iter.next();
					assertFalse(iter.hasNext());
				}
			}
			hbaseRepo.shutDown();
		}
	}

	@Test
	public void advancedSearchTest() throws Exception {
		ValueFactory vf = SimpleValueFactory.getInstance();
		Literal val1 = vf.createLiteral("Whatever Text");
		Literal val2 = vf.createLiteral("Whatever Text", "en");
		Literal val3 = vf.createLiteral("Que sea", "es");
		try (ServerSocket server = startElasticsearch(val1, val2)) {
			IRI whatever = vf.createIRI("http://whatever");
			Repository hbaseRepo = createRepo("testAdvancedLiteralSearch", server);
			try (RepositoryConnection conn = hbaseRepo.getConnection()) {
				conn.add(whatever, whatever, val1);
				conn.add(whatever, whatever, val2);
				conn.add(whatever, whatever, val3);
				TupleQuery q = conn.prepareTupleQuery(
						"PREFIX halyard: <http://merck.github.io/Halyard/ns#> select * { [] a halyard:Query; halyard:query 'what'; halyard:limit 5; halyard:fuzziness 1; halyard:phraseSlop 0; halyard:matches [rdf:value ?v; halyard:score ?score; halyard:index ?index ] }");
				try (TupleQueryResult iter = q.evaluate()) {
					assertTrue(iter.hasNext());
					BindingSet bs = iter.next();
					assertEquals(2, ((Literal) bs.getValue("score")).intValue());
					assertEquals(INDEX, ((Literal) bs.getValue("index")).stringValue());
					assertTrue(iter.hasNext());
					bs = iter.next();
					assertEquals(1, ((Literal) bs.getValue("score")).intValue());
					assertEquals(INDEX, ((Literal) bs.getValue("index")).stringValue());
					assertFalse(iter.hasNext());
				}
			}
			hbaseRepo.shutDown();
		}
	}
}
