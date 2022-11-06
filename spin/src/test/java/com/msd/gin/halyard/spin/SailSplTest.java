package com.msd.gin.halyard.spin;

import static org.junit.Assert.assertEquals;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SPIN;
import org.eclipse.rdf4j.model.vocabulary.SPL;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.Sail;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Runs the spl test cases.
 */
public class SailSplTest {

	private Repository repo;

	private RepositoryConnection conn;

	@Before
	public final void setup() throws Exception {
		repo = new SailRepository(createSail());
		repo.init();
		conn = repo.getConnection();
	}

	protected Sail createSail() throws Exception {
		return new SpinMemoryStore();
	}

	@After
	public final void tearDown() throws Exception {
		conn.close();
		repo.shutDown();
		postCleanup();
	}

	protected void postCleanup() throws Exception {
	}

	@Test
	public void runTests() throws Exception {
		ValueFactory vf = conn.getValueFactory();
		SpinInferencing.insertSchema(conn);
		conn.add(vf.createStatement(vf.createIRI("test:run"), RDF.TYPE, vf.createIRI(SPL.NAMESPACE, "RunTestCases")));
		// add inferred statements required for SpinParser
		conn.add(vf.createStatement(vf.createIRI("test:run"), RDF.TYPE, SPIN.SELECT_TEMPLATES_CLASS));
		conn.add(vf.createStatement(vf.createIRI("test:run"), RDF.TYPE, SPIN.TEMPLATES_CLASS));
		TupleQuery tq = conn.prepareTupleQuery(QueryLanguage.SPARQL,
				"prefix spin: <http://spinrdf.org/spin#> " + "prefix spl: <http://spinrdf.org/spl#> " + "select ?testCase ?expected ?actual where {(<test:run>) spin:select (?testCase ?expected ?actual)}");
		try ( // returns failed tests
				TupleQueryResult tqr = tq.evaluate()) {
			while (tqr.hasNext()) {
				BindingSet bs = tqr.next();
				Value testCase = bs.getValue("testCase");
				Value expected = bs.getValue("expected");
				Value actual = bs.getValue("actual");
				assertEquals(testCase.stringValue(), expected, actual);
			}
		}
	}
}
