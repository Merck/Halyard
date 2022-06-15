package com.msd.gin.halyard.sail.spin;

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.sail.HBaseSail;

import java.util.Arrays;
import java.util.Collection;

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
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;

/**
 * Runs the spl test cases.
 */
@RunWith(Parameterized.class)
public class HBaseSailSplTest {

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[] { true }, new Object[] { false });
	}

	private boolean usePushStrategy;

	private Repository repo;

	private RepositoryConnection conn;

	public HBaseSailSplTest(boolean usePushStrategy) {
		this.usePushStrategy = usePushStrategy;
	}

	@Before
	public void setup() throws Exception {
		HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "spifTestTable", true, 0, usePushStrategy, 0, null, null);
		repo = new SailRepository(sail);
		repo.init();
		conn = repo.getConnection();
	}

	@After
	public void tearDown() throws RepositoryException {
		conn.clear();
		conn.close();
		repo.shutDown();
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
