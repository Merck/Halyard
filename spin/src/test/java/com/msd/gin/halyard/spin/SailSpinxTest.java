package com.msd.gin.halyard.spin;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.Sail;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SailSpinxTest {

	private Repository repo;

	private RepositoryConnection conn;

	@Before
	public void setup() throws Exception {
		repo = new SailRepository(createSail());
		repo.init();
		conn = repo.getConnection();
		conn.add(getClass().getResource("/test-cases/spinx-tests.ttl"));
	}

	protected Sail createSail() throws Exception {
		return new SpinMemoryStore();
	}

	@After
	public void tearDown() throws Exception {
		conn.close();
		repo.shutDown();
		postCleanup();
	}

	protected void postCleanup() throws Exception {
	}

	@Test
	public void testScriptCode() throws Exception {
		TupleQuery tq = conn.prepareTupleQuery("prefix distance: <classpath:/test-cases/> select ?z where {bind(distance:squareInline(4.5) as ?z)}");
		try (TupleQueryResult tqr = tq.evaluate()) {
			assertEquals("20.25", tqr.next().getValue("z").stringValue());
		}
	}

	@Test
	public void testScriptFile() throws Exception {
		TupleQuery tq = conn.prepareTupleQuery("prefix distance: <classpath:/test-cases/> select ?z where {bind(distance:square(4.5) as ?z)}");
		try (TupleQueryResult tqr = tq.evaluate()) {
			assertEquals("20.25", tqr.next().getValue("z").stringValue());
		}
	}
}
