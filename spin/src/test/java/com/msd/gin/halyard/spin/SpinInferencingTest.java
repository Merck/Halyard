package com.msd.gin.halyard.spin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailQueryPreparer;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SpinInferencingTest {
	private SailRepository repo;
	private SailRepositoryConnection repoConn;
	private SailQueryPreparer qp;

	@Before
	public void setup() throws RDFParseException, RepositoryException, IOException {
		Sail sail = new MemoryStore();
		repo = new SailRepository(sail);
		repo.init();
		repoConn = repo.getConnection();
		SpinInferencing.insertSchema(repoConn);
		repoConn.add(getClass().getResource("/test-cases/inferencing-tests.ttl"), RDFFormat.TURTLE);
		qp = new SailQueryPreparer(repoConn, true);
	}

	@After
	public void tearDown() {
		repoConn.close();
		repo.shutDown();
	}

	@Test
	public void testValidAskConstraint() {
		ValueFactory vf = repoConn.getValueFactory();
		IRI constraint = vf.createIRI("http://whatever/AgeConstraint");
		ConstraintViolation cv = SpinInferencing.checkConstraint(vf.createIRI("http://whatever/ValidExample"), constraint, qp, new SpinParser());
		assertNull(cv);
	}

	@Test
	public void testInvalidAskConstraint() {
		ValueFactory vf = repoConn.getValueFactory();
		IRI constraint = vf.createIRI("http://whatever/AgeConstraint");
		ConstraintViolation cv = SpinInferencing.checkConstraint(vf.createIRI("http://whatever/InvalidExample"), constraint, qp, new SpinParser());
		assertNotNull(cv);
	}

	@Test
	public void testValidConstructConstraint() {
		ValueFactory vf = repoConn.getValueFactory();
		IRI constraint = vf.createIRI("http://whatever/NameConstraint");
		ConstraintViolation cv = SpinInferencing.checkConstraint(vf.createIRI("http://whatever/ValidExample"), constraint, qp, new SpinParser());
		assertNull(cv);
	}

	@Test
	public void testInvalidConstructConstraint() {
		ValueFactory vf = repoConn.getValueFactory();
		IRI constraint = vf.createIRI("http://whatever/NameConstraint");
		ConstraintViolation cv = SpinInferencing.checkConstraint(vf.createIRI("http://whatever/InvalidExample"), constraint, qp, new SpinParser());
		assertEquals("Invalid name", cv.getMessage());
		assertEquals(ConstraintViolationLevel.ERROR, cv.getLevel());
		assertEquals(XSD.DECIMAL.stringValue(), cv.getValue());
	}
}
