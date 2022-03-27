package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.Test;

import static org.junit.Assert.*;

public class IdentifiableValueIOExtendedTest {
	private static final IdentifiableValueIO valueIO = IdentifiableValueIO.create();

	@Test
	public void testWellKnownId() {
		Identifier id = valueIO.wellKnownId(RDF.TYPE);
		assertNotNull(id);
		IRI typeIri = valueIO.getWellKnownIRI(id);
		assertEquals(RDF.TYPE, typeIri);
	}

	@Test
	public void testWellKnownIRI() {
		assertTrue(valueIO.isWellKnownIRI(RDF.TYPE));
	}
}
