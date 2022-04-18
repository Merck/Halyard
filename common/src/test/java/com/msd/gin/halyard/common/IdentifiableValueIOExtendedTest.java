package com.msd.gin.halyard.common;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.Test;

import static org.junit.Assert.*;

public class IdentifiableValueIOExtendedTest {
    private static final RDFFactory rdfFactory = RDFFactory.create();

	@Test
	public void testUniqueTypeFlags() throws IllegalAccessException {
		Map<Byte,String> flags = new HashMap<>();
		for (Field f : ValueIO.class.getDeclaredFields()) {
			f.setAccessible(true);
			String fName = f.getName();
			if (fName.endsWith("_TYPE")) {
				byte flag = f.getByte(null);
				String oldName = flags.put(flag, fName);
				if (oldName != null) {
					throw new AssertionError(String.format("%s: %c already used by %s", fName, flag, oldName));
				}
			}
		}
	}

	@Test
	public void testWellKnownId() {
		Identifier id = rdfFactory.wellKnownId(RDF.TYPE);
		assertNotNull(id);
		IRI typeIri = rdfFactory.getWellKnownIRI(id);
		assertEquals(RDF.TYPE, typeIri);
	}

	@Test
	public void testWellKnownIRI() {
		assertTrue(rdfFactory.isWellKnownIRI(RDF.TYPE));
	}
}
