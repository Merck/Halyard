package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.Test;

import static org.junit.Assert.*;

public class XMLLiteralTest {
	@Test
	public void testEqualsHashcode() throws Exception {
		String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><test attr=\"foo\">bar</test>";
		Literal expected = SimpleValueFactory.getInstance().createLiteral(xml, RDF.XMLLITERAL);
		Literal actual = new XMLLiteral(xml);
		assertEquals(expected.hashCode(), actual.hashCode());
		assertTrue(expected.equals(actual));
	}

	@Test
	public void testDocument() throws Exception {
		String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><test attr=\"foo\">bar</test>";
		XMLLiteral l = new XMLLiteral(xml);
		l.documentValue();
	}
}
