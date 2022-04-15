package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.junit.Test;

import static org.junit.Assert.*;

public class WKTLiteralTest {
	@Test
	public void testEqualsHashcode() throws Exception {
		String wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))";
		Literal expected = SimpleValueFactory.getInstance().createLiteral(wkt, GEO.WKT_LITERAL);
		Literal actual = new WKTLiteral(wkt);
		assertEquals(expected.hashCode(), actual.hashCode());
		assertTrue(expected.equals(actual));
	}

	@Test
	public void testGeometry() throws Exception {
		String wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))";
		WKTLiteral l = new WKTLiteral(wkt);
		l.geometryValue();
	}
}
