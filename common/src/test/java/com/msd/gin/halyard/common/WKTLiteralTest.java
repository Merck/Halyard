package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Literal;
import org.junit.jupiter.api.Test;

public class WKTLiteralTest extends AbstractCustomLiteralTest {
	@Override
	protected Literal createLiteral() throws Exception {
		String wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))";
		return new WKTLiteral(wkt);
	}

	@Override
	protected Literal createOtherLiteral() throws Exception {
		String wkt = "POINT (30 10)";
		return new WKTLiteral(wkt);
	}

	@Test
	public void testGeometry() throws Exception {
		String wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))";
		WKTLiteral l = new WKTLiteral(wkt);
		l.objectValue();
	}
}
