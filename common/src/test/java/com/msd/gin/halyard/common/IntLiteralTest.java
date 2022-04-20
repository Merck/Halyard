package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.vocabulary.XSD;

public class IntLiteralTest extends AbstractCustomLiteralTest {
	@Override
	protected Literal createLiteral() {
		return new IntLiteral(56, XSD.INTEGER);
	}

	@Override
	protected Literal createOtherLiteral() {
		return new IntLiteral(56, XSD.INT);
	}
}
