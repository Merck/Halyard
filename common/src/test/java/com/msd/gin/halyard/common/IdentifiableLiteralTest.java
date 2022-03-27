package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.LiteralTest;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public class IdentifiableLiteralTest extends LiteralTest {
	private static final IdentifiableValueIO valueIO = IdentifiableValueIO.create();

	@Override
	protected IRI datatype(String dt) {
		return new IdentifiableIRI(dt, valueIO);
	}

	@Override
	protected Literal literal(String label) {
		return new IdentifiableLiteral(SimpleValueFactory.getInstance().createLiteral(label), valueIO);
	}

	@Override
	protected Literal literal(String label, String lang) {
		return new IdentifiableLiteral(SimpleValueFactory.getInstance().createLiteral(label, lang), valueIO);
	}

	@Override
	protected Literal literal(String label, IRI dt) {
		return new IdentifiableLiteral(SimpleValueFactory.getInstance().createLiteral(label, dt), valueIO);
	}

}
