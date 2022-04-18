package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.LiteralTest;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public class IdentifiableLiteralTest extends LiteralTest {
    private static final RDFFactory rdfFactory = RDFFactory.create();

	@Override
	protected IRI datatype(String dt) {
		return new IdentifiableIRI(dt, rdfFactory);
	}

	@Override
	protected Literal literal(String label) {
		return new IdentifiableLiteral(SimpleValueFactory.getInstance().createLiteral(label), rdfFactory);
	}

	@Override
	protected Literal literal(String label, String lang) {
		return new IdentifiableLiteral(SimpleValueFactory.getInstance().createLiteral(label, lang), rdfFactory);
	}

	@Override
	protected Literal literal(String label, IRI dt) {
		return new IdentifiableLiteral(SimpleValueFactory.getInstance().createLiteral(label, dt), rdfFactory);
	}

}
