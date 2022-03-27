package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.TripleTest;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public class IdentifiableTripleTest extends TripleTest {
	private static final IdentifiableValueIO valueIO = IdentifiableValueIO.create();

	@Override
	protected Triple triple(Resource s, IRI p, Value o) {
		return new IdentifiableTriple(SimpleValueFactory.getInstance().createTriple(s, p, o), valueIO);
	}

	@Override
	protected IRI iri(String iri) {
		return new IdentifiableIRI(iri, valueIO);
	}

}
