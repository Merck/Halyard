package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.IRI;

public final class IdentifiableIRI extends IRIWrapper implements Identifiable {
	private static final long serialVersionUID = 8055405742401584331L;
	private Identifier id;

	static IdentifiableIRI create(IRI iri) {
		return new IdentifiableIRI(Identifier.id(iri), iri);
	}

	private IdentifiableIRI(Identifier id, IRI iri) {
		super(iri);
		this.id = id;
	}

	IdentifiableIRI(IRI iri) {
		super(iri);
	}

	@Override
	public Identifier getId() {
		return id;
	}

	@Override
	public void setId(Identifier id) {
		this.id = id;
	}
}
