package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.IRI;

public class IdentifiableIRI extends IRIWrapper implements Identifiable {
	private static final long serialVersionUID = 8055405742401584331L;
	private byte[] id;

	public IdentifiableIRI(IRI iri) {
		super(iri);
	}

	public IdentifiableIRI(byte[] id, IRI iri) {
		super(iri);
		this.id = id;
	}

	@Override
	public byte[] getId() {
		return id;
	}

	@Override
	public void setId(byte[] id) {
		this.id = id;
	}
}
