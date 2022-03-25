package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.IRI;

public abstract class IRIWrapper implements IRI {
	private static final long serialVersionUID = 4897685753727929653L;
	protected final IRI iri;

	protected IRIWrapper(IRI iri) {
		this.iri = iri;
	}

	@Override
	public final String getLocalName() {
		return iri.getLocalName();
	}

	@Override
	public final String getNamespace() {
		return iri.getNamespace();
	}

	@Override
	public final String stringValue() {
		return iri.stringValue();
	}

	@Override
	public final String toString() {
		return iri.toString();
	}

	@Override
	public final int hashCode() {
		return iri.hashCode();
	}

	@Override
	public final boolean equals(Object o) {
		return iri.equals(o);
	}
}
