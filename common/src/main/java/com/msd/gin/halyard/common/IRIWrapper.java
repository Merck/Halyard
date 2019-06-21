package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.IRI;

public abstract class IRIWrapper implements IRI {
	private static final long serialVersionUID = 4897685753727929653L;
	private final IRI iri;

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

	public final String toString() {
		return iri.toString();
	}

	public final int hashCode() {
		return iri.hashCode();
	}

	public final boolean equals(Object o) {
		return iri.equals(o);
	}
}
