package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;

public abstract class TripleWrapper implements Triple {
	private static final long serialVersionUID = -7504499621357299507L;
	protected final Triple triple;

	protected TripleWrapper(Triple triple) {
		this.triple = triple;
	}

	@Override
	public final Resource getSubject() {
		return triple.getSubject();
	}

	@Override
	public final IRI getPredicate() {
		return triple.getPredicate();
	}

	@Override
	public final Value getObject() {
		return triple.getObject();
	}

	@Override
	public final String stringValue() {
		return triple.stringValue();
	}

	@Override
	public final String toString() {
		return triple.toString();
	}

	@Override
	public final int hashCode() {
		return triple.hashCode();
	}

	@Override
	public final boolean equals(Object o) {
		return triple.equals(o);
	}
}
