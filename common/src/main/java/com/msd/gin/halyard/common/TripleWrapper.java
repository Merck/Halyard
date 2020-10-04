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

	public Resource getSubject() {
		return triple.getSubject();
	}

	public IRI getPredicate() {
		return triple.getPredicate();
	}

	public Value getObject() {
		return triple.getObject();
	}

	public String stringValue() {
		return triple.stringValue();
	}

	public final String toString() {
		return triple.toString();
	}

	public final int hashCode() {
		return triple.hashCode();
	}

	public final boolean equals(Object o) {
		return triple.equals(o);
	}
}
