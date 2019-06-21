package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.BNode;

public abstract class BNodeWrapper implements BNode {
	private static final long serialVersionUID = -3342094906407476268L;
	private final BNode bnode;

	protected BNodeWrapper(BNode iri) {
		this.bnode = iri;
	}

	@Override
	public final String getID() {
		return bnode.getID();
	}

	@Override
	public final String stringValue() {
		return bnode.stringValue();
	}

	public final String toString() {
		return bnode.toString();
	}

	public final int hashCode() {
		return bnode.hashCode();
	}

	public final boolean equals(Object o) {
		return bnode.equals(o);
	}
}
