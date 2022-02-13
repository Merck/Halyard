package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.BNode;

public final class IdentifiableBNode extends BNodeWrapper implements Identifiable {
	private static final long serialVersionUID = -6212507967580561560L;
	private byte[] id;

	static IdentifiableBNode create(BNode bnode) {
		return new IdentifiableBNode(Hashes.id(bnode), bnode);
	}

	private IdentifiableBNode(byte[] id, BNode bnode) {
		super(bnode);
		this.id = id;
	}

	IdentifiableBNode(BNode bnode) {
		super(bnode);
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
