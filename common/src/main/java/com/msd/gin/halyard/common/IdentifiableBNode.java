package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.BNode;

public class IdentifiableBNode extends BNodeWrapper implements Identifiable {
	private static final long serialVersionUID = -6212507967580561560L;
	private byte[] id;

	public IdentifiableBNode(BNode bnode) {
		super(bnode);
	}

	public IdentifiableBNode(byte[] id, BNode bnode) {
		super(bnode);
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
