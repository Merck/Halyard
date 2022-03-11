package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.BNode;

public final class IdentifiableBNode extends BNodeWrapper implements Identifiable {
	private static final long serialVersionUID = -6212507967580561560L;
	private Identifier id;

	static IdentifiableBNode create(BNode bnode) {
		return new IdentifiableBNode(Identifier.id(bnode), bnode);
	}

	private IdentifiableBNode(Identifier id, BNode bnode) {
		super(bnode);
		this.id = id;
	}

	IdentifiableBNode(BNode bnode) {
		super(bnode);
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
