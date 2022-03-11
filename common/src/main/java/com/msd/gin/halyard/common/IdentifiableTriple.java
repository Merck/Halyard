package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Triple;

public final class IdentifiableTriple extends TripleWrapper implements Identifiable {
	private static final long serialVersionUID = 228285959274911416L;
	private Identifier id;

	static IdentifiableTriple create(Triple triple) {
		return new IdentifiableTriple(Identifier.id(triple), triple);
	}

	private IdentifiableTriple(Identifier id, Triple triple) {
		super(triple);
		this.id = id;
	}

	IdentifiableTriple(Triple triple) {
		super(triple);
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
