package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Triple;

public final class IdentifiableTriple extends TripleWrapper implements Identifiable {
	private static final long serialVersionUID = 228285959274911416L;
	private byte[] id;

	public IdentifiableTriple(Triple triple) {
		super(triple);
	}

	public IdentifiableTriple(byte[] id, Triple triple) {
		super(triple);
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
