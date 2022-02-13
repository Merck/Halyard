package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Literal;

public final class IdentifiableLiteral extends LiteralWrapper implements Identifiable {
	private static final long serialVersionUID = 4299930477670062440L;
	private byte[] id;

	static IdentifiableLiteral create(Literal literal) {
		return new IdentifiableLiteral(Hashes.id(literal), literal);
	}

	private IdentifiableLiteral(byte[] id, Literal literal) {
		super(literal);
		this.id = id;
	}

	IdentifiableLiteral(Literal literal) {
		super(literal);
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
