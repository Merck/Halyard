package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Literal;

public class IdentifiableLiteral extends LiteralWrapper implements Identifiable {
	private static final long serialVersionUID = 4299930477670062440L;
	private byte[] id;

	public IdentifiableLiteral(Literal literal) {
		super(literal);
	}

	public IdentifiableLiteral(byte[] id, Literal literal) {
		super(literal);
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
