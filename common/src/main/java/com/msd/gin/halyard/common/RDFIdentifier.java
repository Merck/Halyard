package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;

public class RDFIdentifier<T extends SPOC<?>> {
	private final RDFRole<? extends SPOC<?>> role;
	private Identifier id;

	RDFIdentifier(RDFRole<? extends SPOC<?>> role, Identifier id) {
		this(role);
		this.id = id;
	}

	protected RDFIdentifier(RDFRole<? extends SPOC<?>> role) {
		this.role = role;
	}

	public final RDFRole<? extends SPOC<?>> getRole() {
		return role;
	}

	protected Identifier calculateId() {
		throw new UnsupportedOperationException("ID must be provided");
	}

	private Identifier getId() {
		if (id == null) {
			id = calculateId();
		}
		return id;
	}

	public final byte[] getKeyHash(StatementIndex<?,?,?,?> index) {
		return role.keyHash(index, getId());
	}

	final byte[] getEndKeyHash(StatementIndex<?,?,?,?> index) {
		return role.endKeyHash(index, getId());
	}

	final ByteBuffer writeQualifierHashTo(ByteBuffer bb) {
		return role.writeQualifierHashTo(getId(), bb);
	}

	final ByteBuffer writeEndQualifierHashTo(ByteBuffer bb) {
		return role.writeEndQualifierHashTo(getId(), bb);
	}

	@Override
	public String toString() {
		return "["+getId()+", "+role+"]";
	}
}
