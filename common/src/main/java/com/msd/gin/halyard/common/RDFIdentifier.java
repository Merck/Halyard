package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;
import java.util.Objects;

public class RDFIdentifier<T extends SPOC<?>> {
	private final RDFRole<? extends SPOC<?>> role;
	private ValueIdentifier id;

	RDFIdentifier(RDFRole<? extends SPOC<?>> role, ValueIdentifier id) {
		this(role);
		this.id = Objects.requireNonNull(id);
	}

	protected RDFIdentifier(RDFRole<? extends SPOC<?>> role) {
		this.role = role;
	}

	public final RDFRole<? extends SPOC<?>> getRole() {
		return role;
	}

	protected ValueIdentifier calculateId() {
		throw new AssertionError("ID must be provided");
	}

	private ValueIdentifier getId() {
		if (id == null) {
			id = calculateId();
		}
		return id;
	}

	public final byte[] getKeyHash(StatementIndex<?,?,?,?> index) {
		return index.keyHash(role, getId());
	}

	final byte[] getEndKeyHash(StatementIndex<?,?,?,?> index) {
		return index.endKeyHash(role, getId());
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
