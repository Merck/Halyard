package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;

public class RDFIdentifier {
	private final RDFRole role;
	private Identifier id;

	public static RDFIdentifier create(RDFRole role, Identifier id) {
		return new RDFIdentifier(role, id);
	}

	private RDFIdentifier(RDFRole role, Identifier id) {
		this(role);
		this.id = id;
	}

	protected RDFIdentifier(RDFRole role) {
		this.role = role;
	}

	public final RDFRole getRole() {
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

	public final byte[] getKeyHash(StatementIndex index) {
		return role.keyHash(index, getId());
	}

	final byte[] getEndKeyHash(StatementIndex index) {
		return role.endKeyHash(index, getId());
	}

	final ByteBuffer writeQualifierHashTo(ByteBuffer bb) {
		return role.writeQualifierHashTo(getId(), bb);
	}

	final ByteBuffer writeEndQualifierHashTo(ByteBuffer bb) {
		return role.writeEndQualifierHashTo(getId(), bb);
	}

	final int keyHashSize() {
		return role.keyHashSize();
	}

	final int endKeyHashSize() {
		return role.endKeyHashSize();
	}

	final int qualifierHashSize() {
		return role.qualifierHashSize(getId().size());
	}

	final int endQualifierHashSize() {
		return role.endQualifierHashSize(getId().size());
	}

	@Override
	public String toString() {
		return "["+getId()+", "+role+"]";
	}
}
