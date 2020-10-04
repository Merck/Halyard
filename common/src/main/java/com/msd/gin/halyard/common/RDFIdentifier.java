package com.msd.gin.halyard.common;

public class RDFIdentifier {
	private final RDFRole role;
	private byte[] hash;

	public RDFIdentifier(RDFRole role, byte[] id) {
		this(role);
		this.hash = id;
	}

	protected RDFIdentifier(RDFRole role) {
		this.role = role;
	}

	public final RDFRole getRole() {
		return role;
	}

	protected byte[] calculateHash() {
		throw new UnsupportedOperationException("Hash must be provided");
	}

	private byte[] getUniqueHash() {
		if (hash == null) {
			hash = calculateHash();
		}
		return hash;
	}

	public final byte[] getKeyHash(byte prefix) {
		return role.keyHash(prefix, getUniqueHash());
	}

	final byte[] getEndKeyHash(byte prefix) {
		return role.endKeyHash(prefix, getUniqueHash());
	}

	final byte[] getQualifierHash() {
		return role.qualifierHash(getUniqueHash());
	}

	final byte[] getEndQualifierHash() {
		return role.endQualifierHash(getUniqueHash());
	}

	final int keyHashSize() {
		return role.keyHashSize();
	}

	final int endKeyHashSize() {
		return role.endKeyHashSize();
	}

	final int qualifierHashSize() {
		return role.qualifierHashSize();
	}

	final int endQualifierHashSize() {
		return role.endQualifierHashSize();
	}
}
