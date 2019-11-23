package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Value;

public abstract class RDFValue<V extends Value> {
	final V val;
	private byte[] ser;
	private byte[] hash;

	public static <V extends Value> boolean matches(V value, RDFValue<V> pattern) {
		return pattern == null || pattern.val.equals(value);
	}


	protected RDFValue(V val) {
		this.val = val;
	}

	public abstract RDFRole getRole();

	public final byte[] getSerializedForm() {
		if (ser == null) {
			ser = HalyardTableUtils.writeBytes(val);
		}
		return ser;
	}

	private final byte[] getUniqueHash() {
		if (hash == null) {
			hash = HalyardTableUtils.id(val);
		}
		return hash;
	}

	public final byte[] getKeyHash(byte prefix) {
		return getRole().keyHash(prefix, getUniqueHash());
	}

	final byte[] getEndKeyHash(byte prefix) {
		return getRole().endKeyHash(prefix, getUniqueHash());
	}

	final byte[] getQualifierHash() {
		return getRole().qualifierHash(getUniqueHash());
	}

	final byte[] getEndQualifierHash() {
		return getRole().endQualifierHash(getUniqueHash());
	}

	final int keyHashSize() {
		return getRole().keyHashSize();
	}

	final int endKeyHashSize() {
		return getRole().endKeyHashSize();
	}

	final int qualifierHashSize() {
		return getRole().qualifierHashSize();
	}

	final int endQualifierHashSize() {
		return getRole().endQualifierHashSize();
	}
}
