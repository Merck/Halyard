package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Value;

public abstract class RDFValue<V extends Value> {
	final V val;
	final byte[] ser;
	private byte[] hash;

	public static <V extends Value> boolean matches(V value, RDFValue<V> pattern) {
		return pattern == null || pattern.val.equals(value);
	}

	private static byte[] copy(byte[] src, int offset, int len) {
		byte[] dest = new byte[len];
		System.arraycopy(src, offset, dest, 0, len);
		return dest;
	}

	protected RDFValue(V val, byte[] ser) {
		this.val = val;
		this.ser = ser;
	}

	private final byte[] getUniqueHash() {
		if (hash == null) {
			hash = HalyardTableUtils.hashUnique(ser);
		}
		return hash;
	}

	public byte[] getKeyHash() {
		return copy(getUniqueHash(), 0, keyHashSize());
	}

	byte[] getEndKeyHash() {
		return copy(getUniqueHash(), 0, endKeyHashSize());
	}

	byte[] getQualifierHash() {
		return copy(getUniqueHash(), keyHashSize(), qualifierHashSize());
	}

	byte[] getEndQualifierHash() {
		return copy(getUniqueHash(), endKeyHashSize(), endQualifierHashSize());
	}

	protected abstract int keyHashSize();

	protected abstract int endKeyHashSize();

	final int qualifierHashSize() {
		return getUniqueHash().length - keyHashSize();
	}

	final int endQualifierHashSize() {
		return getUniqueHash().length - endKeyHashSize();
	}
}
