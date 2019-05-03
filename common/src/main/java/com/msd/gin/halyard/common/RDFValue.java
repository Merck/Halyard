package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Value;

public abstract class RDFValue<V extends Value> {
	final V val;
	final byte[] ser;
	private byte[] hash;

	public static <V extends Value> boolean matches(V value, RDFValue<V> pattern) {
		return pattern == null || pattern.val.equals(value);
	}

	protected RDFValue(V val, byte[] ser) {
		this.val = val;
		this.ser = ser;
	}

	public final byte[] getHash() {
		if (hash == null) {
			hash = hash();
		}
		return hash;
	}

	protected abstract byte[] hash();

	abstract byte[] getEndHash();
}
