package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Value;

public abstract class RDFValue<V extends Value> extends RDFIdentifier {
	final V val;
	private byte[] ser;

	public static <V extends Value> boolean matches(V value, RDFValue<V> pattern) {
		return pattern == null || pattern.val.equals(value);
	}


	protected RDFValue(RDFRole role, V val) {
		super(role);
		this.val = val;
	}

	public final byte[] getSerializedForm() {
		if (ser == null) {
			ser = ValueIO.writeBytes(val);
		}
		return ser;
	}

	protected final byte[] calculateHash() {
		return Hashes.id(val);
	}

	@Override
	public String toString() {
		return val+" "+super.toString();
	}
}
