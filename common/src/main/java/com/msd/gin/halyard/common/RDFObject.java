package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Value;

public final class RDFObject extends RDFValue<Value> {
	public static RDFObject create(Value obj) {
		if(obj == null) {
			return null;
		}
		byte[] b = HalyardTableUtils.writeBytes(obj);
		return new RDFObject(obj, b);
	}

	/**
	 * Key hash size in bytes
	 */
	public static final byte KEY_SIZE = 16;
	public static final byte[] STOP_KEY = HalyardTableUtils.STOP_KEY_128;
	public static final byte[] END_STOP_KEY = HalyardTableUtils.STOP_KEY_16;

	private RDFObject(Value val, byte[] ser) {
		super(val, ser);
	}

	protected byte[] hash() {
		return HalyardTableUtils.hash128(ser);
	}

	public byte[] getEndHash() {
		return HalyardTableUtils.hash16(ser);
	}

	public static byte[] hash(Value v) {
		return HalyardTableUtils.hash128(HalyardTableUtils.writeBytes(v));
	}
}
