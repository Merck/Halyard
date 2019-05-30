package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Literal;
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
	public static final byte[] LITERAL_STOP_KEY = new byte[] {(byte) 0x80};
	public static final byte[] END_STOP_KEY = HalyardTableUtils.STOP_KEY_16;

	private RDFObject(Value val, byte[] ser) {
		super(val, ser);
	}

	protected byte[] hash() {
		return literalPrefix(HalyardTableUtils.hash128(ser));
	}

	byte[] getEndHash() {
		return literalPrefix(HalyardTableUtils.hash16(ser));
	}

	byte[] literalPrefix(byte[] hash) {
		if (val instanceof Literal) {
			hash[0] &= 0x7F; // 0 msb
		} else {
			hash[0] |= 0x80; // 1 msb
		}
		return hash;
	}

	static boolean isLiteral(byte[] hash) {
		return (hash[0] & 0x80) == 0;
	}
}
