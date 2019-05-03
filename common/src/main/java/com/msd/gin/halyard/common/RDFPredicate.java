package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.IRI;

public final class RDFPredicate extends RDFValue<IRI> {
	public static RDFPredicate create(IRI pred) {
		if(pred == null) {
			return null;
		}
		byte[] b = HalyardTableUtils.writeBytes(pred);
		return new RDFPredicate(pred, b);
	}

	/**
	 * Key hash size in bytes
	 */
	public static final byte KEY_SIZE = 4;
	public static final byte[] STOP_KEY = HalyardTableUtils.STOP_KEY_32;
	public static final byte[] END_STOP_KEY = HalyardTableUtils.STOP_KEY_16;

	private RDFPredicate(IRI val, byte[] ser) {
		super(val, ser);
	}

	protected byte[] hash() {
		return HalyardTableUtils.hash32(ser);
	}

	public byte[] getEndHash() {
		return HalyardTableUtils.hash16(ser);
	}

	public static byte[] hash(IRI v) {
		return HalyardTableUtils.hash32(HalyardTableUtils.writeBytes(v));
	}
}
