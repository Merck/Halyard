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
	public static final byte END_KEY_SIZE = 2;
	public static final byte[] STOP_KEY = HalyardTableUtils.STOP_KEY_32;
	public static final byte[] END_STOP_KEY = HalyardTableUtils.STOP_KEY_16;

	private RDFPredicate(IRI val, byte[] ser) {
		super(val, ser);
	}

	protected int keyHashSize() {
		return KEY_SIZE;
	}

	protected int endKeyHashSize() {
		return END_KEY_SIZE;
	}
}
