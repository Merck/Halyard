package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Resource;

public final class RDFContext extends RDFValue<Resource> {
	public static RDFContext create(Resource ctx) {
		if(ctx == null) {
			return null;
		}
		byte[] b = HalyardTableUtils.writeBytes(ctx);
		return new RDFContext(ctx, b);
	}


	/**
	 * Key hash size in bytes
	 */
	public static final byte KEY_SIZE = 8;
	public static final byte[] STOP_KEY = HalyardTableUtils.STOP_KEY_64;


	private RDFContext(Resource val, byte[] ser) {
		super(val, ser);
	}

	protected byte[] hash() {
		return HalyardTableUtils.hash64(ser);
	}

	public byte[] getEndHash() {
		throw new AssertionError("Context is never at end");
	}


	public static byte[] hash(Resource v) {
		return HalyardTableUtils.hash64(HalyardTableUtils.writeBytes(v));
	}
}
