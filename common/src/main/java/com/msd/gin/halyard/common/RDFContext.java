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
	public static final byte KEY_SIZE = 6;
	public static final byte[] STOP_KEY = HalyardTableUtils.STOP_KEY_48;


	private RDFContext(Resource val, byte[] ser) {
		super(val, ser);
	}

	protected int keyHashSize() {
		return KEY_SIZE;
	}

	protected int endKeyHashSize() {
		throw new AssertionError("Context is never at end");
	}
}
