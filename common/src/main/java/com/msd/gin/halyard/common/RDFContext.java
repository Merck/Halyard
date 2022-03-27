package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Resource;

public final class RDFContext extends RDFValue<Resource> {
	public static RDFContext create(Resource ctx, IdentifiableValueIO valueIO) {
		if(ctx == null) {
			return null;
		}
		return new RDFContext(ctx, valueIO);
	}

	/**
	 * Key hash size in bytes
	 */
	public static final int KEY_SIZE = 6;
	static final byte[] STOP_KEY = HalyardTableUtils.STOP_KEY_48;

	private RDFContext(Resource val, IdentifiableValueIO valueIO) {
		super(RDFRole.CONTEXT, val, valueIO);
	}
}
