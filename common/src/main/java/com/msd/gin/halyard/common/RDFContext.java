package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Resource;

public final class RDFContext extends RDFValue<Resource> {
	public static RDFContext create(Resource ctx) {
		if(ctx == null) {
			return null;
		}
		return new RDFContext(ctx);
	}

	/**
	 * Key hash size in bytes
	 */
	public static final int KEY_SIZE = 6;
	public static final byte[] STOP_KEY = HalyardTableUtils.STOP_KEY_48;

	private RDFContext(Resource val) {
		super(RDFRole.CONTEXT, val);
	}
}
