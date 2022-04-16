package com.msd.gin.halyard.common;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.Resource;

public final class RDFContext extends RDFValue<Resource> {
	static RDFContext create(RDFRole role, @Nullable Resource ctx, IdentifiableValueIO valueIO) {
		if(ctx == null) {
			return null;
		}
		return new RDFContext(role, ctx, valueIO);
	}

	/**
	 * Key hash size in bytes
	 */
	public static final int KEY_SIZE = 6;
	static final byte[] STOP_KEY = HalyardTableUtils.STOP_KEY_48;

	private RDFContext(RDFRole role, Resource val, IdentifiableValueIO valueIO) {
		super(role, val, valueIO);
	}
}
