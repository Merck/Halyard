package com.msd.gin.halyard.common;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.Value;

public final class RDFObject extends RDFValue<Value> {
	static RDFObject create(RDFRole<RDFObject> role, @Nullable Value obj, RDFFactory rdfFactory) {
		if(obj == null) {
			return null;
		}
		return new RDFObject(role, obj, rdfFactory);
	}

	/**
	 * Key hash size in bytes
	 */
	public static final int KEY_SIZE = 8;
	static final int END_KEY_SIZE = 2;
	static final byte[] STOP_KEY = HalyardTableUtils.STOP_KEY_64;
	static final byte[] END_STOP_KEY = HalyardTableUtils.STOP_KEY_16;

	private RDFObject(RDFRole<RDFObject> role, Value val, RDFFactory rdfFactory) {
		super(role, val, rdfFactory);
	}
}
