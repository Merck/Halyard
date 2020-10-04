package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Value;

public final class RDFObject extends RDFValue<Value> {
	public static RDFObject create(Value obj) {
		if(obj == null) {
			return null;
		}
		return new RDFObject(obj);
	}

	/**
	 * Key hash size in bytes
	 */
	public static final int KEY_SIZE = 8;
	public static final int END_KEY_SIZE = 2;
	public static final byte[] STOP_KEY = HalyardTableUtils.STOP_KEY_64;
	public static final byte[] END_STOP_KEY = HalyardTableUtils.STOP_KEY_16;

	private RDFObject(Value val) {
		super(RDFRole.OBJECT, val);
	}
}
