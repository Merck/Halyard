package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Resource;

public final class RDFSubject extends RDFValue<Resource> {
	public static RDFSubject create(Resource subj) {
		if(subj == null) {
			return null;
		}
		byte[] b = HalyardTableUtils.writeBytes(subj);
		return new RDFSubject(subj, b);
	}


	/**
	 * Key hash size in bytes
	 */
	public static final byte KEY_SIZE = 8;
	public static final byte END_KEY_SIZE = 8;
	public static final byte[] STOP_KEY = HalyardTableUtils.STOP_KEY_64;
	public static final byte[] END_STOP_KEY = HalyardTableUtils.STOP_KEY_64;


	private RDFSubject(Resource val, byte[] ser) {
		super(val, ser);
	}

	protected int keyHashSize() {
		return KEY_SIZE;
	}

	protected int endKeyHashSize() {
		return END_KEY_SIZE;
	}
}
