package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Resource;

public final class RDFSubject extends RDFValue<Resource> {
	public static RDFSubject create(Resource subj) {
		if(subj == null) {
			return null;
		}
		return new RDFSubject(subj);
	}

	/**
	 * Key hash size in bytes
	 */
	public static final int KEY_SIZE = 8;
	public static final int END_KEY_SIZE = 8;
	public static final byte[] STOP_KEY = HalyardTableUtils.STOP_KEY_64;
	public static final byte[] END_STOP_KEY = HalyardTableUtils.STOP_KEY_64;

	private RDFSubject(Resource val) {
		super(RDFRole.SUBJECT, val);
	}
}
