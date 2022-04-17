package com.msd.gin.halyard.common;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.Resource;

public final class RDFSubject extends RDFValue<Resource> {
	static RDFSubject create(RDFRole role, @Nullable Resource subj, IdentifiableValueIO valueIO) {
		if(subj == null) {
			return null;
		}
		return new RDFSubject(role, subj, valueIO);
	}

	/**
	 * Key hash size in bytes
	 */
	public static final int KEY_SIZE = 8;
	static final int END_KEY_SIZE = 8;
	static final byte[] STOP_KEY = HalyardTableUtils.STOP_KEY_64;
	static final byte[] END_STOP_KEY = HalyardTableUtils.STOP_KEY_64;

	private RDFSubject(RDFRole role, Resource val, IdentifiableValueIO valueIO) {
		super(role, val, valueIO);
	}
}
