package com.msd.gin.halyard.common;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.IRI;

public final class RDFPredicate extends RDFValue<IRI> {
	static RDFPredicate create(RDFRole role, @Nullable IRI pred, IdentifiableValueIO valueIO) {
		if(pred == null) {
			return null;
		}
		return new RDFPredicate(role, pred, valueIO);
	}

	/**
	 * Key hash size in bytes
	 */
	public static final int KEY_SIZE = 4;
	static final int END_KEY_SIZE = 2;
	static final byte[] STOP_KEY = HalyardTableUtils.STOP_KEY_32;
	static final byte[] END_STOP_KEY = HalyardTableUtils.STOP_KEY_16;

	private RDFPredicate(RDFRole role, IRI val, IdentifiableValueIO valueIO) {
		super(role, val, valueIO);
	}
}
