package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.IRI;

public final class RDFPredicate extends RDFValue<IRI> {
	public static RDFPredicate create(IRI pred) {
		if(pred == null) {
			return null;
		}
		return new RDFPredicate(pred);
	}

	/**
	 * Key hash size in bytes
	 */
	public static final int KEY_SIZE = 4;
	public static final int END_KEY_SIZE = 2;
	public static final byte[] STOP_KEY = HalyardTableUtils.STOP_KEY_32;
	public static final byte[] END_STOP_KEY = HalyardTableUtils.STOP_KEY_16;

	private RDFPredicate(IRI val) {
		super(RDFRole.PREDICATE, val);
	}
}
