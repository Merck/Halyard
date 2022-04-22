package com.msd.gin.halyard.common;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.Value;

public final class RDFObject extends RDFValue<Value,SPOC.O> {
	static RDFObject create(RDFRole<SPOC.O> role, @Nullable Value obj, RDFFactory rdfFactory) {
		if(obj == null) {
			return null;
		}
		return new RDFObject(role, obj, rdfFactory);
	}

	private RDFObject(RDFRole<SPOC.O> role, Value val, RDFFactory rdfFactory) {
		super(role, val, rdfFactory);
	}
}
