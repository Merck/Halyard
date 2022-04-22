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

	private RDFObject(RDFRole<RDFObject> role, Value val, RDFFactory rdfFactory) {
		super(role, val, rdfFactory);
	}
}
