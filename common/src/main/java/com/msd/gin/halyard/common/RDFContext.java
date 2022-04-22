package com.msd.gin.halyard.common;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.Resource;

public final class RDFContext extends RDFValue<Resource> {
	static RDFContext create(RDFRole<RDFContext> role, @Nullable Resource ctx, RDFFactory rdfFactory) {
		if(ctx == null) {
			return null;
		}
		return new RDFContext(role, ctx, rdfFactory);
	}

	private RDFContext(RDFRole<RDFContext> role, Resource val, RDFFactory rdfFactory) {
		super(role, val, rdfFactory);
	}
}
