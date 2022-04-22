package com.msd.gin.halyard.common;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.IRI;

public final class RDFPredicate extends RDFValue<IRI> {
	static RDFPredicate create(RDFRole<RDFPredicate> role, @Nullable IRI pred, RDFFactory rdfFactory) {
		if(pred == null) {
			return null;
		}
		return new RDFPredicate(role, pred, rdfFactory);
	}

	private RDFPredicate(RDFRole<RDFPredicate> role, IRI val, RDFFactory rdfFactory) {
		super(role, val, rdfFactory);
	}
}
