package com.msd.gin.halyard.common;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.Resource;

public final class RDFSubject extends RDFValue<Resource> {
	static RDFSubject create(RDFRole<RDFSubject> role, @Nullable Resource subj, RDFFactory rdfFactory) {
		if(subj == null) {
			return null;
		}
		return new RDFSubject(role, subj, rdfFactory);
	}

	private RDFSubject(RDFRole<RDFSubject> role, Resource val, RDFFactory rdfFactory) {
		super(role, val, rdfFactory);
	}
}
