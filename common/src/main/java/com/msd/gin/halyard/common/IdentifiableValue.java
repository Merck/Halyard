package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Value;

public interface IdentifiableValue extends Value {
	ValueIdentifier getId(RDFFactory rdfFactory);
	void setId(RDFFactory rdfFactory, ValueIdentifier id);
}
