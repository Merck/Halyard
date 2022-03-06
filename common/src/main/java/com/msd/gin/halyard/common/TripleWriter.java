package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

public interface TripleWriter {
	byte[] writeTriple(Resource subj, IRI pred, Value obj);
}
