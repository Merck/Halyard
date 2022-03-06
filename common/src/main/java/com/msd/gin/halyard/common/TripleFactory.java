package com.msd.gin.halyard.common;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.ValueFactory;

public interface TripleFactory {
	Triple readTriple(ByteBuffer b, ValueFactory vf) throws IOException;
}
