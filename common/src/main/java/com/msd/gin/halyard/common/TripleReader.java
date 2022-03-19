package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.Triple;

public interface TripleReader {
	Triple readTriple(ByteBuffer b, ValueIO.Reader valueReader);
}
