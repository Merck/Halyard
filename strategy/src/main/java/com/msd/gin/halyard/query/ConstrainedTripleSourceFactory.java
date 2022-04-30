package com.msd.gin.halyard.query;

import com.msd.gin.halyard.common.LiteralConstraints;

import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;

public interface ConstrainedTripleSourceFactory {
	TripleSource getTripleSource(LiteralConstraints constraints);
}
