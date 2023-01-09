package com.msd.gin.halyard.algebra.evaluation;

import com.msd.gin.halyard.common.ValueConstraint;

import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;

public interface ConstrainedTripleSourceFactory {
	TripleSource getTripleSource(ValueConstraint subjConstraint, ValueConstraint objConstraints);
}
