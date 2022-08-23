package com.msd.gin.halyard.algebra;

import org.eclipse.rdf4j.common.iteration.Iteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;

public abstract class Algorithm implements Iteration<BindingSet,QueryEvaluationException> {
	@Override
	public boolean hasNext() throws QueryEvaluationException {
		throw new UnsupportedOperationException();
	}

	@Override
	public BindingSet next() throws QueryEvaluationException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void remove() throws QueryEvaluationException {
		throw new UnsupportedOperationException();
	}
}
