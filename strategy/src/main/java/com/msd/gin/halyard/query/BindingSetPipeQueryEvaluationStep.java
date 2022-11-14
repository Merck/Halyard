package com.msd.gin.halyard.query;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;

public interface BindingSetPipeQueryEvaluationStep extends QueryEvaluationStep {
	/**
	 * NB: asynchronous.
	 */
	void evaluate(BindingSetPipe parent, BindingSet bindings);
}
