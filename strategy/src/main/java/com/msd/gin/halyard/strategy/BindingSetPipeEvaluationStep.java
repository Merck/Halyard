package com.msd.gin.halyard.strategy;

import org.eclipse.rdf4j.query.BindingSet;

@FunctionalInterface
public interface BindingSetPipeEvaluationStep {
	/**
	 * NB: asynchronous.
	 */
	void evaluate(BindingSetPipe parent, BindingSet bindings);
}
