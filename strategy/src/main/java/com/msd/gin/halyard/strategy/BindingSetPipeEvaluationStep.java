package com.msd.gin.halyard.strategy;

import com.msd.gin.halyard.query.BindingSetPipe;

import org.eclipse.rdf4j.query.BindingSet;

@FunctionalInterface
interface BindingSetPipeEvaluationStep {
	/**
	 * NB: asynchronous.
	 */
	void evaluate(BindingSetPipe parent, BindingSet bindings);
}
