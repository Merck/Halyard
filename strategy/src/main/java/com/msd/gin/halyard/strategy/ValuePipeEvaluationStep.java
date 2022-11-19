package com.msd.gin.halyard.strategy;

import com.msd.gin.halyard.query.ValuePipe;

import org.eclipse.rdf4j.query.BindingSet;

@FunctionalInterface
interface ValuePipeEvaluationStep {
	/**
	 * NB: asynchronous.
	 */
	void evaluate(ValuePipe parent, BindingSet bindings);

	/**
	 * A step is constant if it always pushes the same value (or error).
	 */
	default boolean isConstant() {
		return false;
	}
}
