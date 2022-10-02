package com.msd.gin.halyard.strategy;

import java.util.function.Function;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryValueEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;

public final class QueryValueStepEvaluator implements Function<BindingSet, Value> {
	private final QueryValueEvaluationStep step;

	QueryValueStepEvaluator(QueryValueEvaluationStep step) {
		this.step = step;
	}

	public Value apply(BindingSet bs) {
		try {
			return step.evaluate(bs);
		} catch (ValueExprEvaluationException e) {
			return null; // treat missing or invalid expressions as null
		}
	}
}
