package com.msd.gin.halyard.strategy.aggregators;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.MathExpr.MathOp;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.util.MathUtil;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateCollector;

public final class AvgCollector implements AggregateCollector {
	private final ValueFactory vf;
	private final AtomicLong count = new AtomicLong();
	private final AtomicReference<Literal> sumRef = new AtomicReference<>(NumberCollector.ZERO);
	private volatile ValueExprEvaluationException typeError;

	public AvgCollector(ValueFactory vf) {
		this.vf = vf;
	}

	public void addValue(Literal l) {
		sumRef.accumulateAndGet(l, (total,next) -> MathUtil.compute(total, next, MathOp.PLUS));
	}

	public void incrementCount() {
		count.incrementAndGet();
	}

	public boolean hasError() {
		return typeError != null;
	}

	public void setError(ValueExprEvaluationException err) {
		typeError = err;
	}

	@Override
	public Value getFinalValue() {
		if (typeError != null) {
			// a type error occurred while processing the aggregate, throw it
			// now.
			throw typeError;
		}

		if (count.get() == 0) {
			return NumberCollector.ZERO;
		} else {
			Literal sizeLit = vf.createLiteral(count.get());
			return MathUtil.compute(sumRef.get(), sizeLit, MathOp.DIVIDE);
		}
	}
}
