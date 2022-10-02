package com.msd.gin.halyard.strategy.aggregators;

import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.MathExpr.MathOp;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.util.MathUtil;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateCollector;

public final class NumberCollector implements AggregateCollector {
	static final Literal ZERO = SimpleValueFactory.getInstance().createLiteral(BigInteger.ZERO);

	private final AtomicReference<Literal> vref = new AtomicReference<>(ZERO);
	private volatile ValueExprEvaluationException typeError;

	public void add(Literal l) {
		vref.accumulateAndGet(l, (total,next) -> MathUtil.compute(total, next, MathOp.PLUS));
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

		return vref.get();
	}
}
