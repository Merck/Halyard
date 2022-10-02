package com.msd.gin.halyard.strategy.aggregators;

import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateCollector;

public final class SampleCollector implements AggregateCollector {
	private final AtomicReference<Value> vref = new AtomicReference<>();

	public boolean hasSample() {
		return vref.get() != null;
	}

	public boolean setInitial(Value v) {
		return vref.compareAndSet(null, v);
	}

	public void setSample(Value v) {
		vref.set(v);
	}

	@Override
	public Value getFinalValue() {
		Value v = vref.get();
		if (v == null) {
			throw new ValueExprEvaluationException("SAMPLE undefined");
		}
		return v;
	}
}
