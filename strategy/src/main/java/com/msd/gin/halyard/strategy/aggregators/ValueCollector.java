package com.msd.gin.halyard.strategy.aggregators;

import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.evaluation.util.ValueComparator;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateCollector;

public final class ValueCollector implements AggregateCollector {
	private final ValueComparator comparator = new ValueComparator();
	private final AtomicReference<Value> vref = new AtomicReference<>();

	public ValueCollector(boolean strict) {
		comparator.setStrict(strict);
	}

	public void min(Value val) {
		vref.accumulateAndGet(val, (current,next) -> {
			if (current == null || comparator.compare(next, current) < 0) {
				return next;
			} else {
				return current;
			}
		});
	}

	public void max(Value val) {
		vref.accumulateAndGet(val, (current,next) -> {
			if (current == null || comparator.compare(next, current) > 0) {
				return next;
			} else {
				return current;
			}
		});
	}

	@Override
	public Value getFinalValue() {
		return vref.get();
	}
}
