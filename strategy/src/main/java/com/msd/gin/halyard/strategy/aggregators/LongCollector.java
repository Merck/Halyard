package com.msd.gin.halyard.strategy.aggregators;

import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateCollector;

public final class LongCollector implements AggregateCollector {
	private final ValueFactory vf;
	private final AtomicLong v = new AtomicLong();

	public LongCollector(ValueFactory vf) {
		this.vf = vf;
	}

	public void increment() {
		v.incrementAndGet();
	}

	@Override
	public Value getFinalValue() {
		return vf.createLiteral(Long.toString(v.get()), CoreDatatype.XSD.INTEGER);
	}
}
