package com.msd.gin.halyard.strategy.aggregators;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateCollector;

public final class CSVCollector implements AggregateCollector {
	private final StringBuilder concatenated = new StringBuilder(128);
	private final String separator;
	private final ValueFactory vf;

	public CSVCollector(String sep, ValueFactory vf) {
		this.separator = sep;
		this.vf = vf;
	}

	public synchronized void append(String s) {
		if (concatenated.length() > 0) {
			concatenated.append(separator);
		}
		concatenated.append(s);
	}

	@Override
	public synchronized Value getFinalValue() {
		if (concatenated.length() == 0) {
			return vf.createLiteral("");
		}

		return vf.createLiteral(concatenated.toString());
	}
}
