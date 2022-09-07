package com.msd.gin.halyard.optimizers;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

import org.eclipse.rdf4j.query.algebra.StatementPattern;

public interface StatementPatternCardinalityCalculator extends Closeable {

	public static interface Factory {
		StatementPatternCardinalityCalculator create() throws IOException;
	}

	double getCardinality(StatementPattern sp, Collection<String> boundVars);
}
