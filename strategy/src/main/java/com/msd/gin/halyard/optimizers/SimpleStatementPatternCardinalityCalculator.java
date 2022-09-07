package com.msd.gin.halyard.optimizers;

import java.io.IOException;
import java.util.Collection;

import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;

public class SimpleStatementPatternCardinalityCalculator implements StatementPatternCardinalityCalculator {
	public static final StatementPatternCardinalityCalculator.Factory FACTORY = () -> new SimpleStatementPatternCardinalityCalculator();

	private static double VAR_CARDINALITY = 10.0;

	@Override
	public double getCardinality(StatementPattern sp, Collection<String> boundVars) {
		return getCardinality(sp.getSubjectVar(), boundVars) * getCardinality(sp.getPredicateVar(), boundVars) * getCardinality(sp.getObjectVar(), boundVars) * getCardinality(sp.getContextVar(), boundVars);
	}

	private double getCardinality(Var var, Collection<String> boundVars) {
		return getCardinality(var, boundVars, VAR_CARDINALITY);
	}

	public static double getCardinality(Var var, Collection<String> boundVars, double varCardinality) {
		return hasValue(var, boundVars) ? 1.0 : varCardinality;
	}

	public static boolean hasValue(Var var, Collection<String> boundVars) {
		return var == null || var.hasValue() || boundVars.contains(var.getName());
	}

	@Override
	public void close() throws IOException {
	}
}
