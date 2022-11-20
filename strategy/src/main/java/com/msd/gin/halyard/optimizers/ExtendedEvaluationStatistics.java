package com.msd.gin.halyard.optimizers;

import com.msd.gin.halyard.algebra.StarJoin;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nonnull;

import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.TupleFunctionCall;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;

public class ExtendedEvaluationStatistics extends EvaluationStatistics {

	protected final StatementPatternCardinalityCalculator.Factory spcalcFactory;

	public ExtendedEvaluationStatistics(@Nonnull StatementPatternCardinalityCalculator.Factory spcalcFactory) {
		this.spcalcFactory = spcalcFactory;
	}

	public double getCardinality(TupleExpr expr, final Set<String> boundVariables) {
		try (StatementPatternCardinalityCalculator spcalc = spcalcFactory.create()) {
			ExtendedCardinalityCalculator cc = new ExtendedCardinalityCalculator(spcalc, boundVariables);
			expr.visit(cc);
			return cc.getCardinality();
		} catch(IOException ioe) {
			throw new QueryEvaluationException(ioe);
		}
	}

	@Override
	public final double getCardinality(TupleExpr expr) {
		return getCardinality(expr, Collections.emptySet());
	}

	@Override
	protected final CardinalityCalculator createCardinalityCalculator() {
		// should never be called
		throw new AssertionError();
	}


	protected static class ExtendedCardinalityCalculator extends CardinalityCalculator {

		protected static final double VAR_CARDINALITY = 10.0;
		private static final double TFC_COST_FACTOR = 0.1;

		protected final StatementPatternCardinalityCalculator spcalc;
		protected final Set<String> boundVars;

		public ExtendedCardinalityCalculator(@Nonnull StatementPatternCardinalityCalculator spcalc, Set<String> boundVariables) {
			this.spcalc = spcalc;
			this.boundVars = boundVariables;
		}

		@Override
		protected double getCardinality(StatementPattern sp) {
			return spcalc.getCardinality(sp, boundVars);
		}

		@Override
		protected final double getCardinality(double varCardinality, Var var) {
			return SimpleStatementPatternCardinalityCalculator.getCardinality(var, boundVars, varCardinality);
		}

		@Override
		protected final int countConstantVars(Iterable<Var> vars) {
			int constantVarCount = 0;
			for(Var var : vars) {
				if(SimpleStatementPatternCardinalityCalculator.hasValue(var, boundVars)) {
					constantVarCount++;
				}
			}
			return constantVarCount;
		}

        @Override
    	public void meetOther(QueryModelNode node) {
    		if (node instanceof TupleFunctionCall) {
    			meet((TupleFunctionCall)node);
    		} else if (node instanceof StarJoin) {
    			meet((StarJoin)node);
    		} else {
    			super.meetOther(node);
    		}
    	}

        public void meet(StarJoin node) {
        	double card = Double.POSITIVE_INFINITY;
        	for (TupleExpr sp : node.getArgs()) {
        		sp.visit(this);
        		card = Math.min(card, this.cardinality);
        	}
        	Set<Var> vars = new HashSet<>();
        	node.getVars(vars);
        	vars.remove(node.getCommonVar());
        	int constCount = countConstantVars(vars);
            cardinality = card*Math.pow(VAR_CARDINALITY*VAR_CARDINALITY, (double)(vars.size()-constCount)/vars.size());
        }

        public void meet(TupleFunctionCall node) {
			// must have all arguments bound to be able to evaluate
			double argCard = 1.0;
			for (ValueExpr expr : node.getArgs()) {
				if (expr instanceof Var) {
					argCard *= getCardinality(VAR_CARDINALITY, (Var) expr);
				} else if (expr instanceof ValueConstant) {
					argCard *= 1;
				} else {
					argCard *= VAR_CARDINALITY;
				}
			}
			// output cardinality tends to be independent of number of result vars
			cardinality = TFC_COST_FACTOR * argCard * VAR_CARDINALITY;
		}
    }
}
