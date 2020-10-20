package com.msd.gin.halyard.optimizers;

import com.msd.gin.halyard.algebra.StarJoin;

import java.util.HashSet;
import java.util.Set;

import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleFunctionCall;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ExternalSet;

public class ExtendedEvaluationStatistics extends EvaluationStatistics {

	@Override
	protected CardinalityCalculator createCardinalityCalculator() {
		return new ExtendedCardinalityCalculator();
	}

    protected static class ExtendedCardinalityCalculator extends CardinalityCalculator {

    	protected static final double VAR_CARDINALITY = 10.0;

        @Override
        protected void meetNode(QueryModelNode node) {
        	if (node instanceof StarJoin) {
        		meetStarJoin((StarJoin) node);
        	} else if (node instanceof TupleFunctionCall) {
        		meetTupleFunctionCall((TupleFunctionCall) node);
			} else if (node instanceof ExternalSet) {
                meetExternalSet((ExternalSet) node);
            } else {
                node.visitChildren(this);
            }
        }

        protected void meetStarJoin(StarJoin node) {
        	double card = Double.POSITIVE_INFINITY;
        	for (StatementPattern sp : node.getArgs()) {
        		card = Math.min(card, getCardinality(sp));
        	}
        	Set<Var> vars = new HashSet<>();
        	node.getVars(vars);
        	vars.remove(node.getCommonVar());
        	int constCount = countConstantVars(vars);
            cardinality = card*Math.pow(VAR_CARDINALITY*VAR_CARDINALITY, (double)(vars.size()-constCount)/vars.size());
        }

        protected void meetTupleFunctionCall(TupleFunctionCall node) {
			// must have all arguments bound to be able to evaluate
			double argCard = 1.0;
			for (ValueExpr expr : node.getArgs()) {
				if (expr instanceof Var) {
					argCard *= getCardinality(1000.0, (Var) expr);
				} else {
					argCard *= 1000.0;
				}
			}
			cardinality = argCard * getCardinality(VAR_CARDINALITY, ((TupleFunctionCall) node).getResultVars());
		}
    }
}
