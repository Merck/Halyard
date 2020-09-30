/*
 * Copyright 2018 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
 * Inc., Kenilworth, NJ, USA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.msd.gin.halyard.optimizers;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.algebra.ArbitraryLengthPath;
import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.EmptySet;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.TupleFunctionCall;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.ZeroLengthPath;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ExternalSet;

import com.msd.gin.halyard.algebra.StarJoin;
import com.msd.gin.halyard.vocab.HALYARD;

/**
 * Must be thread-safe.
 * @author Adam Sotona (MSD)
 */
public final class HalyardEvaluationStatistics extends EvaluationStatistics {

    public static interface StatementPatternCardinalityCalculator {

        public Double getCardinality(StatementPattern sp, Collection<String> boundVars);
    }

    public static interface ServiceStatsProvider {

        public HalyardEvaluationStatistics getStatsForService(String service);
    }

    private final StatementPatternCardinalityCalculator spcalc;
    private final ServiceStatsProvider srvProvider;

    public HalyardEvaluationStatistics(StatementPatternCardinalityCalculator spcalc, ServiceStatsProvider srvProvider) {
        this.spcalc = spcalc;
        this.srvProvider = srvProvider;
    }

    public void updateCardinalityMap(TupleExpr expr, Set<String> boundVars, Set<String> priorityVars, Map<TupleExpr, Double> mapToUpdate) {
        expr.visit(new HalyardCardinalityCalculator(spcalc, srvProvider, boundVars, priorityVars, mapToUpdate));
    }

	public double getCardinality(TupleExpr expr, final Set<String> boundVariables, final Set<String> priorityVariables) {
		HalyardCardinalityCalculator cc = new HalyardCardinalityCalculator(spcalc, srvProvider, boundVariables, priorityVariables, null);
		expr.visit(cc);
		return cc.getCardinality();
	}

	@Override
	public double getCardinality(TupleExpr expr) {
		return getCardinality(expr, Collections.emptySet(), Collections.emptySet());
	}

	@Override
	protected CardinalityCalculator createCardinalityCalculator() {
		return new HalyardCardinalityCalculator(spcalc, srvProvider, Collections.emptySet(), Collections.emptySet(), null);
	}

    private static class HalyardCardinalityCalculator extends CardinalityCalculator {

    	private static final double VAR_CARDINALITY = 10.0;
    	private StatementPatternCardinalityCalculator spcalc;
        private final ServiceStatsProvider srvProvider;
        private final Set<String> boundVars;
        private final Set<String> priorityVariables;
        private final Map<TupleExpr, Double> mapToUpdate;

        public HalyardCardinalityCalculator(StatementPatternCardinalityCalculator spcalc, ServiceStatsProvider srvProvider, Set<String> boundVariables, Set<String> priorityVariables, Map<TupleExpr, Double> mapToUpdate) {
        	this.spcalc = spcalc;
        	this.srvProvider = srvProvider;
            this.boundVars = boundVariables;
            this.priorityVariables = priorityVariables;
            this.mapToUpdate = mapToUpdate;
        }

        @Override
        protected double getCardinality(StatementPattern sp) {
            //always prefer HALYARD.SEARCH_TYPE object literals to move such statements higher in the joins tree
            Var objectVar = sp.getObjectVar();
            if (objectVar.hasValue() && (objectVar.getValue() instanceof Literal) && HALYARD.SEARCH_TYPE.equals(((Literal) objectVar.getValue()).getDatatype())) {
                return 0.0001;
            }
            Double card = spcalc == null ? null : spcalc.getCardinality(sp, boundVars);
            if (card == null) { //fallback to default cardinality calculation
                card = super.getCardinality(sp);
            }
            for (Var v : sp.getVarList()) {
                //decrease cardinality for each priority variable present
                if (v != null && priorityVariables.contains(v.getName())) card /= 1000000.0;
            }
            return card;
        }

        @Override
		protected double getCardinality(double varCardinality, Var var) {
			return hasValue(var, boundVars) ? 1.0 : varCardinality;
		}

        @Override
        protected int countConstantVars(Iterable<Var> vars) {
        	int constantVarCount = 0;
        	for(Var var : vars) {
        		if(hasValue(var, boundVars)) {
        			constantVarCount++;
        		}
        	}
        	return constantVarCount;
        }

        private boolean hasValue(Var partitionVar, Collection<String> boundVars) {
            return partitionVar == null || partitionVar.hasValue() || boundVars.contains(partitionVar.getName());
        }

        @Override
        public void meet(Join node) {
            meetJoin(node);
        }

        @Override
        public void meet(LeftJoin node) {
            meetJoin(node);
        }

        private void meetJoin(BinaryTupleOperator node) {
            node.getLeftArg().visit(this);
            updateMap(node.getLeftArg());
            double leftArgCost = this.cardinality;
            Set<String> newBoundVars = new HashSet<>(boundVars);
            newBoundVars.addAll(node.getLeftArg().getBindingNames());
            HalyardCardinalityCalculator newCalc = new HalyardCardinalityCalculator(spcalc, srvProvider, newBoundVars, priorityVariables, mapToUpdate);
            node.getRightArg().visit(newCalc);
            cardinality = newCalc.cardinality;
            updateMap(node.getRightArg());
            cardinality *= leftArgCost * leftArgCost;
            updateMap(node);
        }

        @Override
        protected void meetBinaryTupleOperator(BinaryTupleOperator node) {
            node.getLeftArg().visit(this);
            updateMap(node.getLeftArg());
            double leftArgCost = this.cardinality;
            node.getRightArg().visit(this);
            updateMap(node.getRightArg());
            cardinality += leftArgCost;
            updateMap(node);
        }

        @Override
        public void meet(Filter node) throws RuntimeException {
            super.meetUnaryTupleOperator(node);
            double subCost = cardinality;
            cardinality = 1;
            node.getCondition().visit(this);
            cardinality *= subCost;
            updateMap(node);
        }

        @Override
        protected void meetUnaryTupleOperator(UnaryTupleOperator node) {
            super.meetUnaryTupleOperator(node);
            updateMap(node);
        }

        @Override
        public void meet(StatementPattern sp) {
            super.meet(sp);
            updateMap(sp);
        }

        @Override
        public void meet(ArbitraryLengthPath node) {
            super.meet(node);
            updateMap(node);
        }

        @Override
        public void meet(ZeroLengthPath node) {
            super.meet(node);
            updateMap(node);
        }

        @Override
        public void meet(BindingSetAssignment node) {
            super.meet(node);
            updateMap(node);
        }

        @Override
        public void meet(EmptySet node) {
            super.meet(node);
            updateMap(node);
        }

        @Override
        public void meet(SingletonSet node) {
            super.meet(node);
            updateMap(node);
        }

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
        	updateMap(node);
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
			updateMap(node);
		}

        @Override
        public void meet(Service node) {
            EvaluationStatistics srvStats = srvProvider != null && node.getServiceRef().hasValue() ? srvProvider.getStatsForService(node.getServiceRef().getValue().stringValue()) : null;
            //try to calculate cardinality also for (Halyard-internally) federated service expressions
            if (srvStats != null) {
                Double servCard = null;
                if (mapToUpdate != null) {
                	node.getServiceExpr().visit(this);
                    servCard = mapToUpdate.get(node.getServiceExpr());
                }
                cardinality = servCard != null ? servCard : srvStats.getCardinality(node.getServiceExpr());
            } else {
                super.meet(node);
            }
            updateMap(node);
        }

        private void updateMap(TupleExpr node) {
            if (mapToUpdate != null) {
                mapToUpdate.put(node, cardinality);
            }
        }
    }
}
