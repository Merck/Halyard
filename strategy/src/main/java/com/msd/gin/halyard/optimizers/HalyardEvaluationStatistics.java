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

import com.msd.gin.halyard.algebra.StarJoin;
import com.msd.gin.halyard.vocab.HALYARD;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.ArbitraryLengthPath;
import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.EmptySet;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.TupleFunctionCall;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.ZeroLengthPath;

/**
 * Must be thread-safe.
 * @author Adam Sotona (MSD)
 */
public final class HalyardEvaluationStatistics extends ExtendedEvaluationStatistics {

	public static interface StatementPatternCardinalityCalculator extends Closeable {
	
		public static interface Factory {
			StatementPatternCardinalityCalculator create() throws IOException;
		}

		double getCardinality(StatementPattern sp, Collection<String> boundVars);
	}

	public static interface ServiceStatsProvider {
	
		HalyardEvaluationStatistics getStatsForService(String service);
	}

    private final StatementPatternCardinalityCalculator.Factory spcalcFactory;
    private final ServiceStatsProvider srvProvider;

    public HalyardEvaluationStatistics(@Nonnull StatementPatternCardinalityCalculator.Factory spcalcFactory, ServiceStatsProvider srvProvider) {
        this.spcalcFactory = spcalcFactory;
        this.srvProvider = srvProvider;
    }

	public void updateCardinalityMap(TupleExpr expr, Set<String> boundVars, Set<String> priorityVars, Map<TupleExpr, Double> mapToUpdate) {
		try (StatementPatternCardinalityCalculator spcalc = spcalcFactory.create()) {
			HalyardCardinalityCalculator cc = new HalyardCardinalityCalculator(spcalc, srvProvider, boundVars, priorityVars, mapToUpdate);
			expr.visit(cc);
		} catch(IOException ioe) {
			throw new QueryEvaluationException(ioe);
		}
	}

	public double getCardinality(TupleExpr expr, final Set<String> boundVariables, final Set<String> priorityVariables) {
		try (StatementPatternCardinalityCalculator spcalc = spcalcFactory.create()) {
			HalyardCardinalityCalculator cc = new HalyardCardinalityCalculator(spcalc, srvProvider, boundVariables, priorityVariables, null);
			expr.visit(cc);
			return cc.getCardinality();
		} catch(IOException ioe) {
			throw new QueryEvaluationException(ioe);
		}
	}

	@Override
	public double getCardinality(TupleExpr expr) {
		return getCardinality(expr, Collections.emptySet(), Collections.emptySet());
	}

	@Override
	protected CardinalityCalculator createCardinalityCalculator() {
		// should never be called
		throw new AssertionError();
	}

    private static class HalyardCardinalityCalculator extends ExtendedCardinalityCalculator {

    	private final StatementPatternCardinalityCalculator spcalc;
        private final ServiceStatsProvider srvProvider;
        private final Set<String> boundVars;
        private final Set<String> priorityVariables;
        private final Map<TupleExpr, Double> mapToUpdate;

        public HalyardCardinalityCalculator(@Nonnull StatementPatternCardinalityCalculator spcalc, ServiceStatsProvider srvProvider, Set<String> boundVariables, Set<String> priorityVariables, @Nullable Map<TupleExpr, Double> mapToUpdate) {
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
            if (objectVar.hasValue() && (objectVar.getValue() instanceof Literal) && HALYARD.SEARCH.equals(((Literal) objectVar.getValue()).getDatatype())) {
                return 0.0001;
            }
            double card = spcalc.getCardinality(sp, boundVars);
            for (Var v : sp.getVarList()) {
                //decrease cardinality for each priority variable present
                if (v != null && priorityVariables.contains(v.getName())) {
                	card /= 1000000.0;
                }
            }
            return card;
        }

        @Override
		protected double getCardinality(double varCardinality, Var var) {
			return SimpleStatementPatternCardinalityCalculator.getCardinality(var, boundVars, varCardinality);
		}

        @Override
        protected int countConstantVars(Iterable<Var> vars) {
        	int constantVarCount = 0;
        	for(Var var : vars) {
        		if(SimpleStatementPatternCardinalityCalculator.hasValue(var, boundVars)) {
        			constantVarCount++;
        		}
        	}
        	return constantVarCount;
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

        protected void meetStarJoin(StarJoin node) {
        	super.meetStarJoin(node);
        	updateMap(node);
        }

        protected void meetTupleFunctionCall(TupleFunctionCall node) {
        	super.meetTupleFunctionCall(node);
			updateMap(node);
		}

        @Override
        public void meet(Service node) {
            HalyardEvaluationStatistics srvStats = (srvProvider != null && node.getServiceRef().hasValue()) ? srvProvider.getStatsForService(node.getServiceRef().getValue().stringValue()) : null;
            //try to calculate cardinality also for (Halyard-internally) federated service expressions
            if (srvStats != null) {
                TupleExpr remoteExpr = node.getServiceExpr();
                if (mapToUpdate != null) {
                    srvStats.updateCardinalityMap(remoteExpr, boundVars, priorityVariables, mapToUpdate);
                    cardinality = mapToUpdate.get(remoteExpr);
                } else {
                	cardinality = srvStats.getCardinality(remoteExpr, boundVars, priorityVariables);
                }
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
