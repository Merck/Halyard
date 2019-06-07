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

import com.msd.gin.halyard.vocab.HALYARD;
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
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.ZeroLengthPath;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ExternalSet;

/**
 *
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

    CardinalityCalculator lastBoundCC = null;
    Set<String> lastBoundVars = null;
    Map<TupleExpr, Double> lastMap = null;

    public synchronized void updateCardinalityMap(TupleExpr expr, Set<String> boundVars, Set<String> priorityVars, Map<TupleExpr, Double> mapToUpdate) {
        if (this.lastBoundVars != boundVars || this.lastMap != mapToUpdate || lastBoundCC == null) {
            lastBoundCC = new HalyardCardinalityCalcualtor(boundVars, priorityVars, mapToUpdate);
            lastBoundVars = boundVars;
            lastMap = mapToUpdate;
        }
        expr.visit(lastBoundCC);
    }

    public synchronized double getCardinality(TupleExpr expr, final Set<String> boundVariables) {
        if (cc == null) {
            cc = new HalyardCardinalityCalcualtor(boundVariables, Collections.emptySet(), null);
        }
        expr.visit(cc);
        return cc.getCardinality();
    }

    @Override
    protected CardinalityCalculator createCardinalityCalculator() {
        return new HalyardCardinalityCalcualtor(Collections.emptySet(), Collections.emptySet(), null);
    }

    private class HalyardCardinalityCalcualtor extends CardinalityCalculator {

        private Set<String> boundVars;
        private final Set<String> priorityVariables;
        private final Map<TupleExpr, Double> mapToUpdate;

        public HalyardCardinalityCalcualtor(Set<String> boundVariables, Set<String> priorityVariables, Map<TupleExpr, Double> mapToUpdate) {
            this.boundVars = boundVariables;
            this.priorityVariables = priorityVariables;
            this.mapToUpdate = mapToUpdate;
        }

        @Override
        protected double getCardinality(StatementPattern sp) {
            //always preffer HALYARD.SEARCH_TYPE object literals to move such statements higher in the joins tree
            Var objectVar = sp.getObjectVar();
            if (objectVar.hasValue() && (objectVar.getValue() instanceof Literal) && HALYARD.SEARCH_TYPE.equals(((Literal) objectVar.getValue()).getDatatype())) {
                return 0.0001;
            }
            Double card = spcalc == null ? null : spcalc.getCardinality(sp, boundVars);
            if (card == null) { //fallback to default cardinality calculation
                card = (hasValue(sp.getSubjectVar(), boundVars) ? 1.0 : 10.0) * (hasValue(sp.getPredicateVar(), boundVars) ? 1.0 : 10.0) * (hasValue(sp.getObjectVar(), boundVars) ? 1.0 : 10.0) * (hasValue(sp.getContextVar(), boundVars) ? 1.0 : 10.0);
            }
            for (Var v : sp.getVarList()) {
                //decrease cardinality for each priority variable present
                if (v != null && priorityVariables.contains(v.getName())) card /= 1000.0;
            }
            return card;
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
            Set<String> origBoundVars = boundVars;
            boundVars = new HashSet<>(boundVars);
            boundVars.addAll(node.getLeftArg().getBindingNames());
            node.getRightArg().visit(this);
            updateMap(node.getRightArg());
            cardinality *= leftArgCost * leftArgCost;
            boundVars = origBoundVars;
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
            if (node instanceof ExternalSet) {
                meetExternalSet((ExternalSet) node);
			} else if (node instanceof TupleFunctionCall) {
				meetTupleFunctionCall((TupleFunctionCall) node);
            } else {
                node.visitChildren(this);
            }
        }

		protected void meetTupleFunctionCall(TupleFunctionCall node) {
			cardinality = Double.MAX_VALUE;
		}

        @Override
        public void meet(Service node) {
            HalyardEvaluationStatistics srvStats = srvProvider != null && node.getServiceRef().hasValue() ? srvProvider.getStatsForService(node.getServiceRef().getValue().stringValue()) : null;
            //try to calculate cardinality also for (Halyard-internally) federated service expressions
            if (srvStats != null) {
                Double servCard = null;
                if (mapToUpdate != null) {
                    srvStats.updateCardinalityMap(node.getServiceExpr(), boundVars, priorityVariables, mapToUpdate);
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
