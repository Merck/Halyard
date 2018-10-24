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
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.ZeroLengthPath;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ExternalSet;

/**
 *
 * @author Adam Sotona (MSD)
 */
public final class HalyardEvaluationStatistics extends EvaluationStatistics {

    public static interface StatementPatternCardinalityCalculator {
        public double getCardinality(StatementPattern sp, Collection<String> boundVars);
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

    public synchronized void updateCardinalityMap(TupleExpr expr, Set<String> boundVars, Map<TupleExpr, Double> mapToUpdate) {
            if (this.lastBoundVars != boundVars || this.lastMap != mapToUpdate || lastBoundCC == null) {
                    lastBoundCC = createCardinalityCalculator(boundVars, mapToUpdate);
                    lastBoundVars = boundVars;
                    lastMap = mapToUpdate;
            }
            expr.visit(lastBoundCC);
    }

    @Override
    protected CardinalityCalculator createCardinalityCalculator() {
        return createCardinalityCalculator(Collections.emptySet(), null);
    }

    private CardinalityCalculator createCardinalityCalculator(final Set<String> boundVariables, final Map<TupleExpr, Double> mapToUpdate) {
        return new CardinalityCalculator() {
            Set<String> boundVars = boundVariables;
            @Override
            protected double getCardinality(StatementPattern sp) {
                return spcalc == null ? super.getCardinality(sp) : spcalc.getCardinality(sp, boundVars);
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
                        meetExternalSet((ExternalSet)node);
                } else {
                    node.visitChildren(this);
                }
            }

            @Override
            public void meet(Service node) {
                HalyardEvaluationStatistics srvStats = srvProvider != null && node.getServiceRef().hasValue() ? srvProvider.getStatsForService(node.getServiceRef().getValue().stringValue()) : null;
                //try to calculate cardinality also for (Halyard-internally) federated service expressions
                if (srvStats != null) {
                    Double servCard = null;
                    if (mapToUpdate != null) {
                        srvStats.updateCardinalityMap(node.getServiceExpr(), boundVars, mapToUpdate);
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
        };
    }
}
