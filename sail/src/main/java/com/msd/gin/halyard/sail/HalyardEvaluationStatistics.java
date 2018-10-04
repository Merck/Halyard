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
package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.common.HalyardTableUtils;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.sail.SailException;

/**
 *
 * @author Adam Sotona (MSD)
 */
public final class HalyardEvaluationStatistics extends EvaluationStatistics {
    private static final Logger LOG = Logger.getLogger(HalyardEvaluationStatistics.class.getName());

    private final HBaseSail sail;

    public HalyardEvaluationStatistics(HBaseSail sail) {
        this.sail = sail;
    }

    CardinalityCalculator lastBoundCC = null;
    Set<String> lastBoundVars = null;

    public synchronized void updateCardinalityMap(TupleExpr expr, Set<String> boundVars, Map<TupleExpr, Double> mapToUpdate) {
            if (this.lastBoundVars != boundVars || lastBoundCC == null) {
                    lastBoundCC = createCardinalityCalculator(boundVars, mapToUpdate);
                    lastBoundVars = boundVars;
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
            protected double getCardinality(StatementPattern sp) { //get the cardinality of the Statement form VOID statistics
                Var objectVar = sp.getObjectVar();
                //always return cardinality 1.0 for HALYARD.SEARCH_TYPE object literals to move such statements higher in the joins tree
                if (objectVar.hasValue() && (objectVar.getValue() instanceof Literal) && HALYARD.SEARCH_TYPE.equals(((Literal) objectVar.getValue()).getDatatype())) {
                    return 1.0;
                }
                final Var contextVar = sp.getContextVar();
                final IRI graphNode = contextVar == null || !contextVar.hasValue() ? HALYARD.STATS_ROOT_NODE : (IRI) contextVar.getValue();
                final long triples = getTriplesCount(graphNode, -1l);
                if (triples > 0) { //stats are present
                    final double card;
                    boolean sv = hasValue(sp.getSubjectVar());
                    boolean pv = hasValue(sp.getPredicateVar());
                    boolean ov = hasValue(sp.getObjectVar());
                    long defaultCardinality = Math.round(Math.sqrt(triples));
                    if (sv) {
                        if (pv) {
                            if (ov) {
                                card = 1.0;
                            } else {
                                card = (double)subsetTriplesPart(graphNode, VOID_EXT.SUBJECT, sp.getSubjectVar(), defaultCardinality) * subsetTriplesPart(graphNode, VOID.PROPERTY, sp.getPredicateVar(), defaultCardinality) / triples;
                            }
                        } else {
                            if (ov) {
                                card = (double)subsetTriplesPart(graphNode, VOID_EXT.SUBJECT, sp.getSubjectVar(), defaultCardinality) * subsetTriplesPart(graphNode, VOID_EXT.OBJECT, sp.getObjectVar(), defaultCardinality) / triples;
                            } else {
                                card = subsetTriplesPart(graphNode, VOID_EXT.SUBJECT, sp.getSubjectVar(), defaultCardinality);
                            }
                        }
                    } else {
                        if (pv) {
                            if (ov) {
                                card = (double)subsetTriplesPart(graphNode, VOID.PROPERTY, sp.getPredicateVar(), defaultCardinality) * subsetTriplesPart(graphNode, VOID_EXT.OBJECT, sp.getObjectVar(), defaultCardinality) / triples;
                            } else {
                                card = subsetTriplesPart(graphNode, VOID.PROPERTY, sp.getPredicateVar(), defaultCardinality);
                            }
                        } else {
                            if (ov) {
                                card = subsetTriplesPart(graphNode, VOID_EXT.OBJECT, sp.getObjectVar(), defaultCardinality);
                            } else {
                                card = triples;
                            }
                        }
                    }
                    LOG.log(Level.FINE, "cardinality of {0} = {1}", new Object[]{sp.toString(), card});
                    return card;
                } else { // stats are not present
                    double card = (hasValue(sp.getSubjectVar()) ? 1.0 : 10.0) * (hasValue(sp.getPredicateVar()) ? 1.0 : 10.0) * (hasValue(sp.getObjectVar()) ? 1.0 : 10.0) * (hasValue(sp.getContextVar()) ? 1.0 : 10.0);
                    LOG.log(Level.FINE, "default cardinality of {0} = {1}", new Object[]{sp.toString(), card});
                    return card;
                }
            }

            //get the Triples count for a giving subject from VOID statistics or return the default value
            private long getTriplesCount(IRI subjectNode, long defaultValue) {
                try (CloseableIteration<? extends Statement, SailException> ci = sail.getStatements(subjectNode, VOID.TRIPLES, null, true, HALYARD.STATS_GRAPH_CONTEXT)) {
                    if (ci.hasNext()) {
                        Value v = ci.next().getObject();
                        if (v instanceof Literal) {
                            try {
                                long l = ((Literal) v).longValue();
                                LOG.log(Level.FINER, "triple stats for {0} = {1}", new Object[]{subjectNode, l});
                                return l;
                            } catch (NumberFormatException ignore) {
                            }
                        }
                        LOG.log(Level.WARNING, "Invalid statistics for:{0}", subjectNode);
                    }
                }
                LOG.log(Level.FINER, "triple stats for {0} are not available", subjectNode);
                return defaultValue;
            }

            private boolean hasValue(Var partitionVar) {
                return partitionVar == null || partitionVar.hasValue() || boundVars.contains(partitionVar.getName());
            }

            //calculate a multiplier for the triple count for this sub-part of the graph
            private long subsetTriplesPart(IRI graph, IRI partitionType, Var partitionVar, long defaultCardinality) {
                if (partitionVar == null || !partitionVar.hasValue()) return defaultCardinality;
                return getTriplesCount(SimpleValueFactory.getInstance().createIRI(graph.stringValue() + "_" + partitionType.getLocalName() + "_" + HalyardTableUtils.encode(HalyardTableUtils.hashKey(partitionVar.getValue()))), 100l);
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
                cardinality *= leftArgCost;
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
            public void meet(Service node) {
                FederatedService servSail;
                //try to calculate cardinality also for (Halyard-internally) federated service expressions
                if (node.getServiceRef().hasValue() && (servSail = sail.getService(node.getServiceRef().getValue().stringValue())) != null) {
                    cardinality = ((HBaseSail)servSail).statistics.getCardinality(node.getServiceExpr());
                    updateMap(node);
                } else {
                    super.meet(node);
                }
            }

            private void updateMap(TupleExpr node) {
                if (mapToUpdate != null) {
                    mapToUpdate.put(node, cardinality);
                }
            }
        };
    }
}
