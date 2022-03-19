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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.IncompatibleOperationException;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryJoinOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;

/**
 *
 * @author Adam Sotona (MSD)
 */
public final class HalyardQueryJoinOptimizer extends QueryJoinOptimizer {

    public HalyardQueryJoinOptimizer(HalyardEvaluationStatistics statistics) {
        super(statistics);
    }

    @Override
    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
        final Set<String> parallelSplitBounds = new HashSet<>();
        tupleExpr.visit(new AbstractQueryModelVisitor<IncompatibleOperationException>() {
            @Override
            public void meet(FunctionCall node) throws IncompatibleOperationException {
                if (HALYARD.PARALLEL_SPLIT_FUNCTION.stringValue().equals(node.getURI())) {
                    for (ValueExpr arg : node.getArgs()) {
                        if (arg instanceof Var) parallelSplitBounds.add(((Var)arg).getName());
                    }
                }
                super.meet(node);
            }
        });
        tupleExpr.visit(new QueryJoinOptimizer.JoinVisitor() {
            private Set<String> priorityBounds = new HashSet<>();

            @Override
            protected double getTupleExprCost(TupleExpr tupleExpr, Map<TupleExpr, Double> cardinalityMap, Map<TupleExpr, List<Var>> varsMap, Map<Var, Integer> varFreqMap, Set<String> boundVars) {
                //treat all bindings mentioned in HALYARD.PARALLEL_SPLIT_FUNCTION as bound to prefer them in optimization
                boundVars.addAll(parallelSplitBounds);
                boundVars.addAll(priorityBounds);
                ((HalyardEvaluationStatistics) statistics).updateCardinalityMap(tupleExpr, boundVars, parallelSplitBounds, cardinalityMap);
                return super.getTupleExprCost(tupleExpr, cardinalityMap, varsMap, varFreqMap, boundVars); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void meet(Join node) {
                Set<String> origPriorityBounds = priorityBounds;
                try {
                    priorityBounds = new HashSet<>(priorityBounds);
                    super.meet(node);
                } finally {
                    priorityBounds = origPriorityBounds;
                }
            }

            @Override
            public void meetNode(QueryModelNode node) {
            	if (node instanceof StarJoin) {
            		meetStarJoin((StarJoin) node);
            	} else {
            		super.meetNode(node);
            	}
            }

            private void meetStarJoin(StarJoin node) {
            	Join joins = node.toJoins();
            	QueryRoot root = new QueryRoot(joins);
            	meet(joins);
        		StatementPatternCollector spc = new StatementPatternCollector();
        		root.visit(spc);
            	node.setArgs(spc.getStatementPatterns());
            }

            @Override
            protected List<Extension> getExtensions(List<TupleExpr> expressions) {
                List<Extension> exts = super.getExtensions(expressions);
                for (Extension ext : exts) {
                    priorityBounds.addAll(ext.getBindingNames());
                }
                return exts;
            }

            @Override
            protected List<TupleExpr> getSubSelects(List<TupleExpr> expressions) {
                List<TupleExpr> subs = super.getSubSelects(expressions);
                for (TupleExpr sub : subs) {
                    priorityBounds.addAll(sub.getBindingNames());
                }
                return subs;
            }

        });
    }
}
