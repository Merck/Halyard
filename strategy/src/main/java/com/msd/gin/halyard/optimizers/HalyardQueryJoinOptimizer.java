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

import com.msd.gin.halyard.algebra.AbstractExtendedQueryModelVisitor;
import com.msd.gin.halyard.algebra.Algebra;
import com.msd.gin.halyard.algebra.StarJoin;
import com.msd.gin.halyard.vocab.HALYARD;

import java.util.ArrayList;
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
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;

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
        tupleExpr.visit(new AbstractExtendedQueryModelVisitor<IncompatibleOperationException>() {
            @Override
            public void meet(FunctionCall node) throws IncompatibleOperationException {
                if (HALYARD.PARALLEL_SPLIT_FUNCTION.stringValue().equals(node.getURI())) {
                    for (ValueExpr arg : node.getArgs()) {
                        if (arg instanceof Var) parallelSplitBounds.add(((Var)arg).getName());
                    }
                }
                super.meet(node);
            }

    		@Override
    		public void meet(StatementPattern node) {
    			// skip children
    		}
        });
        tupleExpr.visit(new QueryJoinOptimizer.JoinVisitor(statistics) {
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
            public void meet(StarJoin node) {
            	List<? extends TupleExpr> sjArgs = node.getArgs();
            	// Detach the args into a join tree.
            	Join joins = (Join) Algebra.join(sjArgs);
            	TupleExpr root = Algebra.ensureRooted(joins);
            	// optimize the join order
            	meet(joins);
            	// re-attach in new order
            	List<TupleExpr> orderedArgs = new ArrayList<>(sjArgs.size());
        		root.visit(new AbstractExtendedQueryModelVisitor<RuntimeException>() {
        			@Override
        			public void meet(Join join) {
        				TupleExpr left = join.getLeftArg();
        				TupleExpr right = join.getRightArg();
        				// joins should be right-recursive
        				assert !(left instanceof Join);
       					orderedArgs.add(left);
       					if (!(right instanceof Join)) {
       						// leaf join has both left and right
       						orderedArgs.add(right);
       					}
       					right.visit(this);
        			}

        			@Override
        			public void meet(StatementPattern node) {
        				// skip children
        			}
				});
            	node.setArgs(orderedArgs);
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
