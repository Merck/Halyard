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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryJoinOptimizer;

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
        tupleExpr.visit(new QueryJoinOptimizer.JoinVisitor() {
            @Override
            protected double getTupleExprCardinality(TupleExpr tupleExpr, Map<TupleExpr, Double> cardinalityMap, Map<TupleExpr, List<Var>> varsMap, Map<Var, Integer> varFreqMap, Set<String> boundVars) {
                ((HalyardEvaluationStatistics) statistics).updateCardinalityMap(tupleExpr, boundVars, cardinalityMap);
                return super.getTupleExprCardinality(tupleExpr, cardinalityMap, varsMap, varFreqMap, boundVars); //To change body of generated methods, choose Tools | Templates.
            }
        });
    }
}
