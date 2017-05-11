/*
 * Copyright 2016 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
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
package com.msd.gin.halyard.strategy;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.util.EvaluationStrategies;

/**
 * Asynchronous parallel push EvaluationStrategy implementation suitable for parallel remote RDF storage systems.
 * @author Adam Sotona (MSD)
 */
public final class HalyardEvaluationStrategy implements EvaluationStrategy {

    private final FederatedServiceResolver serviceResolver;
    private final HalyardTupleExprEvaluation tupleEval;
    private final HalyardValueExprEvaluation valueEval;

    Value sharedValueOfNow;

    /**
     * Default constructor of HalyardEvaluationStrategy
     * @param tripleSource TripleSource
     * @param dataset Dataset
     * @param timeout long query evaluation timeout in seconds, negative values mean no timeout
     */
    public HalyardEvaluationStrategy(TripleSource tripleSource, Dataset dataset, FederatedServiceResolver serviceResolver, long timeout) {
        this.serviceResolver = serviceResolver;
        this.tupleEval = new HalyardTupleExprEvaluation(this, tripleSource, dataset, timeout);
        this.valueEval = new HalyardValueExprEvaluation(this, tripleSource.getValueFactory());
        EvaluationStrategies.register(this);
    }

    @Override
    public FederatedService getService(String serviceUrl) throws QueryEvaluationException {
        return serviceResolver.getService(serviceUrl);
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Service service, String serviceUri, CloseableIteration<BindingSet, QueryEvaluationException> bindings) throws QueryEvaluationException {
        try {
            return serviceResolver.getService(serviceUri).evaluate(service, bindings, service.getBaseURI());
        } catch (QueryEvaluationException e) {
            if (service.isSilent()) {
                return bindings;
            } else {
                throw new QueryEvaluationException(e);
            }
        }
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(TupleExpr expr, BindingSet bindings) throws QueryEvaluationException {
        return tupleEval.evaluate(expr, bindings);
    }

    @Override
    public Value evaluate(ValueExpr expr, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        return valueEval.evaluate(expr, bindings);
    }

    @Override
    public boolean isTrue(ValueExpr expr, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        return valueEval.isTrue(expr, bindings);
    }
}
