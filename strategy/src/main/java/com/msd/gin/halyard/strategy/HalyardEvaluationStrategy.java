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

import com.msd.gin.halyard.optimizers.HalyardEvaluationStatistics;

import java.util.Objects;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryContext;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizerPipeline;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.query.algebra.evaluation.util.EvaluationStrategies;

/**
 * Provides an efficient asynchronous parallel push {@code EvaluationStrategy} implementation for query evaluation in Halyard. This is the default strategy
 * in Halyard. An alternative strategy is the {@code StrictEvaluationStrategy} from RDF4J.
 * @author Adam Sotona (MSD)
 */
public final class HalyardEvaluationStrategy implements EvaluationStrategy {

    public static final class ServiceRoot extends QueryRoot {
        private static final long serialVersionUID = 7052207623408379003L;

        public final TupleExpr originalServiceArgs;

        public ServiceRoot(TupleExpr serviceArgs) {
            super(serviceArgs.clone());
            this.originalServiceArgs = serviceArgs;
        }
    }

	private final Dataset dataset;
	/**
	 * Used to allow queries across more than one Halyard datasets
	 */
    private final FederatedServiceResolver serviceResolver;
    /**
     * Evaluates TupleExpressions and all implementations of that interface
     */
    private final HalyardTupleExprEvaluation tupleEval;

    /**
     * Evaluates ValueExpr expressions and all implementations of that interface
     */
    private final HalyardValueExprEvaluation valueEval;

	private QueryOptimizerPipeline pipeline;

    /**
     * Ensures 'now' is the same across all parts of the query evaluation chain.
     */
    Value sharedValueOfNow;

    /**
	 * Default constructor of HalyardEvaluationStrategy
	 * 
	 * @param tripleSource {@code TripleSource} to be queried for the existence of triples in a context
	 * @param queryContext {@code QueryContext} to use for query evaluation
	 * @param tupleFunctionRegistry {@code TupleFunctionRegistry} to use for {@code TupleFunctionCall} evaluation.
	 * @param functionRegistry {@code FunctionRegistry} to use for {@code FunctionCall} evaluation.
	 * @param dataset {@code Dataset} A dataset consists of a default graph for read and using operations, which is the RDF merge of one or more graphs, a set of named graphs, and
	 * a single update graph for INSERT and DELETE
	 * @param serviceResolver {@code FederatedServiceResolver} resolver for any federated services (graphs) required for the evaluation
	 */
	public HalyardEvaluationStrategy(TripleSource tripleSource, QueryContext queryContext,
			TupleFunctionRegistry tupleFunctionRegistry,
			FunctionRegistry functionRegistry, Dataset dataset, FederatedServiceResolver serviceResolver,
			HalyardEvaluationStatistics statistics) {
		this.dataset = dataset;
        this.serviceResolver = serviceResolver;
		this.tupleEval = new HalyardTupleExprEvaluation(this, queryContext, tupleFunctionRegistry, tripleSource,
				dataset);
		this.valueEval = new HalyardValueExprEvaluation(this, queryContext, functionRegistry, tripleSource);
		this.pipeline = new HalyardQueryOptimizerPipeline(this, tripleSource.getValueFactory(), statistics);
        EvaluationStrategies.register(this);
    }

	public HalyardEvaluationStrategy(TripleSource tripleSource, Dataset dataset,
			FederatedServiceResolver serviceResolver, HalyardEvaluationStatistics statistics) {
		this(tripleSource, new QueryContext(), TupleFunctionRegistry.getInstance(), FunctionRegistry.getInstance(),
				dataset, serviceResolver, statistics);
	}

    /**
     * Get a service for a federated dataset.
     */
    @Override
    public FederatedService getService(String serviceUrl) throws QueryEvaluationException {
        if (serviceResolver == null) {
            throw new QueryEvaluationException("No Service Resolver set.");
        }
        return serviceResolver.getService(serviceUrl);
    }

	@Override
	public void setOptimizerPipeline(QueryOptimizerPipeline pipeline) {
		Objects.requireNonNull(pipeline);
		this.pipeline = pipeline;
	}

	@Override
	public TupleExpr optimize(TupleExpr expr, EvaluationStatistics evaluationStatistics, BindingSet bindings) {
		TupleExpr optimizedExpr = expr;

		for (QueryOptimizer optimizer : pipeline.getOptimizers()) {
			optimizer.optimize(optimizedExpr, dataset, bindings);
		}
		return optimizedExpr;
	}

    /**
     * Called by RDF4J to evaluate a query or part of a query using a service
     */
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Service service, String serviceUri, CloseableIteration<BindingSet, QueryEvaluationException> bindings) throws QueryEvaluationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Called by RDF4J to evaluate a tuple expression
     */
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(TupleExpr expr, BindingSet bindings) throws QueryEvaluationException {
        return tupleEval.evaluate(expr, bindings);
    }

    /**
     * Called by RDF4J to evaluate a value expression
     */
    @Override
    public Value evaluate(ValueExpr expr, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        return valueEval.evaluate(expr, bindings);
    }

    /**
     * Called by RDF4J to evaluate a binary expression
     */
    @Override
    public boolean isTrue(ValueExpr expr, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        return valueEval.isTrue(expr, bindings);
    }
}
