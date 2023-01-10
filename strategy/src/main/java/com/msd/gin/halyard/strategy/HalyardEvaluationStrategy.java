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

import com.google.common.base.Stopwatch;
import com.msd.gin.halyard.algebra.evaluation.ExtendedTripleSource;
import com.msd.gin.halyard.optimizers.HalyardEvaluationStatistics;
import com.msd.gin.halyard.optimizers.JoinAlgorithmOptimizer;
import com.msd.gin.halyard.query.BindingSetPipe;
import com.msd.gin.halyard.query.BindingSetPipeQueryEvaluationStep;
import com.msd.gin.halyard.query.ValuePipeQueryValueEvaluationStep;
import com.msd.gin.halyard.strategy.HalyardTupleExprEvaluation.QuadPattern;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.IterationWrapper;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryContext;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizerPipeline;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryValueEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext;
import org.eclipse.rdf4j.query.algebra.evaluation.util.QueryEvaluationUtility;

/**
 * Provides an efficient asynchronous parallel push {@code EvaluationStrategy} implementation for query evaluation in Halyard. This is the default strategy
 * in Halyard. An alternative strategy is the {@code StrictEvaluationStrategy} from RDF4J.
 * @author Adam Sotona (MSD)
 */
public class HalyardEvaluationStrategy implements EvaluationStrategy {
	public static final String QUERY_CONTEXT_SOURCE_STRING_ATTRIBUTE = "SourceString";

	private final Configuration conf;
    private final Dataset dataset;
	/**
	 * Used to allow queries across more than one Halyard datasets
	 */
    private final FederatedServiceResolver serviceResolver;
    private final TripleSource tripleSource;
    private final QueryContext queryContext;
	final HalyardEvaluationExecutor executor;
    /**
     * Evaluates TupleExpressions and all implementations of that interface
     */
    private final HalyardTupleExprEvaluation tupleEval;

    /**
     * Evaluates ValueExpr expressions and all implementations of that interface
     */
    private final HalyardValueExprEvaluation valueEval;

    private final boolean isStrict = false;

    /** Track the results size that each node in the query plan produces during execution. */
	boolean trackResultSize;

	/** Track the exeution time of each node in the plan. */
	boolean trackTime;

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
	 * @param statistics statistics to use
	 */
	public HalyardEvaluationStrategy(Configuration conf, TripleSource tripleSource, QueryContext queryContext,
			TupleFunctionRegistry tupleFunctionRegistry,
			FunctionRegistry functionRegistry, Dataset dataset, FederatedServiceResolver serviceResolver,
			HalyardEvaluationStatistics statistics, HalyardEvaluationExecutor executor) {
		this.conf = conf;
		this.tripleSource = tripleSource;
		this.queryContext = queryContext;
		this.dataset = dataset;
		this.serviceResolver = serviceResolver;
		this.executor = executor;
		this.tupleEval = new HalyardTupleExprEvaluation(this, queryContext, tupleFunctionRegistry, tripleSource,
				dataset);
		this.valueEval = new HalyardValueExprEvaluation(this, queryContext, functionRegistry, tripleSource);
		this.pipeline = new HalyardQueryOptimizerPipeline(this, tripleSource.getValueFactory(), statistics);
	}

	HalyardEvaluationStrategy(Configuration conf, TripleSource tripleSource, Dataset dataset,
			FederatedServiceResolver serviceResolver, HalyardEvaluationStatistics statistics) {
		this(conf, tripleSource, new QueryContext(), TupleFunctionRegistry.getInstance(), FunctionRegistry.getInstance(),
				dataset, serviceResolver, statistics, HalyardEvaluationExecutor.getInstance(conf));
	}

	@Override
	public void setTrackResultSize(boolean trackResultSize) {
		this.trackResultSize = trackResultSize;
	}

	@Override
	public void setTrackTime(boolean trackTime) {
		this.trackTime = trackTime;
	}

	boolean isStrict() {
		return isStrict;
	}

	Configuration getConfiguration() {
		return conf;
	}

	TripleSource getTripleSource() {
		return tripleSource;
	}

	String getSourceString() {
		return queryContext.getAttribute(QUERY_CONTEXT_SOURCE_STRING_ATTRIBUTE);
	}

	protected JoinAlgorithmOptimizer getJoinAlgorithmOptimizer() {
    	if (pipeline instanceof HalyardQueryOptimizerPipeline) {
    		return ((HalyardQueryOptimizerPipeline)pipeline).getJoinAlgorithmOptimizer();
    	} else {
    		return null;
    	}
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

    @Override
    public BindingSetPipeQueryEvaluationStep precompile(TupleExpr expr) {
    	BindingSetPipeQueryEvaluationStep step = tupleEval.precompile(expr);
    	return new BindingSetPipeQueryEvaluationStep() {
			@Override
			public void evaluate(BindingSetPipe parent, BindingSet bindings) {
				step.evaluate(parent, bindings);
			}

			@Override
			public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bindings) {
				return track(step.evaluate(bindings), expr);
			}
    	};
    }

    /**
	 * Called by RDF4J to evaluate a tuple expression
	 */
	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(TupleExpr expr, BindingSet bindings) throws QueryEvaluationException {
		return precompile(expr).evaluate(bindings);
	}

	CloseableIteration<BindingSet, QueryEvaluationException> track(CloseableIteration<BindingSet, QueryEvaluationException> iter, TupleExpr expr) {
		if (trackTime) {
			iter = new TimedIterator(iter, expr);
		}
	
		if (trackResultSize) {
			iter = new ResultSizeCountingIterator(iter, expr);
		}

		return iter;
	}

	void initTracking(TupleExpr queryNode) {
		if (trackResultSize) {
			synchronized (queryNode) {
				// set resultsSizeActual to at least be 0 so we can track iterations that don't produce anything
				queryNode.setResultSizeActual(Math.max(0, queryNode.getResultSizeActual()));
			}
		}
	}

	void incrementResultSizeActual(TupleExpr queryNode) {
		if (trackResultSize) {
			synchronized (queryNode) {
				queryNode.setResultSizeActual(queryNode.getResultSizeActual() + 1L);
			}
		}
	}

    @Override
    public ValuePipeQueryValueEvaluationStep precompile(ValueExpr expr, QueryEvaluationContext context) {
    	return valueEval.precompile(expr);
    }

	/**
     * Called by RDF4J to evaluate a value expression
     */
    @Override
    public Value evaluate(ValueExpr expr, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        return valueEval.precompile(expr).evaluate(bindings);
    }

    /**
     * Called by RDF4J to evaluate a binary expression
     */
    @Override
    public boolean isTrue(ValueExpr expr, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
    	return isTrue(valueEval.precompile(expr), bindings);
    }

	@Override
	public boolean isTrue(QueryValueEvaluationStep step, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
		Value value = step.evaluate(bindings);
		return QueryEvaluationUtility.getEffectiveBooleanValue(value).orElse(false);
	}

	boolean hasStatement(StatementPattern sp, BindingSet bindings) throws QueryEvaluationException {
		QuadPattern nq = tupleEval.getQuadPattern(sp, bindings);
		if (nq != null) {
			ExtendedTripleSource tripleSource = (ExtendedTripleSource) tupleEval.getTripleSource(sp, bindings);
			if (nq.isAllNamedContexts()) {
				// can't optimize for this
			    try (CloseableIteration<?, QueryEvaluationException> stmtIter = tupleEval.getStatements(nq, tripleSource)) {
			    	return stmtIter.hasNext();
			    }
			} else {
				return tripleSource.hasStatement(nq.subj, nq.pred, nq.obj, nq.ctxs);
			}
        } else {
        	return false;
        }
	}

	@Override
    public String toString() {
        return super.toString() + "[sourceString = " + getSourceString() + ", tripleSource = " + tripleSource + "]";
    }


	/**
	 * This class wraps an iterator and increments the "resultSizeActual" of the query model node that the iterator
	 * represents. This means we can track the number of tuples that have been retrieved from this node.
	 */
	private static final class ResultSizeCountingIterator extends IterationWrapper<BindingSet, QueryEvaluationException> {

		private final CloseableIteration<BindingSet, QueryEvaluationException> iterator;
		private final QueryModelNode queryModelNode;

		public ResultSizeCountingIterator(CloseableIteration<BindingSet, QueryEvaluationException> iterator,
				QueryModelNode queryModelNode) {
			super(iterator);
			this.iterator = iterator;
			this.queryModelNode = queryModelNode;
			// set resultsSizeActual to at least be 0 so we can track iterations that don't procude anything
			queryModelNode.setResultSizeActual(Math.max(0, queryModelNode.getResultSizeActual()));
		}

		@Override
		public BindingSet next() throws QueryEvaluationException {
			queryModelNode.setResultSizeActual(queryModelNode.getResultSizeActual() + 1);
			return iterator.next();
		}
	}

	/**
	 * This class wraps an iterator and tracks the time used to execute next() and hasNext()
	 */
	private static final class TimedIterator extends IterationWrapper<BindingSet, QueryEvaluationException> {

		private final CloseableIteration<BindingSet, QueryEvaluationException> iterator;
		private final QueryModelNode queryModelNode;

		private final Stopwatch stopwatch = Stopwatch.createUnstarted();

		public TimedIterator(CloseableIteration<BindingSet, QueryEvaluationException> iterator,
				QueryModelNode queryModelNode) {
			super(iterator);
			this.iterator = iterator;
			this.queryModelNode = queryModelNode;
			// set resultsSizeActual to at least be 0 so we can track iterations that don't procude anything
			queryModelNode.setTotalTimeNanosActual(Math.max(0, queryModelNode.getTotalTimeNanosActual()));
		}

		@Override
		public BindingSet next() throws QueryEvaluationException {
			stopwatch.reset();
			stopwatch.start();
			BindingSet next = iterator.next();
			stopwatch.stop();
			queryModelNode.setTotalTimeNanosActual(
					queryModelNode.getTotalTimeNanosActual() + stopwatch.elapsed(TimeUnit.NANOSECONDS));
			return next;
		}

		@Override
		public boolean hasNext() throws QueryEvaluationException {
			stopwatch.reset();
			stopwatch.start();
			boolean hasNext = super.hasNext();
			stopwatch.stop();
			queryModelNode.setTotalTimeNanosActual(
					queryModelNode.getTotalTimeNanosActual() + stopwatch.elapsed(TimeUnit.NANOSECONDS));
			return hasNext;
		}
	}
}
