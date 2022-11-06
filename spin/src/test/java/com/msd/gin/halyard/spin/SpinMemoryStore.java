package com.msd.gin.halyard.spin;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryContext;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.TupleFunctionEvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.QueryContextIteration;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.StandardQueryOptimizerPipeline;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.memory.MemoryStoreConnection;

import com.msd.gin.halyard.sail.connection.SailConnectionQueryPreparer;

public class SpinMemoryStore extends MemoryStore {

	private final SpinParser spinParser = new SpinParser();
	private final FunctionRegistry functionRegistry = FunctionRegistry.getInstance();
	private final TupleFunctionRegistry tupleFunctionRegistry = TupleFunctionRegistry.getInstance();

	@Override
	public void init() {
		super.init();
		SpinFunctionInterpreter.registerSpinParsingFunctions(spinParser, functionRegistry, tupleFunctionRegistry);
		SpinMagicPropertyInterpreter.registerSpinParsingTupleFunctions(spinParser, tupleFunctionRegistry);
	}

	@Override
    protected NotifyingSailConnection getConnectionInternal() throws SailException {
        return new SpinMemoryStoreConnection(this);
    }

	final class SpinMemoryStoreConnection extends MemoryStoreConnection {
		protected SpinMemoryStoreConnection(MemoryStore sail) {
			super(sail);
		}

		@Override
        protected EvaluationStrategy getEvaluationStrategy(Dataset dataset, final TripleSource tripleSource) {
			EvaluationStatistics stats = new EvaluationStatistics();
			TupleFunctionEvaluationStrategy evalStrat = new TupleFunctionEvaluationStrategy(tripleSource, dataset, getFederatedServiceResolver(), tupleFunctionRegistry, 0, stats);
			evalStrat.setOptimizerPipeline(new StandardQueryOptimizerPipeline(evalStrat, tripleSource, stats) {
				@Override
				public Iterable<QueryOptimizer> getOptimizers() {
					List<QueryOptimizer> optimizers = new ArrayList<>();
					optimizers.add(new SpinFunctionInterpreter(spinParser, tripleSource, functionRegistry));
					optimizers.add(new SpinMagicPropertyInterpreter(spinParser, tripleSource, tupleFunctionRegistry, null));
					for (QueryOptimizer optimizer : super.getOptimizers()) {
						optimizers.add(optimizer);
					}
					return optimizers;
				}
			});
			return evalStrat;
        }

		private QueryContext createQueryContext(boolean includeInferred) {
			SailConnectionQueryPreparer queryPreparer = new SailConnectionQueryPreparer(this, includeInferred, getValueFactory());
			QueryContext queryContext = new QueryContext(queryPreparer);
			return queryContext;
		}

		@Override
		protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluateInternal(
				TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred)
			throws SailException
		{
			QueryContext queryContext = createQueryContext(includeInferred);
			queryContext.begin();
			try {
				CloseableIteration<? extends BindingSet, QueryEvaluationException> iter = super.evaluateInternal(tupleExpr, dataset, bindings, includeInferred);
				return new QueryContextIteration(iter, queryContext);
			} finally {
				queryContext.end();
			}
		}
	}
}
