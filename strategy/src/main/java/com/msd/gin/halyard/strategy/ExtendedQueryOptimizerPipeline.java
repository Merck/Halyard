package com.msd.gin.halyard.strategy;

import com.msd.gin.halyard.optimizers.ConstrainedValueOptimizer;
import com.msd.gin.halyard.optimizers.ExtendedEvaluationStatistics;
import com.msd.gin.halyard.optimizers.HalyardFilterOptimizer;
import com.msd.gin.halyard.optimizers.QueryJoinOptimizer;

import java.util.Arrays;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizerPipeline;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.ConstantOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.ParentReferenceCleaner;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.RegexAsStringFunctionOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.StandardQueryOptimizerPipeline;

public class ExtendedQueryOptimizerPipeline implements QueryOptimizerPipeline {
	private final ExtendedEvaluationStatistics statistics;
	private final EvaluationStrategy strategy;
	private final ValueFactory valueFactory;

	public ExtendedQueryOptimizerPipeline(EvaluationStrategy strategy, ValueFactory valueFactory, ExtendedEvaluationStatistics statistics) {
		this.strategy = strategy;
		this.valueFactory = valueFactory;
		this.statistics = statistics;
	}

	@Override
	public Iterable<QueryOptimizer> getOptimizers() {
		return Arrays.asList(
			StandardQueryOptimizerPipeline.BINDING_ASSIGNER,
			StandardQueryOptimizerPipeline.BINDING_SET_ASSIGNMENT_INLINER,
			new ConstantOptimizer(strategy),
			new RegexAsStringFunctionOptimizer(valueFactory),
			StandardQueryOptimizerPipeline.COMPARE_OPTIMIZER,
			StandardQueryOptimizerPipeline.CONJUNCTIVE_CONSTRAINT_SPLITTER,
			StandardQueryOptimizerPipeline.DISJUNCTIVE_CONSTRAINT_OPTIMIZER,
			StandardQueryOptimizerPipeline.SAME_TERM_FILTER_OPTIMIZER,
			StandardQueryOptimizerPipeline.UNION_SCOPE_CHANGE_OPTIMIZER,
			StandardQueryOptimizerPipeline.QUERY_MODEL_NORMALIZER,
			StandardQueryOptimizerPipeline.PROJECTION_REMOVAL_OPTIMIZER, // Make sure this is after the UnionScopeChangeOptimizer
			new QueryJoinOptimizer(statistics),
			StandardQueryOptimizerPipeline.ITERATIVE_EVALUATION_OPTIMIZER,
			HalyardFilterOptimizer.PRE,
			new ConstrainedValueOptimizer(),
			HalyardFilterOptimizer.POST,
			StandardQueryOptimizerPipeline.ORDER_LIMIT_OPTIMIZER,
			new ParentReferenceCleaner()
		);
	}
}
