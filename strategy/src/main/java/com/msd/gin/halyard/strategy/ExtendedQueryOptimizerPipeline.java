package com.msd.gin.halyard.strategy;

import com.msd.gin.halyard.optimizers.ConstrainedValueOptimizer;
import com.msd.gin.halyard.optimizers.ExtendedEvaluationStatistics;
import com.msd.gin.halyard.optimizers.QueryJoinOptimizer;
import com.msd.gin.halyard.optimizers.TupleFunctionCallOptimizer;

import java.util.Arrays;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizerPipeline;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.BindingAssigner;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.BindingSetAssignmentInliner;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.CompareOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ConstantOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.DisjunctiveConstraintOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.FilterOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.IterativeEvaluationOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.OrderLimitOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ParentReferenceCleaner;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryModelNormalizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.RegexAsStringFunctionOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.SameTermFilterOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.UnionScopeChangeOptimizer;

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
			new BindingAssigner(),
			new BindingSetAssignmentInliner(),
			new ConstantOptimizer(strategy),
			new RegexAsStringFunctionOptimizer(valueFactory),
			new CompareOptimizer(),
			new ConjunctiveConstraintSplitter(),
			new DisjunctiveConstraintOptimizer(),
			new SameTermFilterOptimizer(),
			new UnionScopeChangeOptimizer(),
			new QueryModelNormalizer(),
			new QueryJoinOptimizer(statistics),
			// new SubSelectJoinOptimizer(),
			new IterativeEvaluationOptimizer(),
			new FilterOptimizer(),
			new ConstrainedValueOptimizer(),
			new OrderLimitOptimizer(),
			new TupleFunctionCallOptimizer(),
			new ParentReferenceCleaner()
		);
	}
}
