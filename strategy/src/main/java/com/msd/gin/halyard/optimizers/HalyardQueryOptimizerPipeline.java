package com.msd.gin.halyard.optimizers;

import java.util.Arrays;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizerPipeline;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.BindingAssigner;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.CompareOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ConstantOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.DisjunctiveConstraintOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.IterativeEvaluationOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.OrderLimitOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryJoinOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryModelNormalizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.RegexAsStringFunctionOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.SameTermFilterOptimizer;

public final class HalyardQueryOptimizerPipeline implements QueryOptimizerPipeline, EvaluationStatisticsDependent {

	private final EvaluationStrategy strategy;
	private final ValueFactory valueFactory;

	private EvaluationStatistics statistics;

	public HalyardQueryOptimizerPipeline(EvaluationStrategy strategy, ValueFactory valueFactory) {
		this.strategy = strategy;
		this.valueFactory = valueFactory;
	}

	@Override
	public void setEvaluationStatistics(EvaluationStatistics statistics) {
		this.statistics = statistics;
	}

	@Override
	public Iterable<QueryOptimizer> getOptimizers() {
		return Arrays.asList(
			new BindingAssigner(),
			new ConstantOptimizer(strategy),
			new RegexAsStringFunctionOptimizer(valueFactory),
			new CompareOptimizer(),
			new ConjunctiveConstraintSplitter(),
			new DisjunctiveConstraintOptimizer(),
			new SameTermFilterOptimizer(),
			new QueryModelNormalizer(),
			(statistics instanceof HalyardEvaluationStatistics) ? new HalyardQueryJoinOptimizer((HalyardEvaluationStatistics) statistics) :
				new QueryJoinOptimizer(statistics),
			// new SubSelectJoinOptimizer(),
			new IterativeEvaluationOptimizer(),
			new HalyardFilterOptimizer(), // apply filter optimizer twice (before and after Joins and Unions shaking)
			new OrderLimitOptimizer()
		);
	}
}
