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
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.BindingAssignerOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.BindingSetAssignmentInlinerOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.CompareOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.ConjunctiveConstraintSplitterOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.ConstantOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.DisjunctiveConstraintOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.FilterOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.IterativeEvaluationOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.OrderLimitOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.ParentReferenceCleaner;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.QueryModelNormalizerOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.RegexAsStringFunctionOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.SameTermFilterOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.UnionScopeChangeOptimizer;

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
			new BindingAssignerOptimizer(),
			new BindingSetAssignmentInlinerOptimizer(),
			new ConstantOptimizer(strategy),
			new RegexAsStringFunctionOptimizer(valueFactory),
			new CompareOptimizer(),
			new ConjunctiveConstraintSplitterOptimizer(),
			new DisjunctiveConstraintOptimizer(),
			new SameTermFilterOptimizer(),
			new UnionScopeChangeOptimizer(),
			new QueryModelNormalizerOptimizer(),
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
