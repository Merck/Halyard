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

import com.msd.gin.halyard.optimizers.ConstrainedValueOptimizer;
import com.msd.gin.halyard.optimizers.ExtendedEvaluationStatistics;
import com.msd.gin.halyard.optimizers.HalyardConstantOptimizer;
import com.msd.gin.halyard.optimizers.HalyardEvaluationStatistics;
import com.msd.gin.halyard.optimizers.HalyardFilterOptimizer;
import com.msd.gin.halyard.optimizers.HalyardQueryJoinOptimizer;
import com.msd.gin.halyard.optimizers.StarJoinOptimizer;

import java.util.Arrays;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizerPipeline;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.BindingAssigner;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.BindingSetAssignmentInliner;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.CompareOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.DisjunctiveConstraintOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.IterativeEvaluationOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.OrderLimitOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ParentReferenceCleaner;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryJoinOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryModelNormalizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.RegexAsStringFunctionOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.SameTermFilterOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.UnionScopeChangeOptimizer;

/**
*
* @author Adam Sotona
*/
public final class HalyardQueryOptimizerPipeline implements QueryOptimizerPipeline {

	private final ExtendedEvaluationStatistics statistics;
	private final EvaluationStrategy strategy;
	private final ValueFactory valueFactory;

	public HalyardQueryOptimizerPipeline(EvaluationStrategy strategy, ValueFactory valueFactory, ExtendedEvaluationStatistics statistics) {
		this.strategy = strategy;
		this.valueFactory = valueFactory;
		this.statistics = statistics;
	}

	@Override
	public Iterable<QueryOptimizer> getOptimizers() {
		return Arrays.asList(
			new BindingAssigner(),
			new BindingSetAssignmentInliner(),
			new HalyardConstantOptimizer(strategy),
			new RegexAsStringFunctionOptimizer(valueFactory),
			new CompareOptimizer(),
			new ConjunctiveConstraintSplitter(),
			new DisjunctiveConstraintOptimizer(),
			new SameTermFilterOptimizer(),
			new StarJoinOptimizer(),
			new UnionScopeChangeOptimizer(),
			new QueryModelNormalizer(),
			(statistics instanceof HalyardEvaluationStatistics) ? new HalyardQueryJoinOptimizer((HalyardEvaluationStatistics) statistics) : new QueryJoinOptimizer(statistics),
			// new SubSelectJoinOptimizer(),
			new IterativeEvaluationOptimizer(),
			new HalyardFilterOptimizer(), // apply filter optimizer twice (before and after Joins and Unions shaking)
			new ConstrainedValueOptimizer(),
			new OrderLimitOptimizer(),
			new ParentReferenceCleaner()
		);
	}
}
