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
import com.msd.gin.halyard.optimizers.JoinAlgorithmOptimizer;
import com.msd.gin.halyard.optimizers.QueryJoinOptimizer;
import com.msd.gin.halyard.optimizers.StarJoinOptimizer;
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
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.DisjunctiveConstraintOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.IterativeEvaluationOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.OrderLimitOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.ParentReferenceCleaner;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.QueryModelNormalizerOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.RegexAsStringFunctionOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.SameTermFilterOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.UnionScopeChangeOptimizer;

/**
*
* @author Adam Sotona
*/
public final class HalyardQueryOptimizerPipeline implements QueryOptimizerPipeline {

	private final ExtendedEvaluationStatistics statistics;
	private final EvaluationStrategy strategy;
	private final ValueFactory valueFactory;
	private final JoinAlgorithmOptimizer joinAlgoOptimizer;

	public HalyardQueryOptimizerPipeline(EvaluationStrategy strategy, ValueFactory valueFactory, ExtendedEvaluationStatistics statistics) {
		this(strategy, valueFactory, statistics, 50000, 2.0f);
	}

	public HalyardQueryOptimizerPipeline(EvaluationStrategy strategy, ValueFactory valueFactory, ExtendedEvaluationStatistics statistics, int hashJoinLimit, float costRatio) {
		this.strategy = strategy;
		this.valueFactory = valueFactory;
		this.statistics = statistics;
		this.joinAlgoOptimizer = new JoinAlgorithmOptimizer(statistics, hashJoinLimit, costRatio);
	}

	JoinAlgorithmOptimizer getJoinAlgorithmOptimizer() {
		return joinAlgoOptimizer;
	}

	@Override
	public Iterable<QueryOptimizer> getOptimizers() {
		return Arrays.asList(
			new BindingAssignerOptimizer(),
			new BindingSetAssignmentInlinerOptimizer(),
			new HalyardConstantOptimizer(strategy),
			new RegexAsStringFunctionOptimizer(valueFactory),
			new CompareOptimizer(),
			new ConjunctiveConstraintSplitterOptimizer(),
			new DisjunctiveConstraintOptimizer(),
			new SameTermFilterOptimizer(),
			new StarJoinOptimizer(),
			new UnionScopeChangeOptimizer(),
			new QueryModelNormalizerOptimizer(),
			(statistics instanceof HalyardEvaluationStatistics) ? new HalyardQueryJoinOptimizer((HalyardEvaluationStatistics) statistics) : new QueryJoinOptimizer(statistics),
			// new SubSelectJoinOptimizer(),
			new IterativeEvaluationOptimizer(),
			new HalyardFilterOptimizer(),
			new ConstrainedValueOptimizer(),
			new OrderLimitOptimizer(),
			new TupleFunctionCallOptimizer(),
			new ParentReferenceCleaner(),
			joinAlgoOptimizer
		);
	}
}
