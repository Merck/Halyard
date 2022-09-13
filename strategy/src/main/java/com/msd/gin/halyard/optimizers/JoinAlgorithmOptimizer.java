package com.msd.gin.halyard.optimizers;

import com.msd.gin.halyard.algebra.HashJoin;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

public class JoinAlgorithmOptimizer implements QueryOptimizer {
	private static double INDEX_LOOKUP_COST = 10;
	private static double HASH_LOOKUP_COST = 0.1;
	private static double HASH_BUILD_COST = 0.3;

	private final ExtendedEvaluationStatistics statistics;
	private final int hashJoinLimit;
	private final float costRatio;

	public JoinAlgorithmOptimizer(ExtendedEvaluationStatistics stats, int hashJoinLimit, float ratio) {
		this.statistics = stats;
		this.hashJoinLimit = hashJoinLimit;
		this.costRatio = ratio;
	}

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		tupleExpr.visit(new AbstractQueryModelVisitor<RuntimeException>() {
			@Override
			public void meet(Join join) throws RuntimeException {
				selectJoinAlgorithm(join);
				super.meet(join);
			}

			@Override
			public void meet(LeftJoin leftJoin) throws RuntimeException {
				selectJoinAlgorithm(leftJoin);
				super.meet(leftJoin);
			}
		});
	}

	private void selectJoinAlgorithm(BinaryTupleOperator join) {
		TupleExpr left = join.getLeftArg();
		TupleExpr right = join.getRightArg();
		double leftCard = statistics.getCardinality(left);
		double rightCard = statistics.getCardinality(right);
		double nestedRightCard = statistics.getCardinality(right, left.getBindingNames());
		double nestedCost = leftCard * nestedRightCard * INDEX_LOOKUP_COST;
		double hashCost = leftCard*HASH_LOOKUP_COST + rightCard*HASH_BUILD_COST;
		boolean useHash = rightCard <= hashJoinLimit && costRatio*hashCost < nestedCost;
		if (useHash) {
			join.setAlgorithm(HashJoin.INSTANCE);
			join.setCostEstimate(hashCost);
		}
	}
}
