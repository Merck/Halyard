package com.msd.gin.halyard.optimizers;

import com.msd.gin.halyard.algebra.AbstractExtendedQueryModelVisitor;
import com.msd.gin.halyard.algebra.Algorithms;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;

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

	public int getHashJoinLimit() {
		return hashJoinLimit;
	}

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		tupleExpr.visit(new AbstractExtendedQueryModelVisitor<RuntimeException>() {
			@Override
			public void meet(Join join) {
				super.meet(join);
				selectJoinAlgorithm(join);
			}

			@Override
			public void meet(LeftJoin leftJoin) {
				super.meet(leftJoin);
				selectJoinAlgorithm(leftJoin);
			}
		});
	}

	private void selectJoinAlgorithm(BinaryTupleOperator join) {
		TupleExpr left = join.getLeftArg();
		TupleExpr right = join.getRightArg();
		if (isSupported(left) && isSupported(right)) {
			double leftCard = statistics.getCardinality(left);
			double rightCard = statistics.getCardinality(right);
			double nestedRightCard = statistics.getCardinality(right, left.getBindingNames());
			double nestedCost = leftCard * nestedRightCard * INDEX_LOOKUP_COST;
			double hashCost = leftCard*HASH_LOOKUP_COST + rightCard*HASH_BUILD_COST;
			boolean useHash = rightCard <= hashJoinLimit && costRatio*hashCost < nestedCost;
			if (useHash) {
				join.setAlgorithm(Algorithms.HASH_JOIN);
				join.setCostEstimate(hashCost);
			}
		}
	}

	/**
	 * NB: Hash-join only coincides with SPARQL semantics in a few special cases (e.g. no complex scoping).
	 * @param expr expression to check
	 * @return true if a hash-join can be used
	 */
	private static boolean isSupported(TupleExpr expr) {
		return (expr instanceof StatementPattern)
				|| (expr instanceof BindingSetAssignment)
				|| ((expr instanceof BinaryTupleOperator) && Algorithms.HASH_JOIN.equals(((BinaryTupleOperator)expr).getAlgorithmName()));
	}
}
