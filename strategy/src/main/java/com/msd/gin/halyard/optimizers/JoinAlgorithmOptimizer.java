package com.msd.gin.halyard.optimizers;

import com.msd.gin.halyard.algebra.NestedLoops;

import java.util.Deque;
import java.util.LinkedList;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.TupleFunctionCall;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

public class JoinAlgorithmOptimizer implements QueryOptimizer {

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		tupleExpr.visit(new AbstractQueryModelVisitor<RuntimeException>() {
			private final Deque<Join> joins = new LinkedList<>();

			@Override
			public void meet(Join node) throws RuntimeException {
				joins.addLast(node);
				super.meet(node);
				joins.removeLast();
			}

			@Override
			public void meetOther(QueryModelNode node) throws RuntimeException {
				if (node instanceof TupleFunctionCall) {
					if (!joins.isEmpty()) {
						// TupleFunctionCalls depend on the resultant binding sets from the driver expression of the join
						joins.getLast().setAlgorithm(new NestedLoops());
					}
				} else {
					super.meetOther(node);
				}
			}
			
		});
	}
}
