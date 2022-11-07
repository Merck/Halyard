package com.msd.gin.halyard.optimizers;

import com.google.common.collect.Sets;
import com.msd.gin.halyard.algebra.AbstractExtendedQueryModelVisitor;
import com.msd.gin.halyard.algebra.Algebra;
import com.msd.gin.halyard.algebra.ExtendedTupleFunctionCall;

import java.util.Set;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;

public class TupleFunctionCallOptimizer implements QueryOptimizer {

	@Override
	public void optimize(TupleExpr root, Dataset dataset, BindingSet bindings) {
		root.visit(new AbstractExtendedQueryModelVisitor<RuntimeException>() {
			@Override
			public void meet(ExtendedTupleFunctionCall tfc) {
				if (tfc.getDependentExpression() == null) {
					Set<String> reqdBindings = tfc.getRequiredBindingNames();
					if (!reqdBindings.isEmpty()) {
						new DependencyCollector(tfc, reqdBindings);
					} else {
						tfc.setDependentExpression(new SingletonSet());
					}
				}
			}

			@Override
			public void meet(Service node) {
				// leave for the remote endpoint
			}
		});
	}


	static final class DependencyCollector extends AbstractExtendedQueryModelVisitor<RuntimeException> {
		final ExtendedTupleFunctionCall tfc;
		final Set<String> reqdBindings;
		final Set<String> resultBindings;
		final Set<TupleExpr> ancestors = Sets.newIdentityHashSet();
		boolean done = false;

		DependencyCollector(ExtendedTupleFunctionCall tfc, Set<String> reqdBindings) {
			this.tfc = tfc;
			this.reqdBindings = reqdBindings;
			this.resultBindings = tfc.getResultBindingNames();
			// find all the ancestors
			TupleExpr child = tfc;
			ancestors.add(child);
			QueryModelNode parent = child.getParentNode();
			while (parent instanceof TupleExpr) {
				child = (TupleExpr) parent;
				ancestors.add(child);
				parent = child.getParentNode();
			}
			if (child != tfc) {
				child.visit(this);
			}
		}

		@Override
		protected void meetBinaryTupleOperator(BinaryTupleOperator node) {
			if (node instanceof Join) {
				if (!done) {
					checkForDependency(node.getLeftArg());
				}
				if (!done) {
					checkForDependency(node.getRightArg());
				}
			}
			if (!done) {
				super.meetBinaryTupleOperator(node);
			}
		}

		@Override
		protected void meetNode(QueryModelNode node) {
			if (!done) {
				super.meetNode(node);
			}
		}

		private void checkForDependency(TupleExpr expr) {
			if (!ancestors.contains(expr)) {
				Set<String> bnames = expr.getBindingNames();
				if (bnames.containsAll(reqdBindings) && Sets.intersection(bnames, resultBindings).isEmpty()) {
					Algebra.remove(expr);
					tfc.setDependentExpression(expr);
					done = true;
				}
			}
		}
	}
}
