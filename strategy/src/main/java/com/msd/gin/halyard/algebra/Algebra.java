package com.msd.gin.halyard.algebra;

import java.util.List;

import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.Var;

public final class Algebra {
	private Algebra() {}

	public static TupleExpr ensureRooted(TupleExpr tupleExpr) {
		if (!(tupleExpr instanceof QueryRoot)) {
			tupleExpr = new QueryRoot(tupleExpr);
		}
		return tupleExpr;
	}

	public static void remove(TupleExpr expr) {
		QueryModelNode parent = expr.getParentNode();
		if (parent instanceof Join) {
			Join join = (Join) parent;
			if (join.getLeftArg() == expr) {
				join.replaceWith(join.getRightArg());
			} else if (join.getRightArg() == expr) {
				join.replaceWith(join.getLeftArg());
			} else {
				throw new QueryEvaluationException(String.format("Corrupt join: {}", join));
			}
		} else if (parent instanceof Union) {
			Union union = (Union) parent;
			if (union.getLeftArg() == expr) {
				union.replaceWith(union.getRightArg());
			} else if (union.getRightArg() == expr) {
				union.replaceWith(union.getLeftArg());
			} else {
				throw new QueryEvaluationException(String.format("Corrupt union: {}", union));
			}
		} else {
			throw new QueryEvaluationException(String.format("Cannot remove {} from {}", expr.getSignature(), parent.getSignature()));
		}
	}

	/**
	 * Builds a right-recursive join tree.
	 * @param exprs list of expressions to join.
	 * @return join tree containing the given expressions.
	 */
	public static TupleExpr join(List<? extends TupleExpr> exprs) {
		int i = exprs.size()-1;
		TupleExpr te = exprs.get(i);
		for (i--; i>=0; i--) {
			te = new Join(exprs.get(i), te);
		}
		return te;
	}

	public static Var createAnonVar(String varName) {
		Var var = new Var(varName);
		var.setAnonymous(true);
		return var;
	}

}
