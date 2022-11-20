package com.msd.gin.halyard.algebra;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.rdf4j.query.algebra.AbstractQueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

public abstract class NAryTupleOperator extends AbstractQueryModelNode implements TupleExpr {
	private static final long serialVersionUID = -3171973153588379214L;
	private TupleExpr[] args;

	public void setArgs(List<? extends TupleExpr> exprs) {
		args = new TupleExpr[exprs.size()];
		for (int i=0; i<exprs.size(); i++) {
			TupleExpr te = exprs.get(i);
			setArg(i, te);
		}
	}

	private void setArg(int i, TupleExpr te) {
		te.setParentNode(this);
		args[i] = te;
	}

	public List<? extends TupleExpr> getArgs() {
		return Arrays.asList(args);
	}

	public int getArgCount() {
		return args.length;
	}

	@Override
	public <X extends Exception> void visitChildren(final QueryModelVisitor<X> visitor) throws X {
		for (TupleExpr arg : args) {
			arg.visit(visitor);
		}
	}

	@Override
	public void replaceChildNode(final QueryModelNode current, final QueryModelNode replacement) {
		for (int i=0; i<args.length; i++) {
			if (current == args[i]) {
				setArg(i, (TupleExpr) replacement);
				return;
			}
		}
	}

	@Override
	public Set<String> getBindingNames() {
		Set<String> bindingNames = new LinkedHashSet<>(16);
		for (TupleExpr arg : args) {
			bindingNames.addAll(arg.getBindingNames());
		}
		return bindingNames;
	}

	@Override
	public Set<String> getAssuredBindingNames() {
		Set<String> bindingNames = new LinkedHashSet<>(16);
		for (TupleExpr arg : args) {
			bindingNames.addAll(arg.getAssuredBindingNames());
		}
		return bindingNames;
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof NAryTupleOperator) {
			NAryTupleOperator o = (NAryTupleOperator) other;
			return getArgs().equals(o.getArgs());
		}

		return false;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(args);
	}

	@Override
	public NAryTupleOperator clone() {
		NAryTupleOperator clone = (NAryTupleOperator) super.clone();

		for (int i=0; i<args.length; i++) {
			TupleExpr exprClone = args[i].clone();
			clone.setArg(i, exprClone);
		}

		return clone;
	}
}
