package com.msd.gin.halyard.algebra;

import java.util.HashSet;
import java.util.Set;

import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.TupleFunctionCall;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;

public class ExtendedTupleFunctionCall extends TupleFunctionCall {
	private static final long serialVersionUID = 2773708379343562817L;

	private TupleExpr depExpr;

	public ExtendedTupleFunctionCall(String uri) {
		setURI(uri);
		setDependentExpression(new SingletonSet());
	}

	public void setDependentExpression(TupleExpr expr) {
		assert expr != null : "expr must not be null";
		expr.setParentNode(this);
		this.depExpr = expr;
	}

	public TupleExpr getDependentExpression() {
		return depExpr;
	}

	@Override
	public Set<String> getBindingNames() {
		Set<String> bindingNames = super.getAssuredBindingNames();
		bindingNames.addAll(depExpr.getBindingNames());
		return bindingNames;
	}

	@Override
	public Set<String> getAssuredBindingNames() {
		Set<String> bindingNames = super.getAssuredBindingNames();
		bindingNames.addAll(depExpr.getAssuredBindingNames());
		return bindingNames;
	}

	@Override
	public <X extends Exception> void visitChildren(QueryModelVisitor<X> visitor) throws X {
		super.visitChildren(visitor);
		depExpr.visit(visitor);
	}

	@Override
	public void replaceChildNode(QueryModelNode current, QueryModelNode replacement) {
		if (current == depExpr) {
			setDependentExpression((TupleExpr) replacement);
		} else {
			super.replaceChildNode(current, replacement);
		}
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof ExtendedTupleFunctionCall && super.equals(other)) {
			ExtendedTupleFunctionCall o = (ExtendedTupleFunctionCall) other;
			return depExpr.equals(o.getDependentExpression());
		}
		return false;
	}

	@Override
	public int hashCode() {
		return super.hashCode() ^ depExpr.hashCode();
	}

	@Override
	public ExtendedTupleFunctionCall clone() {
		ExtendedTupleFunctionCall clone = (ExtendedTupleFunctionCall) super.clone();
		clone.setDependentExpression(getDependentExpression().clone());
		return clone;
	}

	public Set<String> getRequiredBindingNames() {
		Set<String> names = new HashSet<>();
		for (ValueExpr expr : getArgs()) {
			if (expr instanceof Var) {
				Var var = (Var) expr;
				if (!var.hasValue()) {
					names.add(var.getName());
				}
			}
		}
		return names;
	}

	public Set<String> getResultBindingNames() {
		Set<String> names = new HashSet<>();
		for (Var var : getResultVars()) {
			names.add(var.getName());
		}
		return names;
	}
}
