package com.msd.gin.halyard.algebra;

import com.msd.gin.halyard.common.ValueType;

import java.util.Objects;

import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.UnaryValueOperator;
import org.eclipse.rdf4j.query.algebra.ValueExpr;

public class ConstrainedStatementPattern extends StatementPattern {

	private static final long serialVersionUID = -1551292826547140642L;

	private ValueType subjectType;
	private ValueType objectType;
	private UnaryValueOperator literalConstraintFunction;
	private ValueExpr literalConstraintValue;

	public static ConstrainedStatementPattern replace(StatementPattern sp) {
		ConstrainedStatementPattern csp;
		if (sp instanceof ConstrainedStatementPattern) {
			csp = (ConstrainedStatementPattern) sp;
		} else {
			csp = new ConstrainedStatementPattern(sp);
			sp.replaceWith(csp);
		}
		return csp;
	}

	private ConstrainedStatementPattern(StatementPattern sp) {
		super(sp.getScope(), sp.getSubjectVar(), sp.getPredicateVar(), sp.getObjectVar(), sp.getContextVar());
	}

	public void setSubjectType(ValueType t) {
		this.subjectType = t;
	}

	public ValueType getSubjectType() {
		return this.subjectType;
	}

	public void setObjectType(ValueType t) {
		this.objectType = t;
	}

	public ValueType getObjectType() {
		return this.objectType;
	}

	public void setLiteralConstraint(UnaryValueOperator func, ValueExpr value) {
		this.objectType = ValueType.LITERAL;
		this.literalConstraintFunction = func;
		this.literalConstraintValue = value;
	}

	public UnaryValueOperator getLiteralConstraintFunction() {
		return literalConstraintFunction;
	}

	public ValueExpr getLiteralConstraintValue() {
		return literalConstraintValue;
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof ConstrainedStatementPattern) {
			ConstrainedStatementPattern o = (ConstrainedStatementPattern) other;
			return super.equals(other) && nullEquals(objectType, o.getObjectType())
					&& nullEquals(literalConstraintFunction, o.getLiteralConstraintFunction())
					&& nullEquals(literalConstraintValue, o.getLiteralConstraintValue());
		}
		return false;
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result ^= Objects.hashCode(objectType);
		result ^= Objects.hashCode(literalConstraintFunction);
		result ^= Objects.hashCode(literalConstraintValue);
		return result;
	}

	@Override
	public ConstrainedStatementPattern clone() {
		ConstrainedStatementPattern clone = (ConstrainedStatementPattern) super.clone();
		clone.setObjectType(getObjectType());
		if (getObjectType() == ValueType.LITERAL) {
			clone.setLiteralConstraint(getLiteralConstraintFunction().clone(), getLiteralConstraintValue().clone());
		}

		return clone;
	}
}
