package com.msd.gin.halyard.common;

import java.util.Objects;
import java.util.function.Predicate;

import org.eclipse.rdf4j.model.Value;

public class ValueConstraint implements Predicate<Value> {

	private final ValueType type;

	public ValueConstraint(ValueType type) {
		this.type = Objects.requireNonNull(type);
	}

	public ValueType getValueType() {
		return type;
	}

	@Override
	public boolean test(Value v) {
		switch (type) {
			case LITERAL: return v.isLiteral();
			case TRIPLE: return v.isTriple();
			case IRI: return v.isIRI();
			case BNODE: return v.isBNode();
			default:
				throw new AssertionError();
		}
	}

	@Override
	public int hashCode() {
		return type.hashCode();
	}

	@Override
	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (!(other instanceof ValueConstraint)) {
			return false;
		}
		ValueConstraint that = (ValueConstraint) other;
		return this.type.equals(that.type);
	}
}
