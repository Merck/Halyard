package com.msd.gin.halyard.common;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;

public final class Values {
	private Values() {}

	public static Literal literal(ValueFactory vf, Object object, boolean failOnUnknownType) {
		if (object instanceof BigInteger) {
			return vf.createLiteral((BigInteger)object);
		} else if (object instanceof BigDecimal) {
			return vf.createLiteral((BigDecimal)object);
		} else {
			return org.eclipse.rdf4j.model.util.Values.literal(vf, object, failOnUnknownType);
		}
	}
}
