package com.msd.gin.halyard.function;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;

final class Values {
	private Values() {}

	static Literal literal(ValueFactory vf, Object object, boolean failOnUnknownType) {
		// until supported in RDF4J
		if (object instanceof BigInteger) {
			return vf.createLiteral((BigInteger)object);
		} else if (object instanceof BigDecimal) {
			return vf.createLiteral((BigDecimal)object);
		} else {
			return org.eclipse.rdf4j.model.util.Values.literal(vf, object, failOnUnknownType);
		}
	}
}
