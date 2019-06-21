package com.msd.gin.halyard.common;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

import javax.xml.datatype.XMLGregorianCalendar;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;

public abstract class LiteralWrapper implements Literal {
	private static final long serialVersionUID = -3751940963092784186L;
	private final Literal literal;

	protected LiteralWrapper(Literal literal) {
		this.literal = literal;
	}

	@Override
	public final String getLabel() {
		return literal.getLabel();
	}

	public final Optional<String> getLanguage() {
		return literal.getLanguage();
	}

	public final IRI getDatatype() {
		return literal.getDatatype();
	}

	public final byte byteValue() {
		return literal.byteValue();
	}

	public final short shortValue() {
		return literal.shortValue();
	}

	public final int intValue() {
		return literal.intValue();
	}

	public final long longValue() {
		return literal.longValue();
	}

	public final BigInteger integerValue() {
		return literal.integerValue();
	}

	public final BigDecimal decimalValue() {
		return literal.decimalValue();
	}

	public final float floatValue() {
		return literal.floatValue();
	}

	public final double doubleValue() {
		return literal.doubleValue();
	}

	public final boolean booleanValue() {
		return literal.booleanValue();
	}

	public final XMLGregorianCalendar calendarValue() {
		return literal.calendarValue();
	}

	@Override
	public final String stringValue() {
		return literal.stringValue();
	}

	public final String toString() {
		return literal.toString();
	}

	public final int hashCode() {
		return literal.hashCode();
	}

	public final boolean equals(Object o) {
		return literal.equals(o);
	}
}
