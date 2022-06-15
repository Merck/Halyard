package com.msd.gin.halyard.common;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.DateTimeException;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;
import java.util.Optional;

import javax.xml.datatype.XMLGregorianCalendar;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.base.CoreDatatype;

public abstract class LiteralWrapper implements Literal {
	private static final long serialVersionUID = -3751940963092784186L;
	protected final Literal literal;

	protected LiteralWrapper(Literal literal) {
		this.literal = literal;
	}

	@Override
	public final String getLabel() {
		return literal.getLabel();
	}

	@Override
	public final Optional<String> getLanguage() {
		return literal.getLanguage();
	}

	@Override
	public final CoreDatatype getCoreDatatype() {
		return literal.getCoreDatatype();
	}

	@Override
	public final IRI getDatatype() {
		return literal.getDatatype();
	}

	@Override
	public final byte byteValue() {
		return literal.byteValue();
	}

	@Override
	public final short shortValue() {
		return literal.shortValue();
	}

	@Override
	public final int intValue() {
		return literal.intValue();
	}

	@Override
	public final long longValue() {
		return literal.longValue();
	}

	@Override
	public final BigInteger integerValue() {
		return literal.integerValue();
	}

	@Override
	public final BigDecimal decimalValue() {
		return literal.decimalValue();
	}

	@Override
	public final float floatValue() {
		return literal.floatValue();
	}

	@Override
	public final double doubleValue() {
		return literal.doubleValue();
	}

	@Override
	public final boolean booleanValue() {
		return literal.booleanValue();
	}

	@Override
	public final XMLGregorianCalendar calendarValue() {
		return literal.calendarValue();
	}

	@Override
	public final TemporalAccessor temporalAccessorValue() throws DateTimeException {
		return literal.temporalAccessorValue();
	}

	@Override
	public final TemporalAmount temporalAmountValue() throws DateTimeException {
		return literal.temporalAmountValue();
	}

	@Override
	public final String stringValue() {
		return literal.stringValue();
	}

	@Override
	public final String toString() {
		return literal.toString();
	}

	@Override
	public final int hashCode() {
		return literal.hashCode();
	}

	public final boolean equals(Object o) {
		return literal.equals(o);
	}
}
