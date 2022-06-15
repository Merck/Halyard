package com.msd.gin.halyard.common;

import com.msd.gin.halyard.vocab.HALYARD;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

import javax.xml.datatype.XMLGregorianCalendar;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.base.CoreDatatype;

public class InternalObjectLiteral<T> implements ObjectLiteral<T> {
	private static final long serialVersionUID = -124780683447095687L;

	private final T obj;

	public InternalObjectLiteral(T o) {
		this.obj = o;
	}

	public T objectValue() {
		return obj;
	}

	@Override
	public String stringValue() {
		throw new IllegalArgumentException("Object content");
	}

	@Override
	public String getLabel() {
		throw new IllegalArgumentException("Object content");
	}

	@Override
	public Optional<String> getLanguage() {
		return Optional.empty();
	}

	@Override
	public IRI getDatatype() {
		return HALYARD.JAVA_TYPE;
	}

	@Override
	public CoreDatatype getCoreDatatype() {
		return CoreDatatype.NONE;
	}

	@Override
	public boolean booleanValue() {
		throw new IllegalArgumentException("Object content");
	}

	@Override
	public byte byteValue() {
		throw new NumberFormatException("Object content");
	}

	@Override
	public short shortValue() {
		throw new NumberFormatException("Object content");
	}

	@Override
	public int intValue() {
		throw new NumberFormatException("Object content");
	}

	@Override
	public long longValue() {
		throw new NumberFormatException("Object content");
	}

	@Override
	public BigInteger integerValue() {
		throw new NumberFormatException("Object content");
	}

	@Override
	public BigDecimal decimalValue() {
		throw new NumberFormatException("Object content");
	}

	@Override
	public float floatValue() {
		throw new NumberFormatException("Object content");
	}

	@Override
	public double doubleValue() {
		throw new NumberFormatException("Object content");
	}

	@Override
	public XMLGregorianCalendar calendarValue() {
		throw new IllegalArgumentException("Object content");
	}

}
