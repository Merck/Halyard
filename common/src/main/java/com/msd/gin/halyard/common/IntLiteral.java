package com.msd.gin.halyard.common;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

import javax.xml.datatype.XMLGregorianCalendar;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;

public final class IntLiteral implements Literal {
	private static final long serialVersionUID = -8560649777651486999L;

	private final String label;
	private final int number;
	private final IRI datatype;

	public IntLiteral(int v, IRI datatype) {
		this.label = XMLDatatypeUtil.toString(v);
		this.number = v;
		this.datatype = datatype;
	}

	@Override
	public String stringValue() {
		return label;
	}

	@Override
	public String getLabel() {
		return label;
	}

	@Override
	public Optional<String> getLanguage() {
		return Optional.empty();
	}

	@Override
	public IRI getDatatype() {
		return datatype;
	}

	@Override
	public CoreDatatype getCoreDatatype() {
		return CoreDatatype.from(datatype);
	}

	@Override
	public boolean booleanValue() {
		if (number == 1) {
			return true;
		} else if (number == 0) {
			return false;
		} else {
			throw new IllegalArgumentException("Malformed value");
		}
	}

	@Override
	public byte byteValue() {
		return (byte) number;
	}

	@Override
	public short shortValue() {
		return (short) number;
	}

	@Override
	public int intValue() {
		return number;
	}

	@Override
	public long longValue() {
		return number;
	}

	@Override
	public BigInteger integerValue() {
		return BigInteger.valueOf(number);
	}

	@Override
	public BigDecimal decimalValue() {
		return BigDecimal.valueOf(number);
	}

	@Override
	public float floatValue() {
		return number;
	}

	@Override
	public double doubleValue() {
		return number;
	}

	@Override
	public XMLGregorianCalendar calendarValue() {
		throw new IllegalArgumentException("Malformed value");
	}

	@Override
	public boolean equals(Object o) {
		return this == o || o instanceof Literal
				&& label.equals(((Literal) o).getLabel())
				&& datatype.equals(((Literal) o).getDatatype());
	}

	@Override
	public int hashCode() {
		return label.hashCode();
	}

	@Override
	public String toString() {
		return "\"" + label + "\"^^<" + datatype.stringValue() + ">";
	}
}
