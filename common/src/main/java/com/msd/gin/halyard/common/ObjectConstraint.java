package com.msd.gin.halyard.common;

import com.msd.gin.halyard.vocab.HALYARD;

import java.util.Objects;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.vocabulary.RDF;

public final class ObjectConstraint extends ValueConstraint {
	private final IRI datatype;
	private final String lang;

	public ObjectConstraint(ValueType type) {
		this(type, null, null);
	}

	public ObjectConstraint(IRI datatype) {
		this(ValueType.LITERAL, datatype, null);
	}

	public ObjectConstraint(String lang) {
		this(ValueType.LITERAL, RDF.LANGSTRING, lang);
	}

	private ObjectConstraint(ValueType type, IRI datatype, String lang) {
		super(type);
		this.datatype = datatype;
		this.lang = lang;
	}

	public IRI getDatatype() {
		return datatype;
	}

	public String getLanguageTag() {
		return lang;
	}

	@Override
	public boolean test(Value v) {
		if (!super.test(v)) {
			return false;
		}
		if (datatype != null) {
			if (!v.isLiteral()) {
				return false;
			}
			Literal l = (Literal) v;
			IRI dt = l.getDatatype();
			if (!dt.equals(datatype)
				&& !(HALYARD.NON_STRING.equals(datatype) && !Identifier.isString(dt))
				&& !(HALYARD.ANY_NUMERIC.equals(datatype) && XMLDatatypeUtil.isNumericDatatype(dt))) {
				return false;
			}
			if (lang != null) {
				if (!l.getLanguage().map(lang -> lang.equals(this.lang)).orElse(Boolean.FALSE)) {
					return false;
				}
			}
		}
		return true;
	}

	@Override
	public int hashCode() {
		return super.hashCode() ^ Objects.hashCode(datatype) ^ Objects.hashCode(lang);
	}

	@Override
	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (!(other instanceof ObjectConstraint)) {
			return false;
		}
		ObjectConstraint that = (ObjectConstraint) other;
		return super.equals(that) && Objects.equals(this.datatype, that.datatype) && Objects.equals(this.lang, that.lang);
	}
}
