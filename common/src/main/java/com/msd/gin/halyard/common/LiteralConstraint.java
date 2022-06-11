package com.msd.gin.halyard.common;

import com.msd.gin.halyard.vocab.HALYARD;

import java.util.Objects;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.vocabulary.RDF;

public final class LiteralConstraint extends ValueConstraint {
	private final IRI datatype;
	private final String lang;

	public LiteralConstraint(IRI datatype) {
		this(ValueType.LITERAL, Objects.requireNonNull(datatype), null);
	}

	public LiteralConstraint(String lang) {
		this(ValueType.LITERAL, RDF.LANGSTRING, Objects.requireNonNull(lang));
	}

	private LiteralConstraint(ValueType type, IRI datatype, String lang) {
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
		if (!v.isLiteral()) {
			return false;
		}
		Literal l = (Literal) v;
		IRI dt = l.getDatatype();
		if (!dt.equals(datatype)
			&& !(HALYARD.NON_STRING.equals(datatype) && !ValueIdentifier.isString(dt))
			&& !(HALYARD.ANY_NUMERIC.equals(datatype) && XMLDatatypeUtil.isNumericDatatype(dt))) {
			return false;
		}
		if (lang != null) {
			if (!l.getLanguage().map(lang -> lang.equals(this.lang)).orElse(Boolean.FALSE)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public int hashCode() {
		int h = super.hashCode();
		h = 89 * h + Objects.hashCode(datatype);
		h = 89 * h + Objects.hashCode(lang);
		return h;
	}

	@Override
	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (this.getClass() != other.getClass()) {
			return false;
		}
		LiteralConstraint that = (LiteralConstraint) other;
		return super.equals(that) && this.datatype.equals(that.datatype) && Objects.equals(this.lang, that.lang);
	}
}
