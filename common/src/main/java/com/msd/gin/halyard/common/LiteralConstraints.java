package com.msd.gin.halyard.common;

import java.util.function.Predicate;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;

public final class LiteralConstraints implements Predicate<Literal> {
	private final IRI datatype;
	private final String lang;

	public LiteralConstraints() {
		this(null, null);
	}

	public LiteralConstraints(IRI datatype) {
		this(datatype, null);
	}

	public LiteralConstraints(String lang) {
		this(RDF.LANGSTRING, lang);
	}

	private LiteralConstraints(IRI datatype, String lang) {
		this.datatype = datatype;
		this.lang = lang;
	}

	public IRI getDatatype() {
		return datatype;
	}

	public String getLanguageTag() {
		return lang;
	}

	public boolean allLiterals() {
		return (datatype == null) && (lang == null);
	}

	public boolean stringsOnly() {
		return XSD.STRING.equals(datatype) || RDF.LANGSTRING.equals(datatype);
	}

	@Override
	public boolean test(Literal l) {
		if (allLiterals()) {
			return true;
		} else if (lang != null) {
			return l.getLanguage().map(lang -> lang.equals(this.lang)).orElse(Boolean.FALSE);
		} else {
			return l.getDatatype().equals(this.datatype);
		}
	}
}
