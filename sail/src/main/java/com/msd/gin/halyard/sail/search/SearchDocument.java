/**
 * Copyright (c) 2016 Eclipse RDF4J contributors.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */
package com.msd.gin.halyard.sail.search;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.msd.gin.halyard.common.IdentifiableValue;
import com.msd.gin.halyard.common.RDFFactory;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;

public class SearchDocument {
	public static final String ID_FIELD = "id";
	public static final String LABEL_FIELD = "label";
	public static final String LANG_FIELD = "lang";
	public static final String DATATYPE_FIELD = "datatype";

	@JsonProperty(ID_FIELD)
	public String id;
	@JsonProperty(LABEL_FIELD)
	public String label;
	@JsonProperty(LANG_FIELD)
	public String lang;
	@JsonProperty(DATATYPE_FIELD)
	public String datatype;

	@Override
	public String toString() {
		return String.format("id: %s, label: %s, datatype: %s, lang: %s", id, label, datatype, lang);
	}

	public Literal createLiteral(ValueFactory vf, RDFFactory rdfFactory) {
		Literal l;
		if (lang != null) {
			l = vf.createLiteral(label, lang);
		} else {
			l = vf.createLiteral(label, vf.createIRI(datatype));
		}
		if (l instanceof IdentifiableValue) {
			((IdentifiableValue) l).setId(rdfFactory, rdfFactory.idFromString(id));
		}
		return l;
	}
}