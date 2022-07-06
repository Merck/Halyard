/**
 * Copyright (c) 2016 Eclipse RDF4J contributors.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */
package com.msd.gin.halyard.sail.search;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;

public class SearchDocument {
	public String label;
	public String lang;
	public String datatype;

	@Override
	public String toString() {
		return String.format("label: %s, datatype: %s, lang: %s", label, datatype, lang);
	}

	public Literal createLiteral(ValueFactory vf) {
		if (lang != null) {
			return vf.createLiteral(label, lang);
		} else {
			return vf.createLiteral(label, vf.createIRI(datatype));
		}
	}
}