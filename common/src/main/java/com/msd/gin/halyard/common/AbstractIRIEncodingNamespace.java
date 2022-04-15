package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.impl.SimpleNamespace;

public abstract class AbstractIRIEncodingNamespace extends SimpleNamespace implements IRIEncodingNamespace {
	private static final long serialVersionUID = -5434294594135541987L;

	public AbstractIRIEncodingNamespace(String prefix, String name) {
		super(prefix, name);
	}
}
