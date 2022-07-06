package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Literal;

public interface ObjectLiteral<T> extends Literal {
	T objectValue();
}
