package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;

public interface StatementValue<V extends Value> extends Value {
	V getValue();
	Statement getStatement();
}
