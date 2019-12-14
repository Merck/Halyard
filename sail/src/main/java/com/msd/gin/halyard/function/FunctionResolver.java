package com.msd.gin.halyard.function;

import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;

public interface FunctionResolver {
	Function resolveFunction(String iri);
}
