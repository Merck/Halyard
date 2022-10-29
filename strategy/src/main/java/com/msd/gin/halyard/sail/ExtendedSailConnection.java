package com.msd.gin.halyard.sail;

import java.util.ArrayList;
import java.util.Set;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.impl.IteratingTupleQueryResult;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;

public interface ExtendedSailConnection extends SailConnection {
	default void evaluate(TupleQueryResultHandler handler, final TupleExpr tupleExpr, final Dataset dataset, final BindingSet bindings, final boolean includeInferred) throws SailException {
		report(tupleExpr.getBindingNames(), evaluate(tupleExpr, dataset, bindings, includeInferred), handler);
	}

	static void report(Set<String> bindingNames, CloseableIteration<? extends BindingSet, QueryEvaluationException> iter, TupleQueryResultHandler handler) {
		TupleQueryResult result = new IteratingTupleQueryResult(new ArrayList<>(bindingNames), iter);
		QueryResults.report(result, handler);
	}
}
