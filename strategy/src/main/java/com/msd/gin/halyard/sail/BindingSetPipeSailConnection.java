package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.query.BindingSetPipe;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.sail.SailConnection;

public interface BindingSetPipeSailConnection extends SailConnection {
	default void evaluate(BindingSetPipe handler, final TupleExpr tupleExpr, final Dataset dataset, final BindingSet bindings, final boolean includeInferred) {
		report(evaluate(tupleExpr, dataset, bindings, includeInferred), handler);
	}

	static void report(CloseableIteration<? extends BindingSet, QueryEvaluationException> iter, BindingSetPipe handler) {
		try {
			while (iter.hasNext()) {
				handler.push(iter.next());
			}
			handler.close();
		} catch (QueryEvaluationException e) {
			handler.handleException(e);
		}
	}
}
