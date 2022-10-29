package com.msd.gin.halyard.strategy;

import com.msd.gin.halyard.sail.ExtendedSailConnection;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailConnectionWrapper;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

public class PushOnlyMemoryStore extends MemoryStore {

	@Override
    protected NotifyingSailConnection getConnectionInternal() throws SailException {
        return new PushOnlySailConnection(super.getConnectionInternal());
    }

	static final class PushOnlySailConnection extends NotifyingSailConnectionWrapper implements ExtendedSailConnection {
		protected PushOnlySailConnection(NotifyingSailConnection conn) {
			super(conn);
		}

		@Override
		public CloseableIteration<BindingSet,QueryEvaluationException> evaluate(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred) throws SailException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void evaluate(TupleQueryResultHandler handler, final TupleExpr tupleExpr, final Dataset dataset, final BindingSet bindings, final boolean includeInferred) throws SailException {
			ExtendedSailConnection.report(tupleExpr.getBindingNames(), getWrappedConnection().evaluate(tupleExpr, dataset, bindings, includeInferred), handler);
		}
	}
}
