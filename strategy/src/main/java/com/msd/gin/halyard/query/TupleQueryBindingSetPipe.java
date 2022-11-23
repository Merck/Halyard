package com.msd.gin.halyard.query;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryInterruptedException;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;

public class TupleQueryBindingSetPipe extends BindingSetPipe {
	private final CountDownLatch doneLatch = new CountDownLatch(1);
	private final TupleQueryResultHandler handler;
	private volatile Throwable exception;

	public TupleQueryBindingSetPipe(Set<String> bindingNames, TupleQueryResultHandler handler) {
		super(null);
		this.handler = handler;
		handler.startQueryResult(new ArrayList<>(bindingNames));
	}

	/**
	 * NB: BindingSetPipes are designed to be used concurrently but TupleQueryResultHandlers aren't so need to synchronize access.
	 */
	@Override
	protected boolean next(BindingSet bs) {
		synchronized (handler) {
			handler.handleSolution(bs);
		}
		return true;
	}

	@Override
	public boolean handleException(Throwable e) {
		exception = e;
		doneLatch.countDown();
		return false;
	}

	@Override
	protected void doClose() {
		synchronized (handler) {
			handler.endQueryResult();
		}
		doneLatch.countDown();
	}

	public void waitUntilClosed(int timeoutSecs) throws QueryEvaluationException {
		try {
			if (timeoutSecs > 0) {
				if (!doneLatch.await(timeoutSecs, TimeUnit.SECONDS)) {
					throw new QueryInterruptedException(String.format("Query evaluation exceeded specified timeout %ds", timeoutSecs));
				}
			} else {
				doneLatch.await();
			}
		} catch (InterruptedException ie) {
			throw new QueryEvaluationException(ie);
		}
		Throwable e = exception;
		if (e != null) {
			if (e instanceof QueryEvaluationException) {
				throw (QueryEvaluationException) e;
			} else {
				throw new QueryEvaluationException(e);
			}
		}
	}
}
