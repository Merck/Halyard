package com.msd.gin.halyard.common;

import java.lang.ref.WeakReference;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryResultHandlerException;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;

public abstract class TimeLimitTupleQueryResultHandler implements TupleQueryResultHandler {
	private static final Timer timer = new Timer("TimeLimitIteration", true);

	private final TupleQueryResultHandler handler;
	private final InterruptTask interruptTask;
	private volatile boolean isInterrupted = false;

	public TimeLimitTupleQueryResultHandler(TupleQueryResultHandler handler, long timeLimitMs) {
		assert timeLimitMs > 0 : "time limit must be a positive number, is: " + timeLimitMs;
		this.handler = handler;
		interruptTask = new InterruptTask(this);
		timer.schedule(interruptTask, timeLimitMs);
	}

	@Override
	public void startQueryResult(List<String> bindingNames) throws TupleQueryResultHandlerException {
		checkInterrupted();
		handler.startQueryResult(bindingNames);
	}

	@Override
	public void handleBoolean(boolean value) throws QueryResultHandlerException {
		checkInterrupted();
		handler.handleBoolean(value);
	}

	@Override
	public void handleLinks(List<String> linkUrls) throws QueryResultHandlerException {
		checkInterrupted();
		handler.handleLinks(linkUrls);
	}

	@Override
	public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
		checkInterrupted();
		handler.handleSolution(bindingSet);
	}

	@Override
	public void endQueryResult() throws TupleQueryResultHandlerException {
		try {
			interruptTask.cancel();
		} finally {
			handler.endQueryResult();
		}
	}

	private void checkInterrupted() {
		if (isInterrupted) {
			throwInterruptedException();
		}
	}

	protected abstract void throwInterruptedException() throws TupleQueryResultHandlerException;

	void interrupt() {
		isInterrupted = true;
	}


	static final class InterruptTask extends TimerTask {
		private final WeakReference<TimeLimitTupleQueryResultHandler> handlerRef;

		InterruptTask(TimeLimitTupleQueryResultHandler handler) {
			handlerRef = new WeakReference<>(handler);
		}

		@Override
		public void run() {
			TimeLimitTupleQueryResultHandler handler = handlerRef.get();
			if (handler != null) {
				handler.interrupt();
			}
		}
	}
}
