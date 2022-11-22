package com.msd.gin.halyard.query;

import java.lang.ref.WeakReference;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.eclipse.rdf4j.query.QueryInterruptedException;

public final class TimeLimitBindingSetPipe extends BindingSetPipe {
	private static final Timer timer = new Timer("TimeLimitBindingSetPipe", true);

	public static BindingSetPipe apply(BindingSetPipe pipe, int timeLimitSecs) {
		if (timeLimitSecs > 0) {
			pipe = new TimeLimitBindingSetPipe(pipe, TimeUnit.SECONDS.toMillis(timeLimitSecs));
		}
		return pipe;
	}

	private final long timeLimitMillis;
	private final InterruptTask interruptTask;

	public TimeLimitBindingSetPipe(BindingSetPipe parent, long timeLimitMillis) {
		super(parent);
		assert timeLimitMillis > 0 : "time limit must be a positive number, is: " + timeLimitMillis;
		this.timeLimitMillis = timeLimitMillis;
		interruptTask = new InterruptTask(this);
		timer.schedule(interruptTask, timeLimitMillis);
	}

	@Override
	protected void doClose() {
		try {
			interruptTask.cancel();
		} finally {
			super.doClose();
		}
	}

	private void interrupt() {
		try {
			// throw exception to generate stack trace
			throw new QueryInterruptedException(String.format("Query evaluation exceeded specified timeout %ds", TimeUnit.MILLISECONDS.toSeconds(timeLimitMillis)));
		} catch (QueryInterruptedException ex) {
			handleException(ex);
		} finally {
			close();
		}
	}


	private static final class InterruptTask extends TimerTask {
		private final WeakReference<TimeLimitBindingSetPipe> handlerRef;

		private InterruptTask(TimeLimitBindingSetPipe handler) {
			handlerRef = new WeakReference<>(handler);
		}

		@Override
		public void run() {
			TimeLimitBindingSetPipe handler = handlerRef.get();
			if (handler != null) {
				handler.interrupt();
			}
		}
	}
}
