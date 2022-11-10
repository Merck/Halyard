package com.msd.gin.halyard.util;

import java.util.ArrayDeque;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

public final class RateTracker {
	private final Timer timer;
	private final int updateMillis;
	private final int windowSize;
	private final LongSupplier countSupplier;
	private volatile float ratePerSec;
	private TimerTask task;

	public RateTracker(Timer timer, int updateMillis, int windowSize, LongSupplier countSupplier) {
		this.timer = timer;
		this.updateMillis = updateMillis;
		this.windowSize = windowSize;
		this.countSupplier = countSupplier;
	}

	public int getWindowDurationMillis() {
		return updateMillis * windowSize;
	}

	public void start() {
		task = new TimerTask() {
			final ArrayDeque<Sample> samples = new ArrayDeque<>(windowSize);

			{
				samples.addFirst(new Sample(countSupplier.getAsLong()));
			}

			@Override
			public void run() {
				Sample current = new Sample(countSupplier.getAsLong());
				samples.addFirst(current);
				if (samples.size() == windowSize) {
					samples.removeLast();
				}

				Sample oldest = samples.getLast();
				int countDelta = (int) (current.count - oldest.count);
				long timeDelta = (current.timestamp - oldest.timestamp);
				ratePerSec = ((float) (countDelta*1000))/((float) timeDelta);
			}
		};
		timer.scheduleAtFixedRate(task, 0L, updateMillis);
	}

	public void stop() {
		task.cancel();
	}

	public float getRatePerSecond() {
		return ratePerSec;
	}

	private static class Sample {
		final long count;
		final long timestamp = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());

		public Sample(long count) {
			this.count = count;
		}
	}
}
