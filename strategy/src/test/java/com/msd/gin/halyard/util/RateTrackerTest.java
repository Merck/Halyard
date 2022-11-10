package com.msd.gin.halyard.util;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class RateTrackerTest {
	@Test
	public void testFixedRate() throws InterruptedException {
		Timer timer = new Timer(true);
		AtomicInteger count = new AtomicInteger(548); // initial count value should not matter
		long updateTime = 200;
		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				count.incrementAndGet();
			}
		}, 0, updateTime);

		RateTracker tracker = new RateTracker(timer, 100, 10, () -> count.get());
		
		tracker.start();
		// wait for tracker to get a full window + some random amount of time
		Thread.sleep(Math.round(tracker.getWindowDurationMillis()+Math.random()*400));
		assertEquals(5f, tracker.getRatePerSecond(), 0.1f);
		timer.cancel();
	}
}
