/*
 * Copyright 2016 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
 * Inc., Kenilworth, NJ, USA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.msd.gin.halyard.strategy;

import java.util.concurrent.TimeUnit;

/**
 * Avoids the cost of calling System.currentTimeMillis() too often.
 */
public final class TimeoutTracker {

	public static final int TIMEOUT_POLL_MILLIS = 1000;

	private final long timeoutSecs;
    private final long endTime;
	private int counter = 0;
	private int timeoutCount = 1;
	private long counterStartTime;

	public TimeoutTracker(long startTime, long timeoutSecs) {
		this.timeoutSecs = timeoutSecs;
		this.endTime = startTime + TimeUnit.SECONDS.toMillis(timeoutSecs);
		this.counterStartTime = startTime;
	}

	public long getTimeout() {
		return timeoutSecs;
	}

	public boolean checkTimeout() {
		if (timeoutSecs > 0) {
			counter++;
			if (counter >= timeoutCount) {
				long time = System.currentTimeMillis();
				if (time > endTime) {
					return true;
				}
				int elapsed = (int) (time - counterStartTime);
				if (elapsed > 0) {
					timeoutCount = (timeoutCount * TIMEOUT_POLL_MILLIS) / elapsed;
					if (timeoutCount == 0) {
						timeoutCount = 1;
					}
				} else {
					timeoutCount *= 2;
				}
				counter = 0;
				counterStartTime = time;
			}
		}
		return false;
	}
}
