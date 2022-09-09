package com.msd.gin.halyard.strategy;

public interface HalyardEvaluationExecutorMXBean {
	void setMaxRetries(int maxRetries);
	int getMaxRetries();

	void setQueuePollTimeoutMillis(int millis);
	int getQueuePollTimeoutMillis();

	TrackingThreadPoolExecutorMXBean getThreadPoolExecutor();
}
