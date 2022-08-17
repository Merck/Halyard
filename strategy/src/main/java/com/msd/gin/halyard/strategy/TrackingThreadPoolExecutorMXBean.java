package com.msd.gin.halyard.strategy;

public interface TrackingThreadPoolExecutorMXBean {
	int getCorePoolSize();
	void setCorePoolSize(int size);
	int getMaximumPoolSize();
	void setMaximumPoolSize(int size);
	int getPoolSize();
	int getLargestPoolSize();
	int getActiveCount();
	long getCompletedTaskCount();
	long getTaskCount();
	String getState();
}
