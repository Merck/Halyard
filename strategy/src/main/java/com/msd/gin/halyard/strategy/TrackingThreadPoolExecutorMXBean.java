package com.msd.gin.halyard.strategy;

import java.lang.Thread.State;

import javax.management.ConstructorParameters;

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
	int getQueueSize();
	ThreadInfo[] getThreadDump();
	QueueInfo[] getQueueDump();

	public static final class ThreadInfo {
		private final String name;
		private final Thread.State state;
		private final String task;

		@ConstructorParameters({"name", "state", "task"})
		public ThreadInfo(String name, State state, String task) {
			this.name = name;
			this.state = state;
			this.task = task;
		}

		public String getName() {
			return name;
		}

		public Thread.State getState() {
			return state;
		}

		public String getTask() {
			return task;
		}

		@Override
		public String toString() {
			return name + "[" + state + "]: " + task;
		}
	}

	public static final class QueueInfo {
		private final String task;

		@ConstructorParameters({"task"})
		public QueueInfo(String task) {
			this.task = task;
		}

		public String getTask() {
			return task;
		}

		@Override
		public String toString() {
			return task;
		}
	}
}
