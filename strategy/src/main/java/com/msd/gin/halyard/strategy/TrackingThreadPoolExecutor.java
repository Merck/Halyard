package com.msd.gin.halyard.strategy;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class TrackingThreadPoolExecutor extends ThreadPoolExecutor implements TrackingThreadPoolExecutorMXBean {
	private final ConcurrentHashMap<Thread, Runnable> runningTasks;

	public TrackingThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
		runningTasks = new ConcurrentHashMap<>(maximumPoolSize);
	}

	public Map<Thread, Runnable> getActiveTasks() {
		return runningTasks;
	}

	@Override
	protected void beforeExecute(Thread t, Runnable r) {
		runningTasks.put(t, r);
	}

	@Override
	protected void afterExecute(Runnable r, Throwable t) {
		runningTasks.remove(Thread.currentThread());
	}

	@Override
	public String getState() {
		return toString();
	}

	@Override
	public String toString() {
		String s = super.toString();
		StringBuilder fullDetails = new StringBuilder(s);
		fullDetails.append("\nThreads:\n");
		for (Map.Entry<Thread, Runnable> entry : getActiveTasks().entrySet()) {
			Thread t = entry.getKey();
			Runnable r = entry.getValue();
			fullDetails.append("  ").append(t.getName())
				.append('[').append(t.getState()).append("]: ")
				.append(r.toString()).append('\n');
		}
		fullDetails.append("\nQueue:\n");
		for (Runnable r : getQueue()) {
			fullDetails.append(r.toString()).append('\n');
		}
		return fullDetails.toString();
	}
}