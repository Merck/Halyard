package com.msd.gin.halyard.strategy;

import java.util.Iterator;
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
	public String getThreadDump() {
		StringBuilder buf = new StringBuilder(1024);
		buf.append("Threads:\n");
		for (Map.Entry<Thread, Runnable> entry : getActiveTasks().entrySet()) {
			Thread t = entry.getKey();
			Runnable r = entry.getValue();
			buf.append("  ").append(t.getName())
				.append('[').append(t.getState()).append("]: ")
				.append(r).append('\n');
		}
		return buf.toString();
	}

	@Override
	public String getQueueDump() {
		int n = 10;
		StringBuilder buf = new StringBuilder(1024);
		buf.append("Queue (first " + n + "):\n");
		Iterator<Runnable> iter = getQueue().iterator();
		for (int i = 0; i < n && iter.hasNext(); ) {
			buf.append("  ").append(++i).append(": ").append(iter.next()).append('\n');
		}
		return buf.toString();
	}

	@Override
	public String toString() {
		return super.toString() + "\n" + getThreadDump() + "\n" + getQueueDump();
	}
}