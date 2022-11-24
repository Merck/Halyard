/**
 * Copyright (c) 2016 Eclipse RDF4J contributors.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */
package com.msd.gin.halyard.query;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryInterruptedException;

/**
 * Pushes data to a consumer.
 */
public final class QueueingBindingSetPipe extends BindingSetPipe {
    private static final Object END_OF_QUEUE = new Object();
	private final BlockingQueue<Object> queue;
	private final long timeout;
	private final TimeUnit unit;
	private final Thread consumerThread;

	public QueueingBindingSetPipe(int maxQueueSize, long timeout, TimeUnit unit) {
    	super(null);
    	this.queue = new LinkedBlockingQueue<>(maxQueueSize);
    	this.timeout = timeout;
    	this.unit = unit;
    	this.consumerThread = Thread.currentThread();
    }

    public void collect(Consumer<BindingSet> consumer) {
    	boolean isEnd = false;
		while (!isEnd) {
			Object next = poll(timeout, unit);
			isEnd = isEndOfQueue(next);
			if (!isEnd) {
				if (next != null) {
					consumer.accept((BindingSet) next);
				} else {
	    			throw new QueryInterruptedException(String.format("Exceeded time-out of %d%s waiting for producer", timeout, toString(unit)));
				}
			}
		}
    }

    public Object poll(long pollTimeout, TimeUnit unit) {
    	Object o;
    	try {
			o = queue.poll(pollTimeout, unit);
		} catch (InterruptedException ie) {
			throw new QueryInterruptedException(ie);
		}
    	if (o instanceof Throwable) {
    		Throwable ex = (Throwable) o;
			if (ex instanceof QueryEvaluationException) {
				throw (QueryEvaluationException) ex;
			} else {
            	throw new QueryEvaluationException(ex);
            }
    	}
   		return o;
    }

    public boolean isEndOfQueue(Object o) {
		return o == END_OF_QUEUE;
	}

	private boolean addToQueue(Object bs) {
		boolean added;
		try {
			added = queue.offer(bs, timeout, unit);
	    } catch (InterruptedException ie) {
			consumerThread.interrupt();
			return false;
	    }

		if (added) {
    		return true;
    	} else {
    		// timed-out
    		try {
    			// throw to generate a stack trace
    			throw new QueryInterruptedException(String.format("Exceeded time-out of %d%s waiting for consumer", timeout, toString(unit)));
    		} catch (QueryInterruptedException e) {
    			return handleException(e);
    		}
    	}
    }

    @Override
    protected boolean next(BindingSet bs) {
		return addToQueue(bs);
    }

    @Override
    protected void doClose() {
		addToQueue(END_OF_QUEUE);
    }

    @Override
    public boolean handleException(Throwable e) {
        queue.clear();
        addToQueue(e);
        return false;
    }

    @Override
    public String toString() {
    	return "Pipe "+Integer.toHexString(this.hashCode())+" for queue "+Integer.toHexString(queue.hashCode());
    }

    private static String toString(TimeUnit unit) {
    	switch (unit) {
    		case NANOSECONDS:
    			return "ns";
    		case MICROSECONDS:
    			return "μs";
    		case MILLISECONDS:
    			return "ms";
    		case SECONDS:
    			return "s";
    		case MINUTES:
    			return "min";
    		case HOURS:
    			return "hr";
    		case DAYS:
    			return "d";
    		default:
    			throw new IllegalArgumentException(String.format("%s not yet supported", unit));
    	}
    }
}
