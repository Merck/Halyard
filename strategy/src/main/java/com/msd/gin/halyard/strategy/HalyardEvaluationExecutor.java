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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.msd.gin.halyard.common.Config;
import com.msd.gin.halyard.strategy.HalyardEvaluationStrategy.ServiceRoot;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.LookAheadIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;

final class HalyardEvaluationExecutor {

    private static final int THREADS = Config.getInteger("halyard.evaluation.threads", 20);
    private static final int MAX_RETRIES = Config.getInteger("halyard.evaluation.maxRetries", 3);
    private static final int THREAD_GAIN = Config.getInteger("halyard.evaluation.threadGain", 2);
    private static final int MAX_THREADS = Config.getInteger("halyard.evaluation.maxThreads", 100);

    private static ThreadPoolExecutor createExecutor(String groupName, String namePrefix) {
        ThreadGroup tg = new ThreadGroup(groupName);
        AtomicInteger threadSeq = new AtomicInteger();
        ThreadFactory tf = (r) -> {
        	Thread thr = new Thread(tg, r, namePrefix+threadSeq.incrementAndGet());
        	thr.setDaemon(true);
        	return thr;
        };
        ThreadPoolExecutor executor = new ThreadPoolExecutor(THREADS, THREADS, 60L, TimeUnit.SECONDS, new PriorityBlockingQueue<>(64), tf);
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }
    private static final ThreadPoolExecutor EXECUTOR = createExecutor("Halyard Executors", "Halyard ");
    //a map of query model nodes and their priority
    private static final Cache<QueryModelNode, Integer> PRIORITY_MAP_CACHE = CacheBuilder.newBuilder().weakKeys().build();

    private static boolean checkThreads(int retries) {
		if (retries > MAX_RETRIES && EXECUTOR.getActiveCount() == EXECUTOR.getMaximumPoolSize()) {
			// failed to pull a new BindingSet and all threads are busy - add some spare threads
			if (EXECUTOR.getMaximumPoolSize() < MAX_THREADS) {
				EXECUTOR.setMaximumPoolSize(Math.min(EXECUTOR.getMaximumPoolSize()+THREAD_GAIN, MAX_THREADS));
				EXECUTOR.setCorePoolSize(Math.min(EXECUTOR.getCorePoolSize()+THREAD_GAIN, MAX_THREADS));
				return true;
			} else {
				throw new QueryEvaluationException(String.format("Maximum thread limit reached (%d)", MAX_THREADS));
			}
		}
		return false;
    }

    /**
     * Asynchronously pulls from an iteration of binding sets and pushes to a {@link BindingSetPipe}.
     * @param pipe the pipe that evaluation results are returned on
     * @param iter
     * @param node an implementation of any {@QueryModelNode} sub-type, typically a {@code ValueExpression}, {@Code UpdateExpression} or {@TupleExpression}
     */
	static void pullAndPushAsync(BindingSetPipe pipe,
			CloseableIteration<BindingSet, QueryEvaluationException> iter,
			QueryModelNode node) {
        int priority = getPriorityForNode(node);
		EXECUTOR.execute(new PipeAndIteration(pipe, iter, priority));
    }

	static void pullAndPush(BindingSetPipe pipe,
			CloseableIteration<BindingSet, QueryEvaluationException> iter) {
		PipeAndIteration pai = new PipeAndIteration(pipe, iter, 0);
		while(pai.pushNext());
	}

	static CloseableIteration<BindingSet, QueryEvaluationException> consumeAndQueue(Consumer<BindingSetPipe> action, QueryModelNode node) {
        int priority = getPriorityForNode(node);
        BindingSetPipeQueue queue = new BindingSetPipeQueue();
        EXECUTOR.execute(new PipeAndConsumer(queue.pipe, action, priority));
        return queue.iteration;
	}

	/**
     * Get the priority of this node from the PRIORITY_MAP_CACHE or determine the priority and then cache it. Also caches priority for sub-nodes of {@code node}
     * @param node the node that you want the priority for
     * @return the priority of the node, a count of the number of child nodes of {@code node}.
     */
    private static int getPriorityForNode(final QueryModelNode node) {
        Integer p = PRIORITY_MAP_CACHE.getIfPresent(node);
        if (p != null) {
            return p;
        } else {
            QueryModelNode root = node;
            while (root.getParentNode() != null) root = root.getParentNode(); //traverse to the root of the query model
            final AtomicInteger counter = new AtomicInteger(root instanceof ServiceRoot ? getPriorityForNode(((ServiceRoot)root).originalServiceArgs) : 0); //starting priority for ServiceRoot must be evaluated from the original service args node
            final AtomicInteger ret = new AtomicInteger();

            new AbstractQueryModelVisitor<RuntimeException>() {
                @Override
                protected void meetNode(QueryModelNode n) throws RuntimeException {
                    int pp = counter.getAndIncrement();
                    PRIORITY_MAP_CACHE.put(n, pp);
                    if (n == node || n == node.getParentNode()) ret.set(pp);
                    super.meetNode(n);
                }

                @Override
                public void meet(Filter node) throws RuntimeException {
                    super.meet(node);
                    node.getCondition().visit(this);
                }

                @Override
                public void meet(Service n) throws RuntimeException {
                    final int checkpoint = counter.get();
                    n.visitChildren(this);
                    int pp = counter.getAndIncrement();
                    PRIORITY_MAP_CACHE.put(n, pp);
                    if (n == node) ret.set(pp);
                    counter.getAndUpdate((int count) -> 2 * count - checkpoint + 1); //at least double the distance to have a space for service optimizations
                }

                @Override
                public void meet(LeftJoin node) throws RuntimeException {
                    super.meet(node);
                    if (node.hasCondition()) {
                        meetNode(node.getCondition());
                    }
                }
            }.meetOther(root);
            return ret.get();
        }
    }


    static abstract class PrioritizedTask implements Comparable<PrioritizedTask>, Runnable {
    	final int priority;

    	PrioritizedTask(int priority) {
    		this.priority = priority;
    	}

    	public final int getPriority() {
    		return priority;
    	}

    	@Override
		public final int compareTo(PrioritizedTask o) {
			return o.priority - this.priority;
		}
    }

    /**
     * A holder for the BindingSetPipe and the iterator over a tree of query sub-parts
     */
    static final class PipeAndIteration extends PrioritizedTask {
        private final BindingSetPipe pipe;
        private final CloseableIteration<BindingSet, QueryEvaluationException> iter;

        /**
         * Constructor for the class with the supplied variables
         * @param pipe The pipe to return evaluations to
         * @param iter The iterator over the evaluation tree
         * @param priority the 'level' of the evaluation in the over-all tree
         */
		PipeAndIteration(BindingSetPipe pipe,
				CloseableIteration<BindingSet, QueryEvaluationException> iter,
				int priority) {
			super(priority);
            this.pipe = pipe;
            this.iter = iter;
        }

		boolean pushNext() {
        	try {
            	if (!pipe.isClosed()) {
                	if(iter.hasNext()) {
                        BindingSet bs = iter.next();
                        if (pipe.push(bs)) { //true indicates more data is expected from this binding set, put it on the queue
                           	return true;
                        } else {
                        	pipe.close();
                        	iter.close();
                        }
                	} else {
            			pipe.close();
            		}
            	} else {
            		iter.close();
            	}
            } catch (Exception e) {
                pipe.handleException(e);
            }
        	return false;
		}

		@Override
    	public void run() {
        	if (pushNext()) {
                EXECUTOR.execute(this);
        	}
    	}
    }

    static final class PipeAndConsumer extends PrioritizedTask {
        private final BindingSetPipe pipe;
        private final Consumer<BindingSetPipe> action;

		PipeAndConsumer(BindingSetPipe pipe, Consumer<BindingSetPipe> action, int priority) {
			super(priority);
			this.pipe = pipe;
			this.action = action;
		}

		@Override
		public void run() {
			try {
				action.accept(pipe);
			} catch(RuntimeException e) {
				pipe.handleException(e);
			}
		}
    }

    private static final int MAX_QUEUE_SIZE = Config.getInteger("halyard.evaluation.maxQueueSize", 5000);
	private static final int POLL_TIMEOUT_MILLIS = Config.getInteger("halyard.evaluation.pollTimeoutMillis", 1000);
    private static final BindingSet END = new EmptyBindingSet();

    static final class BindingSetPipeQueue {

        private final LinkedBlockingQueue<BindingSet> queue = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);
        private volatile Exception exception;

        final BindingSetPipeIteration iteration = new BindingSetPipeIteration();
        final QueueingBindingSetPipe pipe = new QueueingBindingSetPipe();

        @Override
        public String toString() {
        	return "Queue "+Integer.toHexString(queue.hashCode());
        }

        final class BindingSetPipeIteration extends LookAheadIteration<BindingSet, QueryEvaluationException> {

            @Override
            protected BindingSet getNextElement() throws QueryEvaluationException {
    			BindingSet bs = null;
    			try {
                    for (int retries = 0; bs == null && !isClosed(); retries++) {
    					bs = queue.poll(POLL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    					if (exception instanceof RuntimeException) {
    						throw (RuntimeException) exception;
    					} else if (exception != null) {
                        	throw new QueryEvaluationException(exception);
                        }

						if (checkThreads(retries)) {
							retries = 0;
						}
                    }
                } catch (InterruptedException ex) {
                    throw new QueryEvaluationException(ex);
                }
                return bs == END ? null : bs;
            }

            @Override
            protected void handleClose() throws QueryEvaluationException {
                super.handleClose();
                pipe.isClosed = true;
                queue.clear();
            }

            @Override
            public String toString() {
            	return "Iteration for queue "+Integer.toHexString(queue.hashCode());
            }
        }

        final class QueueingBindingSetPipe extends BindingSetPipe {
        	volatile boolean isClosed = false;

            QueueingBindingSetPipe() {
            	super(null);
            }

            private boolean addToQueue(BindingSet bs) throws InterruptedException {
            	boolean added = false;
            	for (int retries = 0; !added && !isClosed(); retries++) {
            		added = queue.offer(bs, POLL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

					if (checkThreads(retries)) {
						retries = 0;
					}
            	}
            	return added;
            }

            @Override
            protected boolean next(BindingSet bs) throws InterruptedException {
                return addToQueue(bs);
            }

            @Override
            public void close() throws InterruptedException {
            	if(!isClosed) {
	                addToQueue(END);
	                isClosed = true;
            	}
            }

            @Override
            protected boolean handleException(Exception e) {
                if (exception != null) {
                	e.addSuppressed(exception);
                }
                exception = e;
                isClosed = true;
                return false;
            }

            @Override
            protected boolean isClosed() {
                return isClosed || iteration.isClosed();
            }

            @Override
            public String toString() {
            	return "Pipe for queue "+Integer.toHexString(queue.hashCode());
            }
        }
    }
}
