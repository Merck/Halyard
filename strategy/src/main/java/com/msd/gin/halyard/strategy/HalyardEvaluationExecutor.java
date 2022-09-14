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
import com.msd.gin.halyard.algebra.ServiceRoot;
import com.msd.gin.halyard.common.Config;

import java.lang.management.ManagementFactory;
import java.util.Hashtable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.LookAheadIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class HalyardEvaluationExecutor implements HalyardEvaluationExecutorMXBean {
	private static final Logger LOGGER = LoggerFactory.getLogger(HalyardEvaluationExecutor.class);
    private static final BindingSet END_OF_QUEUE = new EmptyBindingSet();
    // high default priority for dynamically created query nodes
    private static final int DEFAULT_PRIORITY = 65535;
    private static final int RETRY_LIMIT = 50;

	static final HalyardEvaluationExecutor INSTANCE = new HalyardEvaluationExecutor();

	private static TrackingThreadPoolExecutor createExecutor(String groupName, String namePrefix, int threads) {
		ThreadGroup tg = new ThreadGroup(groupName);
		AtomicInteger threadSeq = new AtomicInteger();
		ThreadFactory tf = (r) -> {
			Thread thr = new Thread(tg, r, namePrefix+threadSeq.incrementAndGet());
			thr.setDaemon(true);
			return thr;
		};
		// fixed-size thread pool that can wind down when idle
		TrackingThreadPoolExecutor executor = new TrackingThreadPoolExecutor(threads, threads, 60L, TimeUnit.SECONDS, new PriorityBlockingQueue<>(64), tf);
		executor.allowCoreThreadTimeOut(true);
		return executor;
	}

    private int threads = Config.getInteger("halyard.evaluation.threads", 20);
    private int maxRetries = Config.getInteger("halyard.evaluation.maxRetries", 3);
    private int threadGain = Config.getInteger("halyard.evaluation.threadGain", 5);
    private int maxThreads = Config.getInteger("halyard.evaluation.maxThreads", 100);

	private final TrackingThreadPoolExecutor executor = createExecutor("Halyard Executors", "Halyard ", threads);
    // a map of query model nodes and their priority
    private final Cache<QueryModelNode, Integer> priorityMapCache = CacheBuilder.newBuilder().weakKeys().build();

    private int maxQueueSize = Config.getInteger("halyard.evaluation.maxQueueSize", 5000);
	private int pollTimeoutMillis = Config.getInteger("halyard.evaluation.pollTimeoutMillis", 1000);

	private volatile long previousCompletedTaskCount;

	private static void registerMBeans(MBeanServer mbs, HalyardEvaluationExecutor executor) throws JMException {
		{
			Hashtable<String,String> attrs = new Hashtable<>();
			attrs.put("type", HalyardEvaluationExecutor.class.getName());
			attrs.put("id", Integer.toString(executor.hashCode()));
			mbs.registerMBean(executor, ObjectName.getInstance("com.msd.gin.halyard", attrs));
		}
		{
			Hashtable<String,String> attrs = new Hashtable<>();
			attrs.put("type", TrackingThreadPoolExecutor.class.getName());
			attrs.put("id", Integer.toString(executor.executor.hashCode()));
			mbs.registerMBean(executor.executor, ObjectName.getInstance("com.msd.gin.halyard", attrs));
		}
	}

	static {
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		try {
			registerMBeans(mbs, INSTANCE);
		} catch (JMException e) {
			throw new AssertionError(e);
		}
	}

	@Override
	public void setMaxRetries(int maxRetries) {
		this.maxRetries = maxRetries;
	}

	@Override
	public int getMaxRetries() {
		return maxRetries;
	}

	@Override
	public void setQueuePollTimeoutMillis(int millis) {
		this.pollTimeoutMillis = millis;
	}

	@Override
	public int getQueuePollTimeoutMillis() {
		return pollTimeoutMillis;
	}

	@Override
	public TrackingThreadPoolExecutorMXBean getThreadPoolExecutor() {
		return executor;
	}

	private boolean checkThreads(int retries) {
		boolean resetRetries;
		final int maxPoolSize = executor.getMaximumPoolSize();
		// if we've been consistently blocked and are at full capacity
		if (retries > maxRetries && executor.getActiveCount() >= maxPoolSize) {
			// if we are not blocked overall then don't worry about it - might just be taking a long time for results to bubble up the query tree to us
			resetRetries = (executor.getCompletedTaskCount() > previousCompletedTaskCount);
			if (!resetRetries) {
				// we are completely blocked, try adding some emergency threads
				synchronized (HalyardEvaluationExecutor.class) {
					// check thread pool hasn't been modified already in the meantime
					if (maxPoolSize == executor.getMaximumPoolSize()) {
						if (maxPoolSize < maxThreads) {
							LOGGER.warn("All {} threads seem to be blocked - adding {} more\n{}", executor.getPoolSize(), threadGain, executor.toString());
							executor.setMaximumPoolSize(Math.min(executor.getMaximumPoolSize()+threadGain, maxThreads));
							executor.setCorePoolSize(Math.min(executor.getCorePoolSize()+threadGain, maxThreads));
							resetRetries = true;
						} else {
							// out of options
							throw new QueryEvaluationException(String.format("Maximum thread limit reached (%d)", maxThreads));
						}
					} else {
						resetRetries = true;
					}
				}
			}
		} else if (retries > RETRY_LIMIT) {
			throw new QueryEvaluationException(String.format("Maximum retries exceeded: %d", retries));
		} else {
			resetRetries = false;
		}
    	previousCompletedTaskCount = executor.getCompletedTaskCount();
		return resetRetries;
    }

	private void resetThreads() {
		if (executor.getCorePoolSize() > threads) {
			executor.setCorePoolSize(threads);
		}
		if (executor.getMaximumPoolSize() > threads) {
			executor.setMaximumPoolSize(threads);
		}
	}

	/**
     * Asynchronously pulls from an iteration of binding sets and pushes to a {@link BindingSetPipe}.
     * @param pipe the pipe that evaluation results are returned on
     * @param iter
     * @param node an implementation of any {@TupleExpr} sub-type
     */
	void pullAndPushAsync(BindingSetPipe pipe,
			CloseableIteration<BindingSet, QueryEvaluationException> iter,
			TupleExpr node, HalyardEvaluationStrategy strategy) {
		executor.execute(new IterateAndPipeTask(pipe, iter, node, strategy));
    }

    /**
     * Asynchronously pushes to a pipe using the push action, and returns an iteration of binding sets to pull from.
     * @param pushAction action to push to the pipe
     * @param node an implementation of any {@TupleExpr} sub-type
     * @return iteration of binding sets to pull from.
     */
	CloseableIteration<BindingSet, QueryEvaluationException> pushAndPull(Consumer<BindingSetPipe> pushAction, TupleExpr node, HalyardEvaluationStrategy strategy) {
        BindingSetPipeQueue queue = new BindingSetPipeQueue();
        executor.execute(new PipeAndQueueTask(queue.pipe, pushAction, node, strategy));
        return queue.iteration;
	}

	/**
     * Get the priority of this node from the PRIORITY_MAP_CACHE or determine the priority and then cache it. Also caches priority for sub-nodes of {@code node}
     * @param node the node that you want the priority for
     * @return the priority of the node.
     */
    int getPriorityForNode(final TupleExpr node) {
        Integer p = priorityMapCache.getIfPresent(node);
        if (p != null) {
            return p;
        } else {
            QueryModelNode root = node;
            while (root.getParentNode() != null) {
            	root = root.getParentNode(); //traverse to the root of the query model
            }
            // while we have a strong ref to the root node, none of the child node keys should be gc-ed

            //starting priority for ServiceRoot must be evaluated from the original service args node
            int startingPriority = root instanceof ServiceRoot ? getPriorityForNode(((ServiceRoot)root).originalServiceArgs) - 1 : 0;
            final AtomicInteger counter = new AtomicInteger(startingPriority);

            // populate the priority cache
            new AbstractQueryModelVisitor<RuntimeException>() {
                @Override
                protected void meetNode(QueryModelNode n) {
                    int pp = counter.getAndIncrement();
                    priorityMapCache.put(n, pp);
                    super.meetNode(n);
                }

                @Override
                public void meet(Filter node) {
                    super.meet(node);
                    node.getCondition().visit(this);
                }

                @Override
                public void meet(Service n) {
                    int pp = counter.getAndIncrement();
                    priorityMapCache.put(n, pp);
                    n.visitChildren(this);
                    counter.getAndUpdate((int count) -> 2 * count - pp + 1); //at least double the distance to have a space for service optimizations
                }

                @Override
                public void meet(LeftJoin node) {
                    super.meet(node);
                    if (node.hasCondition()) {
                        meetNode(node.getCondition());
                    }
                }
            }.meetOther(root);

            Integer priority = priorityMapCache.getIfPresent(node);
            if (priority == null) {
                // else node is dynamically created, so climb the tree to find an ancestor with a priority
                QueryModelNode parent = node.getParentNode();
                int depth = 1;
                while (parent != null && (priority = priorityMapCache.getIfPresent(parent)) == null) {
                    parent = parent.getParentNode();
                    depth++;
                }
                if (priority != null) {
                    priority = priority + depth;
                }
            }
            if (priority == null) {
                LOGGER.warn("Failed to ascertain a priority for node\n{}\n with root\n{}\n - using default value {}", node, root, DEFAULT_PRIORITY);
                // else fallback to a default value
                priority = DEFAULT_PRIORITY;
            }
            return priority;
        }
    }


	abstract class PrioritizedTask implements Comparable<PrioritizedTask>, Runnable {
    	static final int MIN_SUB_PRIORITY = 0;
    	static final int MAX_SUB_PRIORITY = 999;
    	final TupleExpr queryNode;
    	final int queryPriority;
    	final HalyardEvaluationStrategy strategy;

    	PrioritizedTask(TupleExpr queryNode, HalyardEvaluationStrategy strategy) {
    		this.queryNode = queryNode;
    		this.queryPriority = getPriorityForNode(queryNode);
    		this.strategy = strategy;
    	}

    	public final TupleExpr getQueryNode() {
    		return queryNode;
    	}

    	public final int getTaskPriority() {
    		return 1000*queryPriority + getSubPriority();
    	}

    	/**
    	 * Task sub-priority.
    	 * @return MIN_SUB_PRIORITY to MAX_SUB_PRIORITY inclusive
    	 */
    	protected abstract int getSubPriority();

    	@Override
		public final int compareTo(PrioritizedTask o) {
    		// descending order
			return o.getTaskPriority() - this.getTaskPriority();
		}

    	@Override
    	public String toString() {
    		return super.toString() + "[queryNode = " + queryNode.getSignature() + "[cost = " + queryNode.getCostEstimate() + ", cardinality = " + queryNode.getResultSizeEstimate() + ", count = " + queryNode.getResultSizeActual() + ", time = " + queryNode.getTotalTimeNanosActual() + "], priority = " + getTaskPriority() + ", strategy = " + strategy + "]";
    	}
    }

    /**
     * A holder for the BindingSetPipe and the iterator over a tree of query sub-parts
     */
    final class IterateAndPipeTask extends PrioritizedTask {
        private final BindingSetPipe pipe;
        private final CloseableIteration<BindingSet, QueryEvaluationException> iter;
        private final AtomicInteger pushPriority = new AtomicInteger();

        /**
         * Constructor for the class with the supplied variables
         * @param pipe The pipe to return evaluations to
         * @param iter The iterator over the evaluation tree
         */
		IterateAndPipeTask(BindingSetPipe pipe,
				CloseableIteration<BindingSet, QueryEvaluationException> iter,
				TupleExpr expr, HalyardEvaluationStrategy strategy) {
			super(expr, strategy);
            this.pipe = pipe;
            this.iter = strategy.track(iter, expr);
        }

		boolean pushNext() {
        	try {
            	if (!pipe.isClosed()) {
                	if(iter.hasNext()) {
                        BindingSet bs = iter.next();
                        if (pipe.push(bs)) { //true indicates more data is expected from this binding set, put it on the queue
                           	return true;
                        } else {
                        	iter.close();
                        	pipe.close();
                        }
                	} else {
            			iter.close();
            			pipe.close();
            		}
            	} else {
            		iter.close();
            	}
            } catch (Throwable e) {
                iter.close();
                return pipe.handleException(e);
            }
        	return false;
		}

		@Override
    	public void run() {
        	if (pushNext()) {
        		pushPriority.updateAndGet(count -> (count < MAX_SUB_PRIORITY) ? count+1 : MAX_SUB_PRIORITY);
                executor.execute(this);
        	}
    	}

		@Override
		protected int getSubPriority() {
			return pushPriority.get();
		}
    }

    final class PipeAndQueueTask extends PrioritizedTask {
        private final BindingSetPipe pipe;
        private final Consumer<BindingSetPipe> pushAction;

		PipeAndQueueTask(BindingSetPipe pipe, Consumer<BindingSetPipe> pushAction, TupleExpr expr, HalyardEvaluationStrategy strategy) {
			super(expr, strategy);
			this.pipe = pipe;
			this.pushAction = pushAction;
		}

		@Override
		public void run() {
			try {
				pushAction.accept(pipe);
			} catch(Throwable e) {
				pipe.handleException(e);
			}
		}

		@Override
		protected int getSubPriority() {
			return MIN_SUB_PRIORITY;
		}
    }

    final class BindingSetPipeQueue {

        private final LinkedBlockingQueue<BindingSet> queue = new LinkedBlockingQueue<>(maxQueueSize);
        private volatile Throwable exception;

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
    					bs = queue.poll(pollTimeoutMillis, TimeUnit.MILLISECONDS);
    					Throwable thr = exception;
    					if (thr != null) {
	    					if (thr instanceof RuntimeException) {
	    						throw (RuntimeException) thr;
	    					} else {
	                        	throw new QueryEvaluationException(thr);
	                        }
    					}

						if (bs == null) {
							if(checkThreads(retries)) {
								retries = 0;
							}
						} else {
							resetThreads();
						}
                    }
                } catch (InterruptedException ex) {
                    throw new QueryEvaluationException(ex);
                }
                return bs == END_OF_QUEUE ? null : bs;
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
            		added = queue.offer(bs, pollTimeoutMillis, TimeUnit.MILLISECONDS);

					if (!added) {
						if(checkThreads(retries)) {
							retries = 0;
						}
					} else {
						resetThreads();
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
	                addToQueue(END_OF_QUEUE);
	                isClosed = true;
            	}
            }

            @Override
            protected boolean handleException(Throwable e) {
                Throwable lastEx = exception;
                if (lastEx != null) {
                	e.addSuppressed(lastEx);
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
