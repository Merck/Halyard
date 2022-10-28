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

import java.lang.management.ManagementFactory;
import java.util.Hashtable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.LookAheadIteration;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class HalyardEvaluationExecutor implements HalyardEvaluationExecutorMXBean {
	private static final Logger LOGGER = LoggerFactory.getLogger(HalyardEvaluationExecutor.class);
    private static final BindingSet END_OF_QUEUE = new EmptyBindingSet();
    // high default priority for dynamically created query nodes
    private static final int DEFAULT_PRIORITY = 65535;

    private static volatile HalyardEvaluationExecutor instance;

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

    // a map of query model nodes and their priority
    private final Cache<QueryModelNode, Integer> priorityMapCache = CacheBuilder.newBuilder().weakKeys().build();

    private int threads;
    private int maxRetries;
    private int retryLimit;
    private int threadGain;
    private int maxThreads;

	private final TrackingThreadPoolExecutor executor;

    private int maxQueueSize;
	private int pollTimeoutMillis;

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

	public static HalyardEvaluationExecutor getInstance(Configuration conf) {
		if (instance == null) {
			synchronized (HalyardEvaluationExecutor.class) {
				if (instance == null) {
					instance = new HalyardEvaluationExecutor(conf);
				}
			}
		}
		return instance;
	}

	HalyardEvaluationExecutor(Configuration conf) {
	    threads = conf.getInt(StrategyConfig.HALYARD_EVALUATION_THREADS, 20);
	    maxRetries = conf.getInt(StrategyConfig.HALYARD_EVALUATION_MAX_RETRIES, 3);
	    retryLimit = conf.getInt(StrategyConfig.HALYARD_EVALUATION_RETRY_LIMIT, 100);
	    threadGain = conf.getInt(StrategyConfig.HALYARD_EVALUATION_THREAD_GAIN, 5);
	    maxThreads = conf.getInt(StrategyConfig.HALYARD_EVALUATION_MAX_THREADS, 100);
		executor = createExecutor("Halyard Executors", "Halyard ", threads);

	    maxQueueSize = conf.getInt(StrategyConfig.HALYARD_EVALUATION_MAX_QUEUE_SIZE, 5000);
		pollTimeoutMillis = conf.getInt(StrategyConfig.HALYARD_EVALUATION_POLL_TIMEOUT_MILLIS, 1000);

		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		try {
			registerMBeans(mbs, this);
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
	public void setRetryLimit(int limit) {
		this.retryLimit = limit;
	}

	@Override
	public int getRetryLimit() {
		return retryLimit;
	}

	@Override
	public void setMaxQueueSize(int size) {
		this.maxQueueSize = size;
	}

	@Override
	public int getMaxQueueSize() {
		return maxQueueSize;
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
				synchronized (this) {
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
		} else if (retries > retryLimit) {
			throw new QueryEvaluationException(String.format("Maximum retries exceeded: %d", retries));
		} else {
			resetRetries = false;
		}
    	previousCompletedTaskCount = executor.getCompletedTaskCount();
		return resetRetries;
    }

	private void resetThreads() {
		if (executor.getCompletedTaskCount() - previousCompletedTaskCount > 100) {
			synchronized (this) {
				int corePoolSize = executor.getCorePoolSize();
				if (corePoolSize > threads) {
					executor.setCorePoolSize(corePoolSize - 1);
				}
				int maxPoolSize = executor.getMaximumPoolSize();
				if (maxPoolSize > threads) {
					executor.setMaximumPoolSize(maxPoolSize - 1);
				}
			}
	    	previousCompletedTaskCount = executor.getCompletedTaskCount();
		}
	}

	/**
     * Asynchronously pulls from an iteration of binding sets and pushes to a {@link BindingSetPipe}.
     * @param pipe the pipe that evaluation results are returned on
     * @param iter
     * @param node an implementation of any {@TupleExpr} sub-type
     */
	void pullAndPushAsync(BindingSetPipe pipe,
			Function<BindingSet,CloseableIteration<BindingSet, QueryEvaluationException>> iterFactory,
			TupleExpr node, BindingSet bs, HalyardEvaluationStrategy strategy) {
		executor.execute(new IterateAndPipeTask(pipe, iterFactory, node, bs, strategy));
    }

    /**
     * Asynchronously pushes to a pipe using the push action, and returns an iteration of binding sets to pull from.
     * @param pushAction action to push to the pipe
     * @param node an implementation of any {@TupleExpr} sub-type
     * @return iteration of binding sets to pull from.
     */
	CloseableIteration<BindingSet, QueryEvaluationException> pushAndPull(Consumer<BindingSetPipe> pushAction, TupleExpr node, BindingSet bs, HalyardEvaluationStrategy strategy) {
        BindingSetPipeQueue queue = new BindingSetPipeQueue();
        executor.execute(new PipeAndQueueTask(queue.pipe, pushAction, node, bs, strategy));
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
    	final BindingSet bindingSet;
    	final int queryPriority;
    	final HalyardEvaluationStrategy strategy;

    	PrioritizedTask(TupleExpr queryNode, BindingSet bs, HalyardEvaluationStrategy strategy) {
    		this.queryNode = queryNode;
    		this.bindingSet = bs;
    		this.queryPriority = getPriorityForNode(queryNode);
    		this.strategy = strategy;
    	}

    	public final TupleExpr getQueryNode() {
    		return queryNode;
    	}

    	public final BindingSet getBindingSet() {
    		return bindingSet;
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
    		return super.toString() + "[queryNode = " + printQueryNode(queryNode, bindingSet) + ", priority = " + getTaskPriority() + ", strategy = " + strategy + "]";
    	}
	}

	static String printQueryNode(TupleExpr queryNode, BindingSet bs) {
		final class NodePrinter extends AbstractQueryModelVisitor<RuntimeException> {
			final StringBuilder sb = new StringBuilder(128);
			@Override
			public void meetNode(QueryModelNode node) {
				sb.append(node.getSignature());
				appendStats(node);
			}
			@Override
			public void meet(StatementPattern node) {
				sb.append(node.getSignature());
				sb.append("(");
				appendVar(node.getSubjectVar());
				sb.append(" ");
				appendVar(node.getPredicateVar());
				sb.append(" ");
				appendVar(node.getObjectVar());
				if (node.getContextVar() != null) {
					sb.append(" ");
					appendVar(node.getContextVar());
				}
				sb.append(")");
				appendStats(node);
			}
			@Override
			public void meet(Service node) {
				sb.append(node.getSignature());
				sb.append("(");
				appendVar(node.getServiceRef());
				sb.append(")");
				appendStats(node);
			}
			void appendVar(Var var) {
				if (!var.isConstant()) {
					sb.append("?").append(var.getName());
				}
				Value v = var.getValue();
				if (v == null) {
					v = bs.getValue(var.getName());
				}
				if (!var.isConstant() && v != null) {
					sb.append("=");
				}
				if (v != null) {
					sb.append(v);
				}
			}
			void appendStats(QueryModelNode node) {
				sb.append("[");
				sb.append("cost = ").append(node.getCostEstimate()).append(", ");
				sb.append("cardinality = ").append(node.getResultSizeEstimate()).append(", ");
				sb.append("count = ").append(node.getResultSizeActual()).append(", ");
				sb.append("time = ").append(node.getTotalTimeNanosActual());
				sb.append("]");
			}
			@Override
			public String toString() {
				return sb.toString();
			}
		}
		NodePrinter nodePrinter = new NodePrinter();
		queryNode.visit(nodePrinter);
		return nodePrinter.toString();
	}

	/**
     * A holder for the BindingSetPipe and the iterator over a tree of query sub-parts
     */
    final class IterateAndPipeTask extends PrioritizedTask {
        private final BindingSetPipe pipe;
        private final Function<BindingSet,CloseableIteration<BindingSet, QueryEvaluationException>> iterFactory;
        private final AtomicInteger pushPriority = new AtomicInteger();
        private CloseableIteration<BindingSet, QueryEvaluationException> iter;

        /**
         * Constructor for the class with the supplied variables
         * @param pipe The pipe to return evaluations to
         * @param iterFactory The supplier for the iterator over the evaluation tree
         */
		IterateAndPipeTask(BindingSetPipe pipe,
				Function<BindingSet,CloseableIteration<BindingSet, QueryEvaluationException>> iterFactory,
				TupleExpr expr, BindingSet bs, HalyardEvaluationStrategy strategy) {
			super(expr, bs, strategy);
            this.pipe = pipe;
            this.iterFactory = iterFactory;
        }

		boolean pushNext() {
        	try {
            	if (!pipe.isClosed()) {
            		if (iter == null) {
                        iter = strategy.track(iterFactory.apply(bindingSet), queryNode);
                        return true;
            		} else {
	                	if(iter.hasNext()) {
	                        BindingSet bs = iter.next();
	                        if (pipe.push(bs)) { //true indicates more data is expected from this binding set, put it on the queue
	                           	return true;
	                        }
	            		}
            		}
            	}
            	if (iter != null) {
            		iter.close();
            	}
            	pipe.close();
            	return false;
            } catch (Throwable e) {
            	if (iter != null) {
	            	try {
	                    iter.close();
	            	} catch (QueryEvaluationException ignore) {
	            	}
            	}
                return pipe.handleException(e);
            }
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

		PipeAndQueueTask(BindingSetPipe pipe, Consumer<BindingSetPipe> pushAction, TupleExpr expr, BindingSet bs, HalyardEvaluationStrategy strategy) {
			super(expr, bs, strategy);
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
        private volatile boolean done;
        private volatile Throwable exception;

        final BindingSetPipeIteration iteration = new BindingSetPipeIteration();
        final QueueingBindingSetPipe pipe = new QueueingBindingSetPipe();

        @Override
        public String toString() {
        	return "Queue "+Integer.toHexString(queue.hashCode())+" for pipe "+Integer.toHexString(pipe.hashCode())+" and iteration "+Integer.toHexString(iteration.hashCode());
        }

        final class BindingSetPipeIteration extends LookAheadIteration<BindingSet, QueryEvaluationException> {
        	private final long jitteredTimeout;

        	BindingSetPipeIteration() {
        		// add some jitter to avoid threads timing out at the same time (-5%, +5%)
        		jitteredTimeout = pollTimeoutMillis + (hashCode() % (pollTimeoutMillis/20));
        	}

        	 @Override
            protected BindingSet getNextElement() throws QueryEvaluationException {
    			BindingSet bs = null;
    			try {
                    for (int retries = 0; bs == null && !isClosed(); retries++) {
    					bs = queue.poll(jitteredTimeout, TimeUnit.MILLISECONDS);
    					Throwable thr = exception;
    					if (thr != null) {
	    					if (thr instanceof RuntimeException) {
	    						throw (RuntimeException) thr;
	    					} else {
	                        	throw new QueryEvaluationException(thr);
	                        }
    					}
						if (bs == null) {
							// no data available
							if(checkThreads(retries)) {
								retries = 0;
							}
						}
                    }
                } catch (InterruptedException ex) {
                    throw new QueryEvaluationException(ex);
                }
				resetThreads();
                return (bs == END_OF_QUEUE) ? null : bs;
            }

            @Override
            protected void handleClose() throws QueryEvaluationException {
                super.handleClose();
                done = true;
                try {
                	pipe.close();
                } catch (InterruptedException ignore) {
                }
            }

            @Override
            public String toString() {
            	return "Iteration "+Integer.toHexString(this.hashCode())+" for queue "+Integer.toHexString(queue.hashCode());
            }
        }

        final class QueueingBindingSetPipe extends BindingSetPipe {
            private final long jitteredTimeout;

            QueueingBindingSetPipe() {
            	super(null);
        		// add some jitter to avoid threads timing out at the same time (-5%, +5%)
        		jitteredTimeout = pollTimeoutMillis + (hashCode() % (pollTimeoutMillis/20));
            }

            private boolean addToQueue(BindingSet bs) throws InterruptedException {
            	boolean added = false;
            	for (int retries = 0; !added && !done; retries++) {
            		added = queue.offer(bs, jitteredTimeout, TimeUnit.MILLISECONDS);

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
            protected void doClose() throws InterruptedException {
        		addToQueue(END_OF_QUEUE);
        		done = true;
            }

            @Override
            protected boolean handleException(Throwable e) {
                Throwable lastEx = exception;
                if (lastEx != null) {
                	e.addSuppressed(lastEx);
                }
                exception = e;
                try {
                	close();
                } catch (InterruptedException ignore) {
                }
                return false;
            }

            @Override
            public String toString() {
            	return "Pipe "+Integer.toHexString(this.hashCode())+" for queue "+Integer.toHexString(queue.hashCode());
            }
        }
    }
}
