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
import com.msd.gin.halyard.algebra.AbstractExtendedQueryModelVisitor;
import com.msd.gin.halyard.algebra.Algebra;
import com.msd.gin.halyard.algebra.ServiceRoot;
import com.msd.gin.halyard.query.BindingSetPipe;
import com.msd.gin.halyard.query.QueueingBindingSetPipe;
import com.msd.gin.halyard.util.RateTracker;

import java.lang.management.ManagementFactory;
import java.util.Hashtable;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HalyardEvaluationExecutor implements HalyardEvaluationExecutorMXBean {
	private static final Logger LOGGER = LoggerFactory.getLogger(HalyardEvaluationExecutor.class);
    // high default priority for dynamically created query nodes
    private static final int DEFAULT_PRIORITY = 65535;
    private static final Timer TIMER = new Timer("HalyardEvaluationExecutorTimer", true);

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
    private final Cache<TupleExpr, Integer> priorityMapCache = CacheBuilder.newBuilder().weakKeys().build();

    private final RateTracker taskRateTracker;

    private int threads;
    private int maxRetries;
    private int retryLimit;
    private int threadGain;
    private int maxThreads;

	private final TrackingThreadPoolExecutor executor;

    private int maxQueueSize;
	private int pollTimeoutMillis;
	private int offerTimeoutMillis;

	private static void registerMBeans(MBeanServer mbs, HalyardEvaluationExecutor executor) throws JMException {
		{
			Hashtable<String,String> attrs = new Hashtable<>();
			attrs.put("type", HalyardEvaluationExecutor.class.getName());
			attrs.put("id", Integer.toString(executor.hashCode()));
			mbs.registerMBean(executor, ObjectName.getInstance(StrategyConfig.JMX_DOMAIN, attrs));
		}
		{
			Hashtable<String,String> attrs = new Hashtable<>();
			attrs.put("type", TrackingThreadPoolExecutor.class.getName());
			attrs.put("id", Integer.toString(executor.executor.hashCode()));
			mbs.registerMBean(executor.executor, ObjectName.getInstance(StrategyConfig.JMX_DOMAIN, attrs));
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
	    setMaxRetries(conf.getInt(StrategyConfig.HALYARD_EVALUATION_MAX_RETRIES, 3));
	    setRetryLimit(conf.getInt(StrategyConfig.HALYARD_EVALUATION_RETRY_LIMIT, 100));
	    threadGain = conf.getInt(StrategyConfig.HALYARD_EVALUATION_THREAD_GAIN, 5);
	    maxThreads = conf.getInt(StrategyConfig.HALYARD_EVALUATION_MAX_THREADS, 100);
		executor = createExecutor("Halyard Executors", "Halyard ", threads);

	    maxQueueSize = conf.getInt(StrategyConfig.HALYARD_EVALUATION_MAX_QUEUE_SIZE, 5000);
		pollTimeoutMillis = conf.getInt(StrategyConfig.HALYARD_EVALUATION_POLL_TIMEOUT_MILLIS, 1000);
		offerTimeoutMillis = conf.getInt(StrategyConfig.HALYARD_EVALUATION_OFFER_TIMEOUT_MILLIS, conf.getInt("hbase.client.scanner.timeout.period", 60000));

		int taskRateUpdateMillis = conf.getInt(StrategyConfig.HALYARD_EVALUATION_TASK_RATE_UPDATE_MILLIS, 100);
		int taskRateWindowSize = conf.getInt(StrategyConfig.HALYARD_EVALUATION_TASK_RATE_WINDOW_SIZE, 10);
		taskRateTracker = new RateTracker(TIMER, taskRateUpdateMillis, taskRateWindowSize, () -> executor.getCompletedTaskCount());
		taskRateTracker.start();

		long threadPoolCheckPeriodSecs = conf.getInt(StrategyConfig.HALYARD_EVALUATION_THREAD_POOL_CHECK_PERIOD_SECS, 5);
		TIMER.schedule(new TimerTask() {
			@Override
			public void run() {
    			synchronized (executor) {
    				int corePoolSize = executor.getCorePoolSize();
    				if (corePoolSize > threads) {
    					corePoolSize--;
    					executor.setCorePoolSize(corePoolSize);
    				}
    				int maxPoolSize = executor.getMaximumPoolSize();
    				if (maxPoolSize > threads) {
    					maxPoolSize--;
    					executor.setMaximumPoolSize(Math.max(maxPoolSize, corePoolSize));
    				}
    			}
			}
		}, 1000L, TimeUnit.SECONDS.toMillis(threadPoolCheckPeriodSecs));

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
	public float getTaskRatePerSecond() {
		return taskRateTracker.getRatePerSecond();
	}

	@Override
	public TrackingThreadPoolExecutorMXBean getThreadPoolExecutor() {
		return executor;
	}

	/**
     * Asynchronously pulls from an iteration of binding sets and pushes to a {@link BindingSetPipe}.
     * @param pipe the pipe that evaluation results are returned on
     * @param evalStep query step to evaluate
     * @param node an implementation of any {@TupleExpr} sub-type
     * @param bs binding set
     * @param strategy
     */
	void pullAndPushAsync(BindingSetPipe pipe,
			QueryEvaluationStep evalStep,
			TupleExpr node, BindingSet bs, HalyardEvaluationStrategy strategy) {
		executor.execute(new IterateAndPipeTask(pipe, evalStep, node, bs, strategy));
    }

    /**
     * Asynchronously pushes to a pipe using the push action, and returns an iteration of binding sets to pull from.
     * @param evalStep query step to evaluate
     * @param node an implementation of any {@TupleExpr} sub-type
     * @param bs binding set
     * @param strategy
     * @return iteration of binding sets to pull from.
     */
	CloseableIteration<BindingSet, QueryEvaluationException> pushAndPull(BindingSetPipeEvaluationStep evalStep, TupleExpr node, BindingSet bs, HalyardEvaluationStrategy strategy) {
        BindingSetPipeQueue queue = new BindingSetPipeQueue();
        executor.execute(new PipeAndQueueTask(queue.pipe, evalStep, node, bs, strategy));
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
            new AbstractExtendedQueryModelVisitor<RuntimeException>() {
            	private int setPriority(TupleExpr n) {
                    int pp = counter.getAndIncrement();
                    priorityMapCache.put(n, pp);
                    return pp;
            	}

            	@Override
                protected void meetNode(QueryModelNode n) {
            		if (n instanceof TupleExpr) {
            			setPriority((TupleExpr) n);
            		}
                    n.visitChildren(this);
                }

    			@Override
    			public void meet(StatementPattern node) {
    				setPriority(node);
    				// skip children
    			}

                @Override
                public void meet(Filter node) {
                    super.meet(node);
                    node.getCondition().visit(this);
                }

                @Override
                public void meet(Service n) {
                	int pp = setPriority(n);
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
    	int taskPriority;

    	PrioritizedTask(TupleExpr queryNode, BindingSet bs, HalyardEvaluationStrategy strategy) {
    		this.queryNode = queryNode;
    		this.bindingSet = bs;
    		this.queryPriority = getPriorityForNode(queryNode);
    		this.strategy = strategy;
    		setSubPriority(MIN_SUB_PRIORITY);
    	}

    	public final TupleExpr getQueryNode() {
    		return queryNode;
    	}

    	public final BindingSet getBindingSet() {
    		return bindingSet;
    	}

    	/**
    	 * Sets this task's sub-priority.
    	 * @param subPriority MIN_SUB_PRIORITY to MAX_SUB_PRIORITY inclusive
    	 */
    	protected final void setSubPriority(int subPriority) {
    		taskPriority = 1000*queryPriority + subPriority;
    	}

    	@Override
		public final int compareTo(PrioritizedTask o) {
    		// descending order
			return o.taskPriority - this.taskPriority;
		}

    	@Override
    	public String toString() {
    		return super.toString() + "[queryNode = " + printQueryNode(queryNode, bindingSet) + ", priority = " + taskPriority + ", strategy = " + strategy + "]";
    	}
	}

	static String printQueryNode(TupleExpr queryNode, BindingSet bs) {
		final class NodePrinter extends AbstractExtendedQueryModelVisitor<RuntimeException> {
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
				Value v = Algebra.getVarValue(var, bs);
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
        private final QueryEvaluationStep evalStep;
        private int pushPriority = MIN_SUB_PRIORITY;
        private CloseableIteration<BindingSet, QueryEvaluationException> iter;

        /**
         * Constructor for the class with the supplied variables
         * @param pipe The pipe to return evaluations to
         * @param evalStep The query step to evaluation
         */
		IterateAndPipeTask(BindingSetPipe pipe,
				QueryEvaluationStep evalStep,
				TupleExpr expr, BindingSet bs, HalyardEvaluationStrategy strategy) {
			super(expr, bs, strategy);
            this.pipe = pipe;
            this.evalStep = evalStep;
        }

		boolean pushNext() {
        	try {
            	if (!pipe.isClosed()) {
            		if (iter == null) {
                        iter = strategy.track(evalStep.evaluate(bindingSet), queryNode);
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
	            		e.addSuppressed(ignore);
	            	}
            	}
                return pipe.handleException(e);
            }
		}

		@Override
    	public void run() {
        	if (pushNext()) {
        		if (pushPriority < MAX_SUB_PRIORITY) {
        			pushPriority++;
        		}
        		setSubPriority(pushPriority);
                executor.execute(this);
        	}
    	}
    }

    final class PipeAndQueueTask extends PrioritizedTask {
        private final BindingSetPipe pipe;
        private final BindingSetPipeEvaluationStep evalStep;

		PipeAndQueueTask(BindingSetPipe pipe, BindingSetPipeEvaluationStep evalStep, TupleExpr expr, BindingSet bs, HalyardEvaluationStrategy strategy) {
			super(expr, bs, strategy);
			this.pipe = pipe;
			this.evalStep = evalStep;
		}

		@Override
		public void run() {
			try {
				evalStep.evaluate(pipe, bindingSet);
			} catch(Throwable e) {
				pipe.handleException(e);
			}
		}
    }

    final class BindingSetPipeQueue {

        final BindingSetPipeIteration iteration = new BindingSetPipeIteration();
        final QueueingBindingSetPipe pipe = new QueueingBindingSetPipe(maxQueueSize, offerTimeoutMillis, TimeUnit.MILLISECONDS);

        @Override
        public String toString() {
        	return "Pipe "+Integer.toHexString(pipe.hashCode())+" for iteration "+Integer.toHexString(iteration.hashCode());
        }

        /**
         * Used by client to pull data.
         */
        final class BindingSetPipeIteration extends LookAheadIteration<BindingSet, QueryEvaluationException> {
        	@Override
            protected BindingSet getNextElement() throws QueryEvaluationException {
    			Object bs = null;
                for (int retries = 0; bs == null && !isClosed(); retries++) {
            		bs = pipe.poll(pollTimeoutMillis, TimeUnit.MILLISECONDS);
					if (bs == null) {
						// no data available - see if we can improve things
						if (checkThreads(retries)) {
							retries = 0;
						}
					}
                }
                return pipe.isEndOfQueue(bs) ? null : (BindingSet) bs;
            }

        	private boolean checkThreads(int retries) {
        		final boolean overallProgress = taskRateTracker.getRatePerSecond() > 0.0f;
        		// if not making any progress overall
        		if (!overallProgress) {
            		final int maxPoolSize = executor.getMaximumPoolSize();
	        		// if we've been consistently blocked and are at full capacity
	        		if (retries > maxRetries && executor.getActiveCount() >= maxPoolSize) {
	        			// then try adding some emergency threads
	    				synchronized (executor) {
	    					// check thread pool hasn't been modified already in the meantime and still blocked
	    					if (maxPoolSize == executor.getMaximumPoolSize() && executor.getActiveCount() == maxPoolSize && taskRateTracker.getRatePerSecond() == 0.0f) {
	    						if (maxPoolSize < maxThreads) {
	    							int newMaxPoolSize = Math.min(maxPoolSize + threadGain, maxThreads);
	    							LOGGER.warn("Iteration {}: all {} threads seem to be blocked (taskRate {}) - adding {} more\n{}", Integer.toHexString(this.hashCode()), executor.getPoolSize(), taskRateTracker.getRatePerSecond(), newMaxPoolSize - maxPoolSize, executor.toString());
	    							executor.setMaximumPoolSize(newMaxPoolSize);
	    							executor.setCorePoolSize(Math.min(executor.getCorePoolSize()+threadGain, newMaxPoolSize));
	    						} else {
	    							// out of options
	    							throw new QueryEvaluationException(String.format("Maximum thread limit reached (%d)", maxThreads));
	    						}
	    					}
	    				}
						return true;
	        		} else if (retries > retryLimit) {
	        			// something else is wrong
	        			throw new QueryEvaluationException(String.format("Retry limit exceeded: %d (active threads %d, task rate %f)", retries, executor.getActiveCount(), taskRateTracker.getRatePerSecond()));
	        		}
        		}
        		return overallProgress;
            }

            @Override
            protected void handleClose() throws QueryEvaluationException {
                super.handleClose();
               	pipe.close();
            }

            @Override
            public String toString() {
            	return "Iteration "+Integer.toHexString(this.hashCode())+" for pipe "+Integer.toHexString(pipe.hashCode());
            }
        }
    }
}
