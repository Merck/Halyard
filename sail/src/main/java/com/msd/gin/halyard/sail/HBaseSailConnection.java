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
package com.msd.gin.halyard.sail;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.msd.gin.halyard.algebra.Algebra;
import com.msd.gin.halyard.algebra.evaluation.ExtendedTripleSource;
import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.common.KeyspaceConnection;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.StatementIndices;
import com.msd.gin.halyard.common.Timestamped;
import com.msd.gin.halyard.optimizers.HalyardEvaluationStatistics;
import com.msd.gin.halyard.optimizers.TupleFunctionCallOptimizer;
import com.msd.gin.halyard.query.BindingSetPipe;
import com.msd.gin.halyard.query.BindingSetPipeQueryEvaluationStep;
import com.msd.gin.halyard.query.TimeLimitBindingSetPipe;
import com.msd.gin.halyard.sail.HBaseSail.SailConnectionFactory;
import com.msd.gin.halyard.sail.connection.SailConnectionQueryPreparer;
import com.msd.gin.halyard.sail.geosparql.WithinDistanceInterpreter;
import com.msd.gin.halyard.sail.search.SearchClient;
import com.msd.gin.halyard.sail.search.SearchInterpreter;
import com.msd.gin.halyard.spin.SpinFunctionInterpreter;
import com.msd.gin.halyard.spin.SpinMagicPropertyInterpreter;
import com.msd.gin.halyard.strategy.ExtendedEvaluationStrategy;
import com.msd.gin.halyard.strategy.ExtendedQueryOptimizerPipeline;
import com.msd.gin.halyard.strategy.HalyardEvaluationStrategy;
import com.msd.gin.halyard.vocab.HALYARD;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.ConvertingIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.ExceptionConvertingIteration;
import org.eclipse.rdf4j.common.iteration.ReducedIteration;
import org.eclipse.rdf4j.common.iteration.TimeLimitIteration;
import org.eclipse.rdf4j.common.transaction.IsolationLevel;
import org.eclipse.rdf4j.common.transaction.IsolationLevels;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.util.Literals;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryInterruptedException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryContext;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryContextInitializer;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.RDFStarTripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.QueryContextIteration;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.ParentReferenceCleaner;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.UnknownSailTransactionStateException;
import org.eclipse.rdf4j.sail.UpdateContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch.ElasticsearchClient;

public class HBaseSailConnection extends AbstractSailConnection implements BindingSetPipeSailConnection {
	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseSailConnection.class);

	public static final String SOURCE_STRING_BINDING = "__source__";
	public static final String UPDATE_PART_BINDING = "__update_part__";
	public static final String QUERY_CONTEXT_KEYSPACE_ATTRIBUTE = KeyspaceConnection.class.getName();
	public static final String QUERY_CONTEXT_INDICES_ATTRIBUTE = StatementIndices.class.getName();
	public static final String QUERY_CONTEXT_SEARCH_ATTRIBUTE = SearchClient.class.getName();
	private static final int NO_UPDATE_PARTS = -1;

	private final Cache<PreparedQueryKey, PreparedQuery> queryCache;

    private final HBaseSail sail;
	private final boolean usePush;
	private final SearchClient searchClient;
	private KeyspaceConnection keyspaceConn;
	private BufferedMutator mutator;
	private int pendingUpdateCount;
	private long lastTimestamp = Long.MIN_VALUE;
	private boolean lastUpdateWasDelete;

	public HBaseSailConnection(HBaseSail sail) throws IOException {
		this.sail = sail;
		usePush = sail.pushStrategy;
		Configuration conf = sail.getConfiguration();
		int queryCacheMaxSize = conf.getInt(EvaluationConfig.QUERY_CACHE_MAX_SIZE, 100);
		queryCache = CacheBuilder.newBuilder().concurrencyLevel(1).maximumSize(queryCacheMaxSize).expireAfterWrite(1L, TimeUnit.HOURS).build();
		// tables are lightweight but not thread-safe so get a new instance per sail
		// connection
		this.keyspaceConn = sail.keyspace.getConnection();
		if (sail.esTransport != null) {
			searchClient = new SearchClient(new ElasticsearchClient(sail.esTransport), sail.esSettings.indexName);
		} else {
			searchClient = null;
		}
    }

    private BufferedMutator getBufferedMutator() {
		if (!isOpen()) {
			throw new SailException("Connection is closed");
		}
		if (mutator == null) {
			mutator = sail.getBufferedMutator();
    	}
    	return mutator;
    }

    @Override
    public boolean isOpen() throws SailException {
		return keyspaceConn != null;
    }

    @Override
    public void close() throws SailException {
    	flush();
		if (mutator != null) {
			try {
				mutator.close();
			} catch (IOException e) {
				throw new SailException(e);
			} finally {
				mutator = null;
			}
		}

		if (keyspaceConn != null) {
			try {
				keyspaceConn.close();
			} catch (IOException e) {
				throw new SailException(e);
			} finally {
				keyspaceConn = null;
			}
		}
    }

	private HBaseTripleSource createTripleSource() {
		return new HBaseSearchTripleSource(keyspaceConn, sail.getValueFactory(), sail.getStatementIndices(), sail.evaluationTimeoutSecs, sail.getScanSettings(), searchClient, sail.ticker);
	}

	private QueryContext createQueryContext(TripleSource source, boolean includeInferred, String sourceString) {
		SailConnectionQueryPreparer queryPreparer = new SailConnectionQueryPreparer(this, includeInferred, source);
		QueryContext queryContext = new QueryContext(queryPreparer);
		queryContext.setAttribute(QUERY_CONTEXT_KEYSPACE_ATTRIBUTE, keyspaceConn);
		queryContext.setAttribute(QUERY_CONTEXT_INDICES_ATTRIBUTE, sail.getStatementIndices());
		queryContext.setAttribute(HalyardEvaluationStrategy.QUERY_CONTEXT_SOURCE_STRING_ATTRIBUTE, sourceString);
		queryContext.setAttribute(QUERY_CONTEXT_SEARCH_ATTRIBUTE, searchClient);
		return queryContext;
	}

	private EvaluationStrategy createEvaluationStrategy(TripleSource source, Dataset dataset, QueryContext queryContext) {
		EvaluationStrategy strategy;
		HalyardEvaluationStatistics stats = getStatistics();
		if (usePush) {
			strategy = new HalyardEvaluationStrategy(sail.getConfiguration(), source, queryContext, sail.getTupleFunctionRegistry(), sail.getFunctionRegistry(), dataset, sail.getFederatedServiceResolver(), stats, sail.executor);
		} else {
			strategy = new ExtendedEvaluationStrategy(source, dataset, sail.getFederatedServiceResolver(), sail.getTupleFunctionRegistry(), sail.getFunctionRegistry(), 0L, stats);
			strategy.setOptimizerPipeline(new ExtendedQueryOptimizerPipeline(strategy, source.getValueFactory(), stats));
		}
		if (trackResultSize) {
			strategy.setTrackResultSize(trackResultSize);
		}
		if (trackResultTime) {
			strategy.setTrackTime(trackResultTime);
		}
		return strategy;
	}

	private TupleExpr optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, final boolean includeInferred, TripleSource source, EvaluationStrategy strategy) {
		if (cloneTupleExpression) {
			tupleExpr = tupleExpr.clone();
		}

		// Add a dummy root node to the tuple expressions to allow the
		// optimizers to modify the actual root node
		tupleExpr = Algebra.ensureRooted(tupleExpr);

		new SpinFunctionInterpreter(sail.getSpinParser(), source, sail.getFunctionRegistry()).optimize(tupleExpr, dataset, bindings);
		if (includeInferred) {
			new SpinMagicPropertyInterpreter(sail.getSpinParser(), source, sail.getTupleFunctionRegistry(), null).optimize(tupleExpr, dataset, bindings);
		}
		new SearchInterpreter().optimize(tupleExpr, dataset, bindings);
		new WithinDistanceInterpreter().optimize(tupleExpr, dataset, bindings);
		LOGGER.trace("Query tree after interpretation:\n{}", tupleExpr);

		strategy.optimize(tupleExpr, getStatistics(), bindings);

		// required for correct evaluation of tuple functions
		new TupleFunctionCallOptimizer().optimize(tupleExpr, dataset, bindings);
		new ParentReferenceCleaner().optimize(tupleExpr, dataset, bindings);
		return tupleExpr;
	}

	private TupleExpr getOptimizedQuery(String sourceString, int updatePart, TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, final boolean includeInferred, TripleSource tripleSource, EvaluationStrategy strategy) {
		LOGGER.debug("Query tree before optimization:\n{}", tupleExpr);
		TupleExpr optimizedTree;
		if (sourceString != null && cloneTupleExpression) {
			PreparedQueryKey pqkey = new PreparedQueryKey(sourceString, updatePart, dataset, bindings, includeInferred);
			PreparedQuery preparedQuery;
			try {
				preparedQuery = queryCache.get(pqkey, () -> {
					TupleExpr optimizedTupleExpr = optimize(tupleExpr, dataset, bindings, includeInferred, tripleSource, strategy);
					return new PreparedQuery(optimizedTupleExpr);
				});
			} catch (ExecutionException e) {
				if (e.getCause() instanceof RuntimeException) {
					throw (RuntimeException) e.getCause();
				} else {
					throw new AssertionError(e);
				}
			}
			optimizedTree = preparedQuery.getTupleExpression();
			LOGGER.debug("Query tree after optimization (cached):\n{}", tupleExpr);
		} else {
			optimizedTree = optimize(tupleExpr, dataset, bindings, includeInferred, tripleSource, strategy);
			LOGGER.debug("Query tree after optimization:\n{}", tupleExpr);
		}
		return optimizedTree;
	}

	// evaluate queries/ subqueries
    @Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(final TupleExpr tupleExpr, final Dataset dataset, final BindingSet bindings, final boolean includeInferred) throws SailException {
		flush();

		String sourceString = Literals.getLabel(bindings.getValue(SOURCE_STRING_BINDING), null);
		int updatePart = Literals.getIntValue(bindings.getValue(UPDATE_PART_BINDING), NO_UPDATE_PARTS);
		BindingSet queryBindings = removeImplicitBindings(bindings);

		RDFStarTripleSource tripleSource = createTripleSource();
		QueryContext queryContext = createQueryContext(tripleSource, includeInferred, sourceString);
		EvaluationStrategy strategy = createEvaluationStrategy(tripleSource, dataset, queryContext);

		queryContext.begin();
		try {
			initQueryContext(queryContext);
			TupleExpr optimizedTree = getOptimizedQuery(sourceString, updatePart, tupleExpr, dataset, queryBindings, includeInferred, tripleSource, strategy);
			sail.trackQuery(sourceString, tupleExpr, optimizedTree);
			try {
				CloseableIteration<BindingSet, QueryEvaluationException> iter = evaluateInternal(optimizedTree, strategy);
				if (!usePush) {
					// NB: Iteration methods may do on-demand evaluation hence need to wrap these too
					iter = new QueryContextIteration(iter, queryContext);
				}
				return sail.evaluationTimeoutSecs <= 0 ? iter
						: new TimeLimitIteration<BindingSet, QueryEvaluationException>(iter, TimeUnit.SECONDS.toMillis(sail.evaluationTimeoutSecs)) {
							@Override
							protected void throwInterruptedException() {
								throw new QueryInterruptedException(
										String.format("Query evaluation exceeded specified timeout %ds", sail.evaluationTimeoutSecs));
							}
						};
			} catch (QueryEvaluationException ex) {
				throw new SailException(ex);
			}
		} finally {
			try {
				destroyQueryContext(queryContext);
			} finally {
				queryContext.end();
			}
		}
    }

	@Override
	public void evaluate(BindingSetPipe handler, final TupleExpr tupleExpr, final Dataset dataset, final BindingSet bindings, final boolean includeInferred) {
		flush();

		String sourceString = Literals.getLabel(bindings.getValue(SOURCE_STRING_BINDING), null);
		int updatePart = Literals.getIntValue(bindings.getValue(UPDATE_PART_BINDING), NO_UPDATE_PARTS);
		BindingSet queryBindings = removeImplicitBindings(bindings);

		RDFStarTripleSource tripleSource = createTripleSource();
		QueryContext queryContext = createQueryContext(tripleSource, includeInferred, sourceString);
		EvaluationStrategy strategy = createEvaluationStrategy(tripleSource, dataset, queryContext);

		queryContext.begin();
		try {
			initQueryContext(queryContext);
			TupleExpr optimizedTree = getOptimizedQuery(sourceString, updatePart, tupleExpr, dataset, queryBindings, includeInferred, tripleSource, strategy);
			sail.trackQuery(sourceString, tupleExpr, optimizedTree);
			handler = TimeLimitBindingSetPipe.apply(handler, sail.evaluationTimeoutSecs);
			try {
				evaluateInternal(handler, optimizedTree, strategy);
			} catch (QueryEvaluationException ex) {
				throw new SailException(ex);
			}
		} finally {
			try {
				destroyQueryContext(queryContext);
			} finally {
				queryContext.end();
			}
		}
	}

	private BindingSet removeImplicitBindings(BindingSet bs) {
		QueryBindingSet cleaned = new QueryBindingSet();
		for (Binding b : bs) {
			if (!(b.getName().startsWith("__") && b.getName().endsWith("__"))) {
				cleaned.addBinding(b);
			}
		}
		// canonicalise
		return (!cleaned.isEmpty()) ? cleaned : EmptyBindingSet.getInstance();
	}

	private void initQueryContext(QueryContext qctx) {
		for (QueryContextInitializer initializer : sail.getQueryContextInitializers()) {
			initializer.init(qctx);
		}
	}

	private void destroyQueryContext(QueryContext qctx) {
		for (QueryContextInitializer initializer : sail.getQueryContextInitializers()) {
			initializer.destroy(qctx);
		}
	}

	protected CloseableIteration<BindingSet, QueryEvaluationException> evaluateInternal(TupleExpr tupleExpr, EvaluationStrategy strategy) throws QueryEvaluationException {
		QueryEvaluationStep step = strategy.precompile(tupleExpr);
		return step.evaluate(EmptyBindingSet.getInstance());
	}

	protected void evaluateInternal(BindingSetPipe handler, TupleExpr tupleExpr, EvaluationStrategy strategy) throws QueryEvaluationException {
		QueryEvaluationStep step = strategy.precompile(tupleExpr);
		if (step instanceof BindingSetPipeQueryEvaluationStep) {
			((BindingSetPipeQueryEvaluationStep) step).evaluate(handler, EmptyBindingSet.getInstance());
		} else {
			BindingSetPipeSailConnection.report(step.evaluate(EmptyBindingSet.getInstance()), handler);
		}
	}

    @Override
    public CloseableIteration<? extends Resource, SailException> getContextIDs() throws SailException {

        //generate an iterator over the identifiers of the contexts available in Halyard.
		final CloseableIteration<? extends Statement, SailException> scanner = getStatements(HALYARD.STATS_ROOT_NODE, SD.NAMED_GRAPH_PROPERTY, null, true, HALYARD.STATS_GRAPH_CONTEXT);
		if (scanner.hasNext()) {
			return new ConvertingIteration<Statement, Resource, SailException>(scanner) {

				@Override
				protected Resource convert(Statement stmt) {
					return (IRI) stmt.getObject();
				}

			};
		} else {
			scanner.close();

			if (sail.evaluationTimeoutSecs > 0) {
				// try to find them manually if there are no stats and there is a specific timeout
				class StatementScanner extends AbstractStatementScanner {
					final ResultScanner rs;

					StatementScanner(RDFFactory rdfFactory) throws IOException {
						super(sail.getStatementIndices().createTableReader(sail.getValueFactory(), keyspaceConn), sail.getStatementIndices());
						rs = keyspaceConn.getScanner(sail.getStatementIndices().getCSPOIndex().scan());
					}

					@Override
					protected Result nextResult() throws IOException {
						return rs.next();
					}

					@Override
					protected void handleClose() throws IOException {
						rs.close();
					}
				}

				try {
					return new TimeLimitIteration<Resource, SailException>(
							new ReducedIteration<Resource, SailException>(new ConvertingIteration<Statement, Resource, SailException>(new ExceptionConvertingIteration<Statement, SailException>(new StatementScanner(sail.getRDFFactory())) {
								@Override
								protected SailException convert(Exception e) {
									return new SailException(e);
								}
							}) {
								@Override
								protected Resource convert(Statement stmt) {
									return stmt.getContext();
								}
							}), TimeUnit.SECONDS.toMillis(sail.evaluationTimeoutSecs)) {
						@Override
						protected void throwInterruptedException() {
							throw new SailException(String.format("Evaluation exceeded specified timeout %ds", sail.evaluationTimeoutSecs));
						}
					};
				} catch (IOException ioe) {
					throw new SailException(ioe);
				}
			} else {
				return new EmptyIteration<>();
			}
		}
    }

    @Override
    public CloseableIteration<? extends Statement, SailException> getStatements(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) throws SailException {
		flush();
		TripleSource tripleSource = createTripleSource();
		return new ExceptionConvertingIteration<Statement, SailException>(tripleSource.getStatements(subj, pred, obj, contexts)) {
			@Override
			protected SailException convert(Exception e) {
				throw new SailException(e);
			}
		};
    }

    @Override
	public boolean hasStatement(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) throws SailException {
		for (Resource ctx : contexts) {
			if (ctx != null && ctx.isTriple()) {
				return false;
			}
		}
		flush();
		ExtendedTripleSource tripleSource = createTripleSource();
		return tripleSource.hasStatement(subj, pred, obj, contexts);
	}

	@Override
    public synchronized long size(Resource... contexts) throws SailException {
        long size = 0;
        if (contexts != null && contexts.length > 0 && contexts[0] != null) {
            for (Resource ctx : contexts) {
            		//examine the VOID statistics for the count of triples in this context
                try (CloseableIteration<? extends Statement, SailException> scanner = getStatements(ctx, VOID.TRIPLES, null, true, HALYARD.STATS_GRAPH_CONTEXT)) {
                    if (scanner.hasNext()) {
                        size += ((Literal)scanner.next().getObject()).longValue();
                    }
                    if (scanner.hasNext()) {
                        throw new SailException("Multiple different values exist in VOID statistics for context: "+ctx.stringValue()+". Considering removing and recomputing statistics");
                    }
                }
            }
        } else {
            try (CloseableIteration<? extends Statement, SailException> scanner = getStatements(HALYARD.STATS_ROOT_NODE, VOID.TRIPLES, null, true, HALYARD.STATS_GRAPH_CONTEXT)) {
                if (scanner.hasNext()) {
                    size += ((Literal)scanner.next().getObject()).longValue();
                }
                if (scanner.hasNext()) {
                    throw new SailException("Multiple different values exist in VOID statistics. Considering removing and recomputing statistics");
                }
            }
        }
        // try to count it manually if there are no stats and there is a specific timeout
		if (size == 0 && sail.evaluationTimeoutSecs > 0) {
			try (CloseableIteration<? extends Statement, SailException> scanner = getStatements(null, null, null, true, contexts)) {
				while (scanner.hasNext()) {
					scanner.next();
					size++;
				}
			}
		}
        return size;
    }

    @Override
    public void begin() throws SailException { //transactions are not supported
    }

    @Override
    public void begin(IsolationLevel level) throws UnknownSailTransactionStateException, SailException {
        if (level != null && level != IsolationLevels.NONE) {
            throw new UnknownSailTransactionStateException("Isolation level " + level + " is not compatible with this HBaseSail");
        }
    }

    @Override
    public void flush() throws SailException {
		if (pendingUpdateCount > 0) {
			try {
				mutator.flush();
				pendingUpdateCount = 0;
			} catch (IOException e) {
				throw new SailException(e);
			}
		}
    }

    @Override
    public void prepare() throws SailException {
    }

    @Override
    public void commit() throws SailException {
    }

    @Override
    public void rollback() throws SailException {
    }

    @Override
    public boolean isActive() throws UnknownSailTransactionStateException {
        return true;
    }

	protected final HalyardEvaluationStatistics getStatistics() {
		return sail.statistics;
	}

	protected long getTimestamp(UpdateContext op, boolean isDelete) {
		return (op instanceof Timestamped) ? ((Timestamped) op).getTimestamp() : getDefaultTimestamp(isDelete);
    }

	/**
	 * Timestamp to use if none specified by the UpdateContext, e.g. via halyard:timestamp.
	 * 
	 * @param isDelete flag to indicate timestamp is for a delete operation
	 * @return millisecond timestamp
	 */
	protected long getDefaultTimestamp(boolean isDelete) {
		long ts = System.currentTimeMillis();
		if (ts > lastTimestamp) {
			lastTimestamp = ts;
		} else {
			if (!lastUpdateWasDelete && isDelete) {
				lastTimestamp++; // ensure delete is ordered after any previous add
			}
			ts = lastTimestamp;
		}
		lastUpdateWasDelete = isDelete;
		return ts;
    }

    @Override
    public void addStatement(Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
		addStatement(null, subj, pred, obj, contexts);
    }

    @Override
    public void addStatement(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
		long timestamp = getTimestamp(op, false);
        addStatementInternal(subj, pred, obj, contexts, timestamp);
    }

    private void checkWritable() {
		if (!isOpen()) {
			throw new IllegalStateException("Connection is closed");
		}
		if (!sail.isWritable()) {
			throw new SailException(sail.tableName + " is read only");
		}
    }

	private void addStatementInternal(Resource subj, IRI pred, Value obj, Resource[] contexts, long timestamp) throws SailException {
    	checkWritable();
		if (contexts == null || contexts.length == 0) {
			// if all contexts then insert into the default context
			contexts = new Resource[] { null };
		}
        try {
			for (Resource ctx : contexts) {
				insertStatement(subj, pred, obj, ctx, timestamp);
			}
        } catch (IOException e) {
            throw new SailException(e);
        }
    }

	private void insertStatement(Resource subj, IRI pred, Value obj, @Nullable Resource ctx, long timestamp) throws IOException {
		if (ctx != null && ctx.isTriple()) {
			throw new SailException("context argument can not be of type Triple: " + ctx);
		}
		for (KeyValue kv : HalyardTableUtils.insertKeyValues(subj, pred, obj, ctx, timestamp, sail.getStatementIndices())) {
			put(kv);
		}
	}

	private void insertSystemStatement(Resource subj, IRI pred, Value obj, Resource ctx, long timestamp) throws IOException {
		for (KeyValue kv : HalyardTableUtils.insertSystemKeyValues(subj, pred, obj, ctx, timestamp, sail.getStatementIndices())) {
			put(kv);
		}
	}

	protected void put(KeyValue kv) throws IOException {
		getBufferedMutator().mutate(new Put(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), kv.getTimestamp()).add(kv));
		pendingUpdateCount++;
	}
	
	@Override
	public void removeStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
		removeStatements(null, subj, pred, obj, contexts);
	}

	private void removeStatements(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
		checkWritable();
		if (subj == null && pred == null && obj == null && (contexts == null || contexts.length == 0)) {
			clearAllStatements();
		} else {
			long timestamp = getTimestamp(op, true);
			try (CloseableIteration<? extends Statement, SailException> iter = getStatements(subj, pred, obj, true, contexts)) {
				while (iter.hasNext()) {
					Statement st = iter.next();
					removeStatementInternal(st.getSubject(), st.getPredicate(), st.getObject(), new Resource[] { st.getContext() }, timestamp);
				}
			}
		}
	}

	@Override
	public void removeStatement(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
		if (subj != null && pred != null && obj != null && contexts != null && contexts.length > 0) {
			long timestamp = getTimestamp(op, true);
			removeStatementInternal(subj, pred, obj, contexts, timestamp);
		} else {
			removeStatements(op, subj, pred, obj, contexts);
		}
	}

	private void removeStatementInternal(Resource subj, IRI pred, Value obj, Resource[] contexts, long timestamp) throws SailException {
		checkWritable();
		try {
			for (Resource ctx : contexts) {
				deleteStatement(subj, pred, obj, ctx, timestamp);
			}
			if (subj.isTriple()) {
				removeTriple((Triple) subj, timestamp);
			}
			if (obj.isTriple()) {
				removeTriple((Triple) obj, timestamp);
			}
		} catch (IOException e) {
			throw new SailException(e);
		}
	}

	protected void removeTriple(Triple t, Long timestamp) throws IOException {
		flush();
		if (!HalyardTableUtils.isTripleReferenced(keyspaceConn, t, sail.getStatementIndices())) {
			// orphaned so safe to remove
			deleteStatement(t.getSubject(), t.getPredicate(), t.getObject(), HALYARD.TRIPLE_GRAPH_CONTEXT, timestamp);
			if (t.getSubject().isTriple()) {
				removeTriple((Triple) t.getSubject(), timestamp);
			}
			if (t.getObject().isTriple()) {
				removeTriple((Triple) t.getObject(), timestamp);
			}
		}
	}

	private void deleteStatement(Resource subj, IRI pred, Value obj, Resource ctx, long timestamp) throws IOException {
		for (KeyValue kv : HalyardTableUtils.deleteKeyValues(subj, pred, obj, ctx, timestamp, sail.getStatementIndices())) {
			delete(kv);
		}
	}

	private void deleteSystemStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws IOException {
		long timestamp = getDefaultTimestamp(true);
		try (CloseableIteration<? extends Statement, SailException> iter = getStatements(subj, pred, obj, true, contexts)) {
			while (iter.hasNext()) {
				Statement st = iter.next();
				deleteSystemStatement(st.getSubject(), st.getPredicate(), st.getObject(), st.getContext(), timestamp);
			}
		}
	}

	private void deleteSystemStatement(Resource subj, IRI pred, Value obj, Resource ctx, long timestamp) throws IOException {
		for (KeyValue kv : HalyardTableUtils.deleteSystemKeyValues(subj, pred, obj, ctx, timestamp, sail.getStatementIndices())) {
			delete(kv);
		}
	}

	protected void delete(KeyValue kv) throws IOException {
		getBufferedMutator().mutate(new Delete(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()).add(kv));
		pendingUpdateCount++;
	}

    @Override
    public boolean pendingRemovals() {
        return false;
    }

    @Override
    public void startUpdate(UpdateContext op) throws SailException {
    }

    @Override
    public void endUpdate(UpdateContext op) throws SailException {
    }

    @Override
    public void clear(Resource... contexts) throws SailException {
        removeStatements(null, null, null, contexts); //remove all statements in the contexts.
    }

	private void clearAllStatements() throws SailException {
        try {
			HalyardTableUtils.clearTriples(sail.hConnection, sail.tableName);
        } catch (IOException ex) {
            throw new SailException(ex);
        }
    }

    @Override
    public String getNamespace(String prefix) throws SailException {
        ValueFactory vf = sail.getValueFactory();
    	try (CloseableIteration<? extends Statement, SailException> nsIter = getStatements(null, HALYARD.NAMESPACE_PREFIX_PROPERTY, vf.createLiteral(prefix), false, new Resource[] { HALYARD.SYSTEM_GRAPH_CONTEXT })) {
    		if (nsIter.hasNext()) {
    			IRI namespace = (IRI) nsIter.next().getSubject();
    			return namespace.stringValue();
    		} else {
    			return null;
    		}
    	}
    }

    @Override
    public CloseableIteration<? extends Namespace, SailException> getNamespaces() {
    	CloseableIteration<? extends Statement, SailException> nsIter = getStatements(null, HALYARD.NAMESPACE_PREFIX_PROPERTY, null, false, new Resource[] { HALYARD.SYSTEM_GRAPH_CONTEXT });
    	return new ConvertingIteration<Statement, Namespace, SailException>(nsIter) {
			@Override
			protected Namespace convert(Statement stmt)
				throws SailException {
                String name = stmt.getSubject().stringValue();
                String prefix = stmt.getObject().stringValue();
				return new SimpleNamespace(prefix, name);
			}
    	};
    }

    @Override
    public void setNamespace(String prefix, String name) throws SailException {
		checkWritable();
        ValueFactory vf = sail.getValueFactory();
        try {
			deleteSystemStatements(null, HALYARD.NAMESPACE_PREFIX_PROPERTY, vf.createLiteral(prefix), new Resource[] { HALYARD.SYSTEM_GRAPH_CONTEXT });
			long timestamp = getDefaultTimestamp(false);
			insertSystemStatement(vf.createIRI(name), HALYARD.NAMESPACE_PREFIX_PROPERTY, vf.createLiteral(prefix), HALYARD.SYSTEM_GRAPH_CONTEXT, timestamp);
        } catch (IOException e) {
			throw new SailException("Namespace prefix could not be presisted due to an exception", e);
        }
    }

    @Override
    public void removeNamespace(String prefix) throws SailException {
		checkWritable();
        ValueFactory vf = sail.getValueFactory();
        try {
			deleteSystemStatements(null, HALYARD.NAMESPACE_PREFIX_PROPERTY, vf.createLiteral(prefix), new Resource[] { HALYARD.SYSTEM_GRAPH_CONTEXT });
        } catch (IOException e) {
        	throw new SailException("Namespace prefix could not be removed due to an exception", e);
        }
    }

    @Override
    public void clearNamespaces() throws SailException {
		checkWritable();
        try {
			deleteSystemStatements(null, HALYARD.NAMESPACE_PREFIX_PROPERTY, null, new Resource[] { HALYARD.SYSTEM_GRAPH_CONTEXT });
        } catch (IOException e) {
        	throw new SailException("Namespaces could not be cleared due to an exception", e);
        }
    }

	private static final class PreparedQueryKey implements Serializable {
		private static final long serialVersionUID = -8673870599435959092L;

		final String sourceString;
		final Integer updatePart;
		final Set<IRI> datasetGraphs;
		final Set<IRI> datasetNamedGraphs;
		final IRI datasetInsertGraph;
		final Set<IRI> datasetRemoveGraphs;
		final Set<String> bindingNames;
		final boolean includeInferred;

		static <E> Set<E> copy(Set<E> set) {
			switch (set.size()) {
				case 0:
					return Collections.emptySet();
				case 1:
					return Collections.singleton(set.iterator().next());
				default:
					return new HashSet<>(set);
			}
		}

		PreparedQueryKey(String sourceString, int updatePart, Dataset dataset, BindingSet bindings, boolean includeInferred) {
			this.sourceString = sourceString;
			this.updatePart = Integer.valueOf(updatePart);
			this.datasetGraphs = dataset != null ? copy(dataset.getDefaultGraphs()) : null;
			this.datasetNamedGraphs = dataset != null ? copy(dataset.getNamedGraphs()) : null;
			this.datasetInsertGraph = dataset != null ? dataset.getDefaultInsertGraph() : null;
			this.datasetRemoveGraphs = dataset != null ? copy(dataset.getDefaultRemoveGraphs()) : null;
			this.bindingNames = copy(bindings.getBindingNames());
			this.includeInferred = includeInferred;
		}

		private Object[] toArray() {
			return new Object[] { sourceString, updatePart, bindingNames, includeInferred, datasetGraphs, datasetNamedGraphs, datasetInsertGraph, datasetRemoveGraphs };
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof PreparedQueryKey)) {
				return false;
			}
			PreparedQueryKey other = (PreparedQueryKey) o;
			return Arrays.equals(this.toArray(), other.toArray());
		}

		@Override
		public int hashCode() {
			return Objects.hash(toArray());
		}
	}

	protected static final class PreparedQuery {
		private final TupleExpr tree; // our golden copy

		public PreparedQuery(TupleExpr tree) {
			this.tree = tree;
		}

		/**
		 * Returns a copy of the query tree for execution.
		 */
		public TupleExpr getTupleExpression() {
			return tree.clone();
		}
	}

	static final class Factory implements SailConnectionFactory {
		static final SailConnectionFactory INSTANCE = new Factory();

		@Override
		public HBaseSailConnection createConnection(HBaseSail sail) throws IOException {
			return new HBaseSailConnection(sail);
		}
	}
}
