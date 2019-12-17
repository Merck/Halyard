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

import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.common.Timestamped;
import com.msd.gin.halyard.optimizers.HalyardEvaluationStatistics;
import com.msd.gin.halyard.sail.HBaseSail.ConnectionFactory;
import com.msd.gin.halyard.strategy.HalyardEvaluationStrategy;
import com.msd.gin.halyard.strategy.HalyardEvaluationStrategy.ServiceRoot;
import com.msd.gin.halyard.vocab.HALYARD;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.eclipse.rdf4j.IsolationLevel;
import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.ConvertingIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.ExceptionConvertingIteration;
import org.eclipse.rdf4j.common.iteration.TimeLimitIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.SPIN;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryContext;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryContextInitializer;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ExtendedEvaluationStrategy;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailConnectionQueryPreparer;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.UnknownSailTransactionStateException;
import org.eclipse.rdf4j.sail.UpdateContext;
import org.eclipse.rdf4j.sail.spin.SpinFunctionInterpreter;
import org.eclipse.rdf4j.sail.spin.SpinMagicPropertyInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseSailConnection implements SailConnection {
	private static final Logger LOG = LoggerFactory.getLogger(HBaseSailConnection.class);

	public static final String QUERY_CONTEXT_TABLE_ATTRIBUTE = Table.class.getName();

    private final HBaseSail sail;
	private Table table;
	private BufferedMutator mutator;
	private boolean pendingUpdates;
	private long lastTimestamp = Long.MIN_VALUE;
	private boolean lastUpdateWasDelete;

    public HBaseSailConnection(HBaseSail sail) {
    	this.sail = sail;
		// tables are lightweight but not thread-safe so get a new instance per sail
		// connection
    	this.table = sail.getTable();
    }

    private BufferedMutator getBufferedMutator() {
    	if (mutator == null) {
    		mutator = sail.getBufferedMutator(table);
    	}
    	return mutator;
    }

    @Override
    public boolean isOpen() throws SailException {
        return table != null;  //if the table exists the table is open
    }

    @Override
    public void close() throws SailException {
		if (mutator != null) {
			try {
				mutator.close();
			} catch (IOException e) {
				throw new SailException(e);
			}
		}

		if (table != null) {
			try {
				table.close();
				table = null;
			} catch (IOException e) {
				throw new SailException(e);
			}
		}
    }

	private TripleSource createTripleSource(long startTime) {
		return new HBaseSearchTripleSource(table, sail.getValueFactory(), startTime, sail.evaluationTimeout, sail.scanSettings, sail.elasticIndexURL, sail.ticker);
	}

    //evaluate queries/ subqueries
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, final boolean includeInferred) throws SailException {
		flush();
		LOG.debug("Evaluated TupleExpr before optimizers:\n{}", tupleExpr);
        tupleExpr = tupleExpr.clone();
        if (!(tupleExpr instanceof QueryRoot)) {
            // Add a dummy root node to the tuple expressions to allow the
            // optimizers to modify the actual root node
            tupleExpr = new QueryRoot(tupleExpr);
        }

        final long startTime = System.currentTimeMillis();
		TripleSource baseSource = createTripleSource(startTime);
        TripleSource source = new TripleSource() {
            @Override
            public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
				if (RDF.TYPE.equals(pred) && SPIN.MAGIC_PROPERTY_CLASS.equals(obj)) {
            		// cache magic property definitions here
            		return new EmptyIteration<>();
            	} else {
					return baseSource.getStatements(subj, pred, obj, contexts);
            	}
            }

            @Override
            public ValueFactory getValueFactory() {
				return baseSource.getValueFactory();
            }
        };

		SailConnectionQueryPreparer queryPreparer = new SailConnectionQueryPreparer(this, includeInferred, source);
		QueryContext queryContext = new QueryContext(queryPreparer);
		queryContext.setAttribute(QUERY_CONTEXT_TABLE_ATTRIBUTE, table);
		HalyardEvaluationStatistics stats = getStatistics();
		EvaluationStrategy strategy = sail.pushStrategy
				? new HalyardEvaluationStrategy(source, queryContext, sail.getTupleFunctionRegistry(), sail.getFunctionRegistry(), dataset, sail.getFederatedServiceResolver(), stats, sail.evaluationTimeout)
				: new ExtendedEvaluationStrategy(source, dataset, sail.getFederatedServiceResolver(), 0L, stats);

		queryContext.begin();
		try {
			initQueryContext(queryContext);

			if(!(tupleExpr instanceof ServiceRoot)) {
				// if this is a Halyard federated query then the full query has already passed through the optimizer so don't need to re-run these again
				new SpinFunctionInterpreter(sail.getSpinParser(), source, sail.getFunctionRegistry()).optimize(tupleExpr, dataset, bindings);
				if (includeInferred) {
					new SpinMagicPropertyInterpreter(sail.getSpinParser(), source, sail.getTupleFunctionRegistry(), null).optimize(tupleExpr, dataset, bindings);
				}
			}
			strategy.optimize(tupleExpr, getStatistics(), bindings);
			LOG.debug("Evaluated TupleExpr after optimization:\n{}", tupleExpr);
			try {
				// evaluate the expression against the TripleSource according to the
				// EvaluationStrategy.
				CloseableIteration<BindingSet, QueryEvaluationException> iter = evaluateInternal(strategy, tupleExpr);
				return sail.evaluationTimeout <= 0 ? iter
						: new TimeLimitIteration<BindingSet, QueryEvaluationException>(iter, TimeUnit.SECONDS.toMillis(sail.evaluationTimeout)) {
							@Override
							protected void throwInterruptedException() throws QueryEvaluationException {
								throw new QueryEvaluationException(
										"Query evaluation exceeded specified timeout " + sail.evaluationTimeout + "s");
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

    protected CloseableIteration<BindingSet, QueryEvaluationException> evaluateInternal(EvaluationStrategy strategy, TupleExpr tupleExpr) {
        return strategy.evaluate(tupleExpr, EmptyBindingSet.getInstance());
    }

    @Override
    public CloseableIteration<? extends Resource, SailException> getContextIDs() throws SailException {

        //generate an iterator over the identifiers of the contexts available in Halyard.
        final CloseableIteration<? extends Statement, SailException> scanner = getStatements(HALYARD.STATS_ROOT_NODE, SD.NAMED_GRAPH_PROPERTY, null, true, HALYARD.STATS_GRAPH_CONTEXT);
        return new ConvertingIteration<Statement, Resource, SailException>(scanner) {

			@Override
			protected Resource convert(Statement stmt) throws SailException {
				return (IRI) stmt.getObject();
			}

		};
    }

    @Override
    public CloseableIteration<? extends Statement, SailException> getStatements(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) throws SailException {
		flush();
		long startTime = System.currentTimeMillis();
		TripleSource tripleSource = createTripleSource(startTime);
		return new ExceptionConvertingIteration<Statement, SailException>(tripleSource.getStatements(subj, pred, obj, contexts)) {
			@Override
			protected SailException convert(Exception e) {
				throw new SailException(e);
			}
		};
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
        if (size == 0 && sail.evaluationTimeout > 0) try (CloseableIteration<? extends Statement, SailException> scanner = getStatements(null, null, null, true, contexts)) {
            while (scanner.hasNext()) {
                scanner.next();
                size++;
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
		if (pendingUpdates) {
			try {
				mutator.flush();
				pendingUpdates = false;
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
        if (!sail.isWritable()) throw new SailException(sail.tableName + " is read only");
    }

	protected void addStatementInternal(Resource subj, IRI pred, Value obj, Resource[] contexts, long timestamp) throws SailException {
    	checkWritable();
        try {
			for (Resource ctx : HBaseTripleSource.normalizeContexts(contexts)) {
				for (KeyValue kv : HalyardTableUtils.toKeyValues(subj, pred, obj, ctx, false, timestamp)) { // serialize the key value pairs relating to the statement in HBase
					put(kv);
				}
			}
        } catch (IOException e) {
            throw new SailException(e);
        }
    }

	protected void put(KeyValue kv) throws IOException {
		getBufferedMutator().mutate(new Put(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), kv.getTimestamp()).add(kv));
		pendingUpdates = true;
    }

    @Override
    public void removeStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
    	checkWritable();
		contexts = HBaseTripleSource.normalizeContexts(contexts);
        if (subj == null && pred == null && obj == null && contexts[0] == null) {
            clearAll();
        } else {
            try (CloseableIteration<? extends Statement, SailException> iter = getStatements(subj, pred, obj, true, contexts)) {
                while (iter.hasNext()) {
                    Statement st = iter.next();
                    removeStatement(null, st.getSubject(), st.getPredicate(), st.getObject(), st.getContext());
                }
            }
        }
    }

    @Override
    public void removeStatement(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
		// should we be defensive about nulls for subj, pre and obj? removeStatements is
		// where you would use nulls, not here.

		long timestamp = getTimestamp(op, true);
		removeStatementInternal(op, subj, pred, obj, contexts, timestamp);
    }

	protected void removeStatementInternal(UpdateContext op, Resource subj, IRI pred, Value obj, Resource[] contexts, long timestamp) throws SailException {
		checkWritable();
		try {
			for (Resource ctx : HBaseTripleSource.normalizeContexts(contexts)) {
				for (KeyValue kv : HalyardTableUtils.toKeyValues(subj, pred, obj, ctx, true, timestamp)) { // calculate the kv's corresponding to the quad (or triple)
					delete(kv);
				}
			}
		} catch (IOException e) {
			throw new SailException(e);
		}
	}

	protected void delete(KeyValue kv) throws IOException {
		getBufferedMutator().mutate(new Delete(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()).add(kv));
		pendingUpdates = true;
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

    private void clearAll() throws SailException {
    	checkWritable();
        try {
			HalyardTableUtils.truncateTable(sail.hConnection, table); // delete all triples, the whole DB but retains splits!
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
        ValueFactory vf = sail.getValueFactory();
        try {
            removeStatements(null, HALYARD.NAMESPACE_PREFIX_PROPERTY, vf.createLiteral(prefix), new Resource[] { HALYARD.SYSTEM_GRAPH_CONTEXT });
			addStatement(vf.createIRI(name), HALYARD.NAMESPACE_PREFIX_PROPERTY, vf.createLiteral(prefix), new Resource[] { HALYARD.SYSTEM_GRAPH_CONTEXT });
        } catch (SailException e) {
			LOG.warn("Namespace prefix could not be presisted due to an exception", e);
        }
    }

    @Override
    public void removeNamespace(String prefix) throws SailException {
        ValueFactory vf = sail.getValueFactory();
        try {
            removeStatements(null, HALYARD.NAMESPACE_PREFIX_PROPERTY, vf.createLiteral(prefix), new Resource[] { HALYARD.SYSTEM_GRAPH_CONTEXT });
        } catch (SailException e) {
			LOG.warn("Namespace prefix could not be removed due to an exception", e);
        }
    }

    @Override
    public void clearNamespaces() throws SailException {
        try {
            removeStatements(null, HALYARD.NAMESPACE_PREFIX_PROPERTY, null, new Resource[] { HALYARD.SYSTEM_GRAPH_CONTEXT });
        } catch (SailException e) {
			LOG.warn("Namespaces could not be cleared due to an exception", e);
        }
    }

	public static class Factory implements ConnectionFactory {
		public static final ConnectionFactory INSTANCE = new Factory();

		@Override
		public SailConnection createConnection(HBaseSail sail) {
			return new HBaseSailConnection(sail);
		}
	}
}
