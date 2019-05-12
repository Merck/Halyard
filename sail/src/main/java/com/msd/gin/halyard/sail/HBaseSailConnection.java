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
import com.msd.gin.halyard.common.RDFContext;
import com.msd.gin.halyard.common.RDFObject;
import com.msd.gin.halyard.common.RDFPredicate;
import com.msd.gin.halyard.common.RDFSubject;
import com.msd.gin.halyard.common.Timestamped;
import com.msd.gin.halyard.optimizers.HalyardEvaluationStatistics;
import com.msd.gin.halyard.optimizers.HalyardFilterOptimizer;
import com.msd.gin.halyard.optimizers.HalyardQueryJoinOptimizer;
import com.msd.gin.halyard.sail.HBaseSail.ConnectionFactory;
import com.msd.gin.halyard.strategy.HalyardEvaluationStrategy;
import com.msd.gin.halyard.strategy.HalyardEvaluationStrategy.ServiceRoot;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.eclipse.rdf4j.IsolationLevel;
import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.CloseableIteratorIteration;
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
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.IncompatibleOperationException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryContext;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.BindingAssigner;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.CompareOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ConstantOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.DisjunctiveConstraintOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ExtendedEvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.IterativeEvaluationOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.OrderLimitOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryModelNormalizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.SameTermFilterOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailConnectionQueryPreparer;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.UnknownSailTransactionStateException;
import org.eclipse.rdf4j.sail.UpdateContext;
import org.eclipse.rdf4j.sail.spin.SpinFunctionInterpreter;
import org.eclipse.rdf4j.sail.spin.SpinMagicPropertyInterpreter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseSailConnection implements SailConnection {
	private static final Logger LOG = LoggerFactory.getLogger(HBaseSailConnection.class);

    private static final int ELASTIC_RESULT_SIZE = 10000;

    private final HBaseSail sail;
	private Table table;
	private BufferedMutator mutator;

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
    	commit();
		if (table != null) {
			try {
				table.close();
				table = null;
			} catch (IOException e) {
				throw new SailException(e);
			}
		}
    }

    //evaluate queries/ subqueries
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, final boolean includeInferred) throws SailException {
		LOG.debug("Evaluated TupleExpr before optimizers:\n{}", tupleExpr);
        tupleExpr = tupleExpr.clone();
        if (!(tupleExpr instanceof QueryRoot)) {
            // Add a dummy root node to the tuple expressions to allow the
            // optimizers to modify the actual root node
            tupleExpr = new QueryRoot(tupleExpr);
        }

        final long startTime = System.currentTimeMillis();

        TripleSource source = new TripleSource() {
            @Override
            public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
            	if (RDF.TYPE.equals(pred) && SPIN.MAGIC_PROPERTIES_CLASS.equals(obj)) {
            		// cache magic property definitions here
            		return new EmptyIteration<>();
            	} else {
	                try {
	                    return new ExceptionConvertingIteration<Statement, QueryEvaluationException>(createScanner(startTime, subj, pred, obj, contexts)) {
	                        @Override
	                        protected QueryEvaluationException convert(Exception e) {
	                            return new QueryEvaluationException(e);
	                        }
	
	                    };
	                } catch (SailException ex) {
	                    throw new QueryEvaluationException(ex);
	                }
            	}
            }

            @Override
            public ValueFactory getValueFactory() {
                return sail.getValueFactory();
            }
        };

		SailConnectionQueryPreparer queryPreparer = new SailConnectionQueryPreparer(this, includeInferred, source);
		QueryContext queryContext = new QueryContext(queryPreparer);
		EvaluationStrategy strategy = sail.pushStrategy ? new HalyardEvaluationStrategy(source, queryContext, sail.tupleFunctionRegistry, sail.functionRegistry, dataset,
				sail, sail.evaluationTimeout)
				: new ExtendedEvaluationStrategy(source, dataset, sail, 0L);

		queryContext.begin();
		try {
			if(!(tupleExpr instanceof ServiceRoot)) {
				// if this is a Halyard federated query then the full query has already passed through the optimizer so don't need to re-run these again
				new SpinFunctionInterpreter(sail.spinParser, source, sail.functionRegistry).optimize(tupleExpr, dataset, bindings);
				new SpinMagicPropertyInterpreter(sail.spinParser, source, sail.tupleFunctionRegistry, null).optimize(tupleExpr, dataset, bindings);
			}
			new BindingAssigner().optimize(tupleExpr, dataset, bindings);
			new ConstantOptimizer(strategy).optimize(tupleExpr, dataset, bindings);
			new CompareOptimizer().optimize(tupleExpr, dataset, bindings);
			new ConjunctiveConstraintSplitter().optimize(tupleExpr, dataset, bindings);
			new DisjunctiveConstraintOptimizer().optimize(tupleExpr, dataset, bindings);
			new SameTermFilterOptimizer().optimize(tupleExpr, dataset, bindings);
			new QueryModelNormalizer().optimize(tupleExpr, dataset, bindings);
			try {
				// seach for presence of HALYARD.SEARCH_TYPE or HALYARD.PARALLEL_SPLIT_FUNCTION
				// within the query
				tupleExpr.visit(new AbstractQueryModelVisitor<IncompatibleOperationException>() {
					private void checkForSearchType(Value val) {
						if (HALYARD.SEARCH_TYPE.equals(val) || ((val instanceof Literal)
								&& HALYARD.SEARCH_TYPE.equals(((Literal) val).getDatatype()))) {
							throw new IncompatibleOperationException();
						}
					}

					@Override
					public void meet(ValueConstant node) throws RuntimeException {
						checkForSearchType(node.getValue());
						super.meet(node);
					}

					@Override
					public void meet(Var node) throws RuntimeException {
						if (node.hasValue()) {
							checkForSearchType(node.getValue());
						}
						super.meet(node);
					}

					@Override
					public void meet(FunctionCall node) throws IncompatibleOperationException {
						if (HALYARD.PARALLEL_SPLIT_FUNCTION.toString().equals(node.getURI())) {
							throw new IncompatibleOperationException();
						}
						super.meet(node);
					}

					@Override
					public void meet(BindingSetAssignment node) throws RuntimeException {
						for (BindingSet bs : node.getBindingSets()) {
							for (Binding b : bs) {
								checkForSearchType(b.getValue());
							}
						}
						super.meet(node);
					}
				});
				new HalyardQueryJoinOptimizer(sail.statistics).optimize(tupleExpr, dataset, bindings);
			} catch (IncompatibleOperationException ioe) {
				// skip HalyardQueryJoinOptimizer when HALYARD.PARALLEL_SPLIT_FUNCTION or
				// HALYARD.SEARCH_TYPE is present
			}
			// new SubSelectJoinOptimizer().optimize(tupleExpr, dataset, bindings);
			new IterativeEvaluationOptimizer().optimize(tupleExpr, dataset, bindings);
			new HalyardFilterOptimizer().optimize(tupleExpr, dataset, bindings); // apply filter optimizer twice (before
			new OrderLimitOptimizer().optimize(tupleExpr, dataset, bindings);
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
			queryContext.end();
		}
    }

    protected CloseableIteration<BindingSet, QueryEvaluationException> evaluateInternal(EvaluationStrategy strategy, TupleExpr tupleExpr) {
        return strategy.evaluate(tupleExpr, EmptyBindingSet.getInstance());
    }

    @Override
    public CloseableIteration<? extends Resource, SailException> getContextIDs() throws SailException {

        //generate an iterator over the identifiers of the contexts available in Halyard.
        final CloseableIteration<? extends Statement, SailException> scanner = getStatements(HALYARD.STATS_ROOT_NODE, SD.NAMED_GRAPH_PROPERTY, null, true, HALYARD.STATS_GRAPH_CONTEXT);
        return new CloseableIteration<Resource, SailException>() {
            @Override
            public void close() throws SailException {
                scanner.close();
            }

            @Override
            public boolean hasNext() throws SailException {
                return scanner.hasNext();
            }

            @Override
            public Resource next() throws SailException {
                return (IRI)scanner.next().getObject();
            }

            @Override
            public void remove() throws SailException {
                throw new UnsupportedOperationException();
            }

        };
    }

    @Override
    public CloseableIteration<? extends Statement, SailException> getStatements(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) throws SailException {
        return createScanner(System.currentTimeMillis(), subj, pred, obj, contexts);
    }

    private StatementScanner createScanner(long startTime, Resource subj, IRI pred, Value obj, Resource...contexts) throws SailException {
        if ((obj instanceof Literal) && (HALYARD.SEARCH_TYPE.equals(((Literal)obj).getDatatype()))) {
            return new LiteralSearchStatementScanner(startTime, subj, pred, obj.stringValue(), contexts);
        } else {
            return new StatementScanner(startTime, subj, pred, obj, contexts);
        }
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
    }

    @Override
    public void prepare() throws SailException {
    }

    @Override
    public void commit() throws SailException {
    	if (mutator != null) {
	    	try {
				mutator.flush();
			} catch (IOException e) {
				throw new SailException(e);
			}
    	}
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

    protected long getTimeStamp(UpdateContext op) {
        return (op instanceof Timestamped) ? ((Timestamped)op).getTimestamp() : getDefaultTimeStamp();
    }

    protected long getDefaultTimeStamp() {
    	return System.currentTimeMillis();
    }

    @Override
    public void addStatement(Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
		addStatement(null, subj, pred, obj, contexts);
    }

    @Override
    public void addStatement(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
        long timestamp = getTimeStamp(op);
        addStatementInternal(subj, pred, obj, contexts, timestamp);
    }

    private void checkWritable() {
        if (!sail.isWritable()) throw new SailException(sail.tableName + " is read only");
    }

	protected void addStatementInternal(Resource subj, IRI pred, Value obj, Resource[] contexts, long timestamp) throws SailException {
    	checkWritable();
        try {
			for (Resource ctx : normalizeContexts(contexts)) {
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
    }

    @Override
    public void removeStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
    	checkWritable();
        contexts = normalizeContexts(contexts);
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

        long timestamp = getTimeStamp(op);
		removeStatementInternal(op, subj, pred, obj, contexts, timestamp);
    }

	protected void removeStatementInternal(UpdateContext op, Resource subj, IRI pred, Value obj, Resource[] contexts, long timestamp) throws SailException {
		checkWritable();
		try {
			for (Resource ctx : normalizeContexts(contexts)) {
				for (KeyValue kv : HalyardTableUtils.toKeyValues(subj, pred, obj, ctx, true, timestamp)) { // calculate the kv's corresponding to the quad (or triple)
					delete(kv);
				}
			}
		} catch (IOException e) {
			throw new SailException(e);
		}
	}

	protected void delete(KeyValue kv) throws IOException {
		getBufferedMutator().mutate(new Delete(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()).addDeleteMarker(kv));
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
        commit();
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
        Namespace namespace = sail.namespaces.get(prefix);
        return (namespace == null) ? null : namespace.getName();
    }

    @Override
    public CloseableIteration<? extends Namespace, SailException> getNamespaces() {
        return new CloseableIteratorIteration<>(sail.namespaces.values().iterator());
    }

    @Override
    public void setNamespace(String prefix, String name) throws SailException {
        sail.namespaces.put(prefix, new SimpleNamespace(prefix, name));
        ValueFactory vf = sail.getValueFactory();
        try {
            removeStatements(null, HALYARD.NAMESPACE_PREFIX_PROPERTY, vf.createLiteral(prefix));
			addStatement(vf.createIRI(name), HALYARD.NAMESPACE_PREFIX_PROPERTY, vf.createLiteral(prefix), new Resource[] { HALYARD.SYSTEM_GRAPH_CONTEXT });
        } catch (SailException e) {
			LOG.warn("Namespace prefix could not be presisted due to an exception", e);
        }
    }

    @Override
    public void removeNamespace(String prefix) throws SailException {
        ValueFactory vf = sail.getValueFactory();
        sail.namespaces.remove(prefix);
        try {
            removeStatements(null, HALYARD.NAMESPACE_PREFIX_PROPERTY, vf.createLiteral(prefix));
        } catch (SailException e) {
			LOG.warn("Namespace prefix could not be removed due to an exception", e);
        }
    }

    @Override
    public void clearNamespaces() throws SailException {
        try {
            removeStatements(null, HALYARD.NAMESPACE_PREFIX_PROPERTY, null);
        } catch (SailException e) {
			LOG.warn("Namespaces could not be cleared due to an exception", e);
        }
        sail.namespaces.clear();
    }

	private static final Map<String, List<RDFObject>> SEARCH_CACHE = new WeakHashMap<>();

    //Scans the Halyard table for statements that match the specified pattern
    private class LiteralSearchStatementScanner extends StatementScanner {

		final ValueFactory vf = sail.getValueFactory();
		Iterator<RDFObject> objectHashes = null;
        private final String literalSearchQuery;

        public LiteralSearchStatementScanner(long startTime, Resource subj, IRI pred, String literalSearchQuery, Resource... contexts) throws SailException {
            super(startTime, subj, pred, null, contexts);
            if (sail.elasticIndexURL == null || sail.elasticIndexURL.length() == 0) {
                throw new SailException("ElasticSearch Index URL is not properly configured.");
            }
            this.literalSearchQuery = literalSearchQuery;
        }

        @Override
        protected Result nextResult() throws IOException {
            while (true) {
                if (obj == null) {
                    if (objectHashes == null) { //perform ES query and parse results
                        synchronized (SEARCH_CACHE) {
							List<RDFObject> objectHashesList = SEARCH_CACHE.get(literalSearchQuery);
                            if (objectHashesList == null) {
                                objectHashesList = new ArrayList<>();
                                HttpURLConnection http = (HttpURLConnection)(new URL(sail.elasticIndexURL + "/_search").openConnection());
                                try {
                                    http.setRequestMethod("POST");
                                    http.setDoOutput(true);
                                    http.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
                                    http.connect();
                                    try (PrintStream out = new PrintStream(http.getOutputStream(), true, "UTF-8")) {
										out.print("{\"query\":{\"query_string\":{\"query\":" + JSONObject.quote(literalSearchQuery) + "}},\"size\":" + ELASTIC_RESULT_SIZE + "}");
                                    }
                                    int response = http.getResponseCode();
                                    String msg = http.getResponseMessage();
                                    if (response != 200) {
                                        LOG.info("ElasticSearch error response: " + msg + " on query: " + literalSearchQuery);
                                    } else {
                                        try (InputStreamReader isr = new InputStreamReader(http.getInputStream(), "UTF-8")) {
                                            JSONArray hits = new JSONObject(new JSONTokener(isr)).getJSONObject("hits").getJSONArray("hits");
                                            for (int i=0; i<hits.length(); i++) {
												JSONObject source = hits.getJSONObject(i).getJSONObject("_source");
												if(source.has("lang")) {
													objectHashesList.add(RDFObject.create(vf.createLiteral(source.getString("label"), source.getString("lang"))));
												} else {
													objectHashesList.add(RDFObject.create(vf.createLiteral(source.getString("label"), vf.createIRI(source.getString("datatype")))));
												}
                                            }
                                        }
                                        SEARCH_CACHE.put(new String(literalSearchQuery), objectHashesList);
                                    }
								} catch (JSONException ex) {
                                    throw new IOException(ex);
                                } finally {
                                    http.disconnect();
                                }
                            }
                            objectHashes = objectHashesList.iterator();
                        }
                    }
                    if (objectHashes.hasNext()) {
                        obj = objectHashes.next();
                    } else {
                        return null;
                    }
                    contexts = contextsList.iterator(); //reset iterator over contexts
                }
                Result res = super.nextResult();
                if (res == null) {
                    obj = null;
                } else {
                    return res;
                }
            }
        }
    }

	private static final int TIMEOUT_POLL_MILLIS = 1000;

	private class StatementScanner implements CloseableIteration<Statement, SailException> {

        private final RDFSubject subj;
        private final RDFPredicate pred;
		protected RDFObject obj;
		private RDFContext ctx;
        protected final List<Resource> contextsList;
        protected Iterator<Resource> contexts;
        private ResultScanner rs = null;
        private final long endTime;
        private Statement next = null;
        private Iterator<Statement> iter = null;
		private int counter = 0;
		private int timeoutCount = 1;
		private long counterStartTime;

        public StatementScanner(long startTime, Resource subj, IRI pred, Value obj, Resource...contexts) throws SailException {
            this.subj = RDFSubject.create(subj);
            this.pred = RDFPredicate.create(pred);
            this.obj = RDFObject.create(obj);
            this.contextsList = Arrays.asList(normalizeContexts(contexts));
            this.contexts = contextsList.iterator();
			this.endTime = startTime + TimeUnit.SECONDS.toMillis(sail.evaluationTimeout);
			this.counterStartTime = startTime;
			LOG.trace("New StatementScanner {} {} {} {}", subj, pred, obj, contextsList);
        }

        protected Result nextResult() throws IOException { //gets the next result to consider from the HBase Scan
            while (true) {
                if (rs == null) {
                    if (contexts.hasNext()) {

                        //build a ResultScanner from an HBase Scan that finds potential matches
                    	ctx = RDFContext.create(contexts.next());
                    	Scan scan = HalyardTableUtils.scan(subj, pred, obj, ctx);
						scan.setTimeRange(sail.minTimestamp, sail.maxTimestamp);
						scan.setMaxVersions(sail.maxVersions);
                        rs = table.getScanner(scan);
                    } else {
                        return null;
                    }
                }
                Result res = rs.next();
                if (sail.ticker != null) sail.ticker.tick(); //sends a tick for keep alive purposes
                if (res == null) { // no more results from this ResultScanner, close and clean up.
                    rs.close();
                    rs = null;
                } else {
                    return res;
                }
            }
        }

        @Override
        public void close() throws SailException {
            if (rs != null) {
                rs.close();
            }
        }

        @Override
        public synchronized boolean hasNext() throws SailException {
			if (sail.evaluationTimeout > 0) {
				// avoid the cost of calling System.currentTimeMillis() too often
				counter++;
				if (counter >= timeoutCount) {
					long time = System.currentTimeMillis();
					if (time > endTime) {
						throw new SailException("Statements scanning exceeded specified timeout " + sail.evaluationTimeout + "s");
					}
					int elapsed = (int) (time - counterStartTime);
					if (elapsed > 0) {
						timeoutCount = (timeoutCount * TIMEOUT_POLL_MILLIS) / elapsed;
						if (timeoutCount == 0) {
							timeoutCount = 1;
						}
					} else {
						timeoutCount *= 2;
					}
					counter = 0;
					counterStartTime = time;
				}
			}

			if (next == null) {
				try { // try and find the next result
					while (true) {
						if (iter == null) {
							Result res = nextResult();
							if (res == null) {
								return false; // no more Results
							} else {
								iter = HalyardTableUtils.parseStatements(subj, pred, obj, ctx, res, sail.getValueFactory()).iterator();
							}
						}
						while (iter.hasNext()) {
							Statement s = iter.next();
							next = s; // cache the next statement which will be returned with a call to next().
							return true; // there is another statement
						}
						iter = null;
					}
				} catch (IOException e) {
					throw new SailException(e);
				}
			} else {
				return true;
			}
        }

        @Override
        public synchronized Statement next() throws SailException {
            if (hasNext()) { //return the next statement and set next to null so it can be refilled by the next call to hasNext()
                Statement st = next;
                next = null;
                return st;
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() throws SailException {
            throw new UnsupportedOperationException();
        }
    }

    //generates a Resource[] from 0 or more Resources
    protected static Resource[] normalizeContexts(Resource... contexts) {
        if (contexts == null || contexts.length == 0) {
            return new Resource[] {null};
        } else {
            return contexts;
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
