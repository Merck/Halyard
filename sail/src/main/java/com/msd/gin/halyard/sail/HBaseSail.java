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
import com.msd.gin.halyard.strategy.HalyardEvaluationStrategy;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.eclipse.rdf4j.IsolationLevel;
import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.CloseableIteratorIteration;
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
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.IncompatibleOperationException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.BindingAssigner;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.CompareOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ConstantOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.DisjunctiveConstraintOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.FilterOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.IterativeEvaluationOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.OrderLimitOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryJoinOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryModelNormalizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.SameTermFilterOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.UnknownSailTransactionStateException;
import org.eclipse.rdf4j.sail.UpdateContext;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * HBaseSail is the RDF Storage And Inference Layer (SAIL) implementation on top of Apache HBase.
 * It implements the interfaces - {@code Sail, SailConnection} and {@code FederatedServiceResolver}. Currently federated queries are
 * only supported for queries across multiple graphs in one Halyard database.
 * @author Adam Sotona (MSD)
 */
public class HBaseSail implements Sail, SailConnection, FederatedServiceResolver, FederatedService {

    /**
     * Ticker is a simple service interface that is notified when some data are processed.
     * It's purpose is to notify a caller (for example MapReduce task) that the execution is still alive.
     */
    public interface Ticker {

        /**
         * This method is called whenever a new Statement is populated from HBase.
         */
        public void tick();
    }

    private static final Logger LOG = Logger.getLogger(HBaseSail.class.getName());
    private static final long STATUS_CACHING_TIMEOUT = 60000l;
    private static final int ELASTIC_RESULT_SIZE = 10000;

    private final Configuration config; //the configuration of the HBase database
    final String tableName;
    final boolean create;
    final boolean pushStrategy;
    final int splitBits;
    protected final HalyardEvaluationStatistics statistics;
    final int evaluationTimeout;
    private boolean readOnly = false;
    private long readOnlyTimestamp = -1;
    final String elasticIndexURL;
    private final Ticker ticker;

    HTable table = null;

    private final Map<String, Namespace> namespaces = new HashMap<>();
    private final Map<String, HBaseSail> federatedServices = new HashMap<>();

    /**
     * Construct HBaseSail object with given arguments.
     * @param config Hadoop Configuration to access HBase
     * @param tableName HBase table name used to store data
     * @param create boolean option to create the table if it does not exist
     * @param splitBits int number of bits used for the calculation of HTable region pre-splits (applies for new tables only)
     * @param pushStrategy boolean option to use {@link com.msd.gin.halyard.strategy.HalyardEvaluationStrategy} instead of {@link org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategy}
     * @param evaluationTimeout int timeout in seconds for each query evaluation, negative values mean no timeout
     * @param elasticIndexURL String optional ElasticSearch index URL
     * @param ticker optional Ticker callback for keep-alive notifications
     */
    public HBaseSail(Configuration config, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, String elasticIndexURL, Ticker ticker) {
        this.config = config;
        this.tableName = tableName;
        this.create = create;
        this.splitBits = splitBits;
        this.pushStrategy = pushStrategy;
        this.statistics = new HalyardEvaluationStatistics(this);
        this.evaluationTimeout = evaluationTimeout;
        this.elasticIndexURL = elasticIndexURL;
        this.ticker = ticker;
    }

    /**
     * Not used in Halyard
     */
    @Override
    public void setDataDir(File dataDir) {
    }

    /**
     * Not used in Halyard
     */
    @Override
    public File getDataDir() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void initialize() throws SailException { //initialize the SAIL
        try {
        	    //get or create and get the HBase table
            table = HalyardTableUtils.getTable(config, tableName, create, splitBits);

            //Iterate over statements relating to namespaces and add them to the namespace map.
            try (CloseableIteration<? extends Statement, SailException> nsIter = getStatements(null, HALYARD.NAMESPACE_PREFIX_PROPERTY, null, true)) {
                while (nsIter.hasNext()) {
                    Statement st = nsIter.next();
                    if (st.getObject() instanceof Literal) {
                        String prefix = st.getObject().stringValue();
                        String name = st.getSubject().stringValue();
                        namespaces.put(prefix, new SimpleNamespace(prefix, name));
                    }
                }
            }
        } catch (IOException ex) {
            throw new SailException(ex);
        }
    }

    @Override
    public FederatedService getService(String serviceUrl) throws QueryEvaluationException {
        //provide a service to query over Halyard graphs. Remote service queries are not supported.
        if (serviceUrl.startsWith(HALYARD.NAMESPACE)) {
            String federatedTable = serviceUrl.substring(HALYARD.NAMESPACE.length());
            HBaseSail sail = federatedServices.get(federatedTable);
            if (sail == null) {
                sail = new HBaseSail(config, federatedTable, false, 0, true, evaluationTimeout, null, ticker);
                federatedServices.put(federatedTable, sail);
                sail.initialize();
            }
            return sail;
        } else {
            throw new QueryEvaluationException("Unsupported service URL: " + serviceUrl);
        }
    }

    @Override
    public boolean ask(Service service, BindingSet bindings, String baseUri) throws QueryEvaluationException {
        try (CloseableIteration<BindingSet, QueryEvaluationException> res = evaluate(service.getArg(), null, bindings, true)) {
            return res.hasNext();
        }
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> select(Service service, Set<String> projectionVars, BindingSet bindings, String baseUri) throws QueryEvaluationException {
        return evaluate(service.getArg(), null, bindings, true);
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Service service, CloseableIteration<BindingSet, QueryEvaluationException> bindings, String baseUri) throws QueryEvaluationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isInitialized() {
        return table != null;
    }

    @Override
    public void shutdown() throws QueryEvaluationException {
        shutDown();
    }

    @Override
    public void shutDown() throws SailException { //release resources
        try {
            table.close(); //close the HTable
            table = null;
        } catch (IOException ex) {
            throw new SailException(ex);
        }
        for (HBaseSail s : federatedServices.values()) { //shutdown all federated services
            s.shutdown();
        }
        federatedServices.clear(); // release the references to the services
    }

    @Override
    public boolean isWritable() throws SailException {
        if (readOnlyTimestamp + STATUS_CACHING_TIMEOUT < System.currentTimeMillis()) try {
            readOnly = table.getTableDescriptor().isReadOnly();
            readOnlyTimestamp = System.currentTimeMillis();
        } catch (IOException ex) {
            throw new SailException(ex);
        }
        return !readOnly;
    }

    @Override
    public SailConnection getConnection() throws SailException {
        return this;
    }

    @Override
    public ValueFactory getValueFactory() {
        return SimpleValueFactory.getInstance();
    }

    @Override
    public List<IsolationLevel> getSupportedIsolationLevels() {
        return Collections.singletonList((IsolationLevel) IsolationLevels.NONE); //limited by HBase's capabilities
    }

    @Override
    public IsolationLevel getDefaultIsolationLevel() {
        return IsolationLevels.NONE;
    }

    @Override
    public boolean isOpen() throws SailException {
        return table != null;  //if the table exists the table is open
    }

    @Override
    public void close() throws SailException {
    }

    //generates a Resource[] from 0 or more Resources
    protected static Resource[] normalizeContexts(Resource... contexts) {
        if (contexts == null || contexts.length == 0) {
            return new Resource[] {null};
        } else {
            return contexts;
        }
    }

    //evaluate queries/ subqueries
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, final boolean includeInferred) throws SailException {
        LOG.log(Level.FINE, "Evaluated TupleExpr before optimizers:\n{0}", tupleExpr);
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

            @Override
            public ValueFactory getValueFactory() {
                return SimpleValueFactory.getInstance();
            }
        };

        EvaluationStrategy strategy = pushStrategy ? new HalyardEvaluationStrategy(source, dataset, this, evaluationTimeout) : new StrictEvaluationStrategy(source, dataset, this);

        new BindingAssigner().optimize(tupleExpr, dataset, bindings);
        new ConstantOptimizer(strategy).optimize(tupleExpr, dataset, bindings);
        new CompareOptimizer().optimize(tupleExpr, dataset, bindings);
        new ConjunctiveConstraintSplitter().optimize(tupleExpr, dataset, bindings);
        new DisjunctiveConstraintOptimizer().optimize(tupleExpr, dataset, bindings);
        new SameTermFilterOptimizer().optimize(tupleExpr, dataset, bindings);
        new QueryModelNormalizer().optimize(tupleExpr, dataset, bindings);
        try {
            //seach for presence of HALYARD.SEARCH_TYPE or HALYARD.PARALLEL_SPLIT_FUNCTION within the query
            tupleExpr.visit(new AbstractQueryModelVisitor<IncompatibleOperationException>(){
                private void checkForSearchType(Value val) {
                    if ((val instanceof Literal) && HALYARD.SEARCH_TYPE.equals(((Literal)val).getDatatype())) {
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
            new QueryJoinOptimizer(statistics) {
                @Override
                public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
                    tupleExpr.visit(new QueryJoinOptimizer.JoinVisitor(){
                        @Override
                        protected double getTupleExprCardinality(TupleExpr tupleExpr, Map<TupleExpr, Double> cardinalityMap, Map<TupleExpr, List<Var>> varsMap, Map<Var, Integer> varFreqMap, Set<String> boundVars) {
                            ((HalyardEvaluationStatistics)statistics).updateCardinalityMap(tupleExpr, boundVars, cardinalityMap);
                            return super.getTupleExprCardinality(tupleExpr, cardinalityMap, varsMap, varFreqMap, boundVars); //To change body of generated methods, choose Tools | Templates.
                        }
                    });
                }
            }.optimize(tupleExpr, dataset, bindings);
        } catch (IncompatibleOperationException ex) {
            //skip QueryJoinOptimizer when HALYARD.SEARCH_TYPE or HALYARD.PARALLEL_SPLIT_FUNCTION is present in the query to avoid re-shuffling of the joins
        }
        // new SubSelectJoinOptimizer().optimize(tupleExpr, dataset, bindings);
        new IterativeEvaluationOptimizer().optimize(tupleExpr, dataset, bindings);
        new FilterOptimizer().optimize(tupleExpr, dataset, bindings);
        new OrderLimitOptimizer().optimize(tupleExpr, dataset, bindings);
        LOG.log(Level.FINE, "Evaluated TupleExpr after optimization:\n{0}", tupleExpr);
        try {
        		//evaluate the expression against the TripleSource according to the EvaluationStrategy.
            CloseableIteration<BindingSet, QueryEvaluationException> iter = evaluateInternal(strategy, tupleExpr);
            return evaluationTimeout <= 0 ? iter : new TimeLimitIteration<BindingSet, QueryEvaluationException>(iter, 1000l * evaluationTimeout) {
                @Override
                protected void throwInterruptedException() throws QueryEvaluationException {
                    throw new QueryEvaluationException("Query evaluation exceeded specified timeout " + evaluationTimeout + "s");
                }
            };
        } catch (QueryEvaluationException ex) {
            throw new SailException(ex);
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
        if (size == 0 && evaluationTimeout > 0) try (CloseableIteration<? extends Statement, SailException> scanner = getStatements(null, null, null, true, contexts)) {
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
        try {
            table.flushCommits(); //execute all buffered puts in HBase
        } catch (IOException ex) {
            throw new SailException(ex);
        }
    }

    @Override
    public void rollback() throws SailException {
    }

    @Override
    public boolean isActive() throws UnknownSailTransactionStateException {
        return true;
    }

    @Override
    public void addStatement(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
        addStatement(subj, pred, obj, contexts);
    }

    protected long getDefaultTimeStamp() {
        return System.currentTimeMillis();
    }

    @Override
    public void addStatement(Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
        long timestamp = getDefaultTimeStamp();
        for (Resource ctx : normalizeContexts(contexts)) {
            addStatementInternal(subj, pred, obj, ctx, timestamp);
        }
    }

    protected void addStatementInternal(Resource subj, IRI pred, Value obj, Resource context, long timestamp) throws SailException {
        if (!isWritable()) throw new SailException(tableName + " is read only");
        try {
            for (KeyValue kv : HalyardTableUtils.toKeyValues(subj, pred, obj, context, false, timestamp)) { //serialize the key value pairs relating to the statement in HBase
                put(kv);
            }
        } catch (IOException e) {
            throw new SailException(e);
        }
    }

    protected void put(KeyValue kv) throws IOException {
        table.put(new Put(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), kv.getTimestamp()).add(kv));
    }

    @Override
    public void removeStatement(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
    	    //should we be defensive about nulls for subj, pre and obj? removeStatements is where you would use nulls, not here.

        if (!isWritable()) throw new SailException(tableName + " is read only");
        long timestamp = getDefaultTimeStamp();
        try {
            for (Resource ctx : normalizeContexts(contexts)) {
                for (KeyValue kv : HalyardTableUtils.toKeyValues(subj, pred, obj, ctx, true, timestamp)) { //calculate the kv's corresponding to the quad (or triple)
                    delete(kv);
                }
            }
        } catch (IOException e) {
            throw new SailException(e);
        }
    }

    protected void delete(KeyValue kv) throws IOException {
        table.delete(new Delete(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()).addDeleteMarker(kv));
    }

    @Override
    public void removeStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
        if (!isWritable()) throw new SailException(tableName + " is read only");
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
        if (!isWritable()) throw new SailException(tableName + " is read only");
        try {
            table = HalyardTableUtils.truncateTable(table); //delete all triples, the whole DB but retains splits!
        } catch (IOException ex) {
            throw new SailException(ex);
        }
    }

    @Override
    public String getNamespace(String prefix) throws SailException {
        Namespace namespace = namespaces.get(prefix);
        return (namespace == null) ? null : namespace.getName();
    }

    @Override
    public CloseableIteration<? extends Namespace, SailException> getNamespaces() {
        return new CloseableIteratorIteration<>(namespaces.values().iterator());
    }

    @Override
    public void setNamespace(String prefix, String name) throws SailException {
        namespaces.put(prefix, new SimpleNamespace(prefix, name));
        ValueFactory vf = SimpleValueFactory.getInstance();
        try {
            removeStatements(null, HALYARD.NAMESPACE_PREFIX_PROPERTY, vf.createLiteral(prefix));
            addStatementInternal(vf.createIRI(name), HALYARD.NAMESPACE_PREFIX_PROPERTY, vf.createLiteral(prefix), HALYARD.SYSTEM_GRAPH_CONTEXT, getDefaultTimeStamp());
        } catch (SailException e) {
            LOG.log(Level.WARNING, "Namespace prefix could not be presisted due to an exception", e);
        }
    }

    @Override
    public void removeNamespace(String prefix) throws SailException {
        ValueFactory vf = SimpleValueFactory.getInstance();
        namespaces.remove(prefix);
        try {
            removeStatements(null, HALYARD.NAMESPACE_PREFIX_PROPERTY, vf.createLiteral(prefix));
        } catch (SailException e) {
            LOG.log(Level.WARNING, "Namespace prefix could not be removed due to an exception", e);
        }
    }

    @Override
    public void clearNamespaces() throws SailException {
        try {
            removeStatements(null, HALYARD.NAMESPACE_PREFIX_PROPERTY, null);
        } catch (SailException e) {
            LOG.log(Level.WARNING, "Namespaces could not be cleared due to an exception", e);
        }
        namespaces.clear();
    }

    Map <String, List<byte[]>> searchCache = Collections.synchronizedMap(new WeakHashMap<>());

    //Scans the Halyard table for statements that match the specified pattern
    private class LiteralSearchStatementScanner extends StatementScanner {

        Iterator<byte[]> objectHashes = null;
        private final String literalSearchQuery;

        public LiteralSearchStatementScanner(long startTime, Resource subj, IRI pred, String literalSearchQuery, Resource... contexts) throws SailException {
            super(startTime, subj, pred, null, contexts);
            if (elasticIndexURL == null || elasticIndexURL.length() == 0) {
                throw new SailException("ElasticSearch Index URL is not properly configured.");
            }
            this.literalSearchQuery = literalSearchQuery;
        }

        @Override
        protected Result nextResult() throws IOException {
            while (true) {
                if (objHash == null) {
                    if (objectHashes == null) { //perform ES query and parse results
                        List<byte[]> objectHashesList = searchCache.get(literalSearchQuery);
                        if (objectHashesList == null) {
                            objectHashesList = new ArrayList<>();
                            HttpURLConnection http = (HttpURLConnection)(new URL(elasticIndexURL + "/_search").openConnection());
                            try {
                                http.setRequestMethod("POST");
                                http.setDoOutput(true);
                                http.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
                                http.connect();
                                try (PrintStream out = new PrintStream(http.getOutputStream(), true, "UTF-8")) {
                                    out.print("{\"query\":{\"query_string\":{\"query\":" + JSONObject.quote(literalSearchQuery) + "}},\"_source\":false,\"stored_fields\":\"_id\",\"size\":" + ELASTIC_RESULT_SIZE + "}");
                                }
                                int response = http.getResponseCode();
                                String msg = http.getResponseMessage();
                                if (response != 200) {
                                    throw new IOException(msg);
                                }
                                try (InputStreamReader isr = new InputStreamReader(http.getInputStream(), "UTF-8")) {
                                    JSONArray hits = new JSONObject(new JSONTokener(isr)).getJSONObject("hits").getJSONArray("hits");
                                    for (int i=0; i<hits.length(); i++) {
                                        objectHashesList.add(Hex.decodeHex(hits.getJSONObject(i).getString("_id").toCharArray()));
                                    }
                                }
                            } catch (JSONException | DecoderException ex) {
                                throw new IOException(ex);
                            } finally {
                                http.disconnect();
                            }
                            searchCache.put(new String(literalSearchQuery), objectHashesList);
                        }
                        objectHashes = objectHashesList.iterator();
                    }
                    if (objectHashes.hasNext()) {
                        objHash = objectHashes.next();
                    } else {
                        return null;
                    }
                    contexts = contextsList.iterator(); //reset iterator over contexts
                }
                Result res = super.nextResult();
                if (res == null) {
                    objHash = null;
                } else {
                    return res;
                }
            }
        }
    }

    private class StatementScanner implements CloseableIteration<Statement, SailException> {

        private final Resource subj;
        private final IRI pred;
        private final Value obj;
        private final byte[] subjHash, predHash;
        protected final List<Resource> contextsList;
        protected Iterator<Resource> contexts;
        protected byte[] objHash;
        private ResultScanner rs = null;
        private final long endTime;
        private Statement next = null;
        private Iterator<Statement> iter = null;

        public StatementScanner(long startTime, Resource subj, IRI pred, Value obj, Resource...contexts) throws SailException {
            this.subj = subj;
            this.pred = pred;
            this.obj = obj;
            this.subjHash = HalyardTableUtils.hashKey(subj);
            this.predHash = HalyardTableUtils.hashKey(pred);
            this.objHash = HalyardTableUtils.hashKey(obj);
            this.contextsList = Arrays.asList(normalizeContexts(contexts));
            this.contexts = contextsList.iterator();
            this.endTime = startTime + (1000l * evaluationTimeout);
            LOG.log(Level.FINEST, "New StatementScanner {0} {1} {2} {3}", new Object[]{subj, pred, obj, contextsList});
        }

        protected Result nextResult() throws IOException { //gets the next result to consider from the HBase Scan
            while (true) {
                if (rs == null) {
                    if (contexts.hasNext()) {

                        //build a ResultScanner from an HBase Scan that finds potential matches
                        rs = table.getScanner(HalyardTableUtils.scan(subjHash, predHash, objHash, HalyardTableUtils.hashKey(contexts.next())));
                    } else {
                        return null;
                    }
                }
                Result res = rs.next();
                if (ticker != null) ticker.tick(); //sends a tick for keep alive purposes
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
            if (evaluationTimeout > 0 && System.currentTimeMillis() > endTime) {
                throw new SailException("Statements scanning exceeded specified timeout " + evaluationTimeout + "s");
            }
            if (next == null) try { //try and find the next result
                while (true) {
                    if (iter == null) {
                        Result res = nextResult();
                        if (res == null) {
                            return false; //no more Results
                        } else {
                            iter = HalyardTableUtils.parseStatements(res).iterator();
                        }
                    }
                    while (iter.hasNext()) {
                        Statement s = iter.next();
                        if ((subj == null || subj.equals(s.getSubject())) && (pred == null || pred.equals(s.getPredicate())) && (obj == null || obj.equals(s.getObject()))) {
                            next = s;  //cache the next statement which will be returned with a call to next().
                            return true; //there is another statement
                        }
                    }
                    iter = null;
                }
            } catch (IOException e) {
                throw new SailException(e);
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
}
