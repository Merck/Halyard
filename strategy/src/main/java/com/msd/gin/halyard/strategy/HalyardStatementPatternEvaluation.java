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
import com.msd.gin.halyard.common.Timestamped;
import com.msd.gin.halyard.strategy.HalyardEvaluationStrategy.ServiceRoot;
import com.msd.gin.halyard.vocab.HALYARD;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.ConvertingIteration;
import org.eclipse.rdf4j.common.iteration.FilterIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.SESAME;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

/**
 * This class evaluates statement patterns as part of query evaluations. It is a helper class for the {@code HalyardEvaluationStrategy}
 * @author Adam Sotona (MSD)
 */
final class HalyardStatementPatternEvaluation {

    private static final int THREADS = 50;

    /**
     * A holder for the BindingSetPipe and the iterator over a tree of query sub-parts
     */
    private static class PipeAndIteration implements Comparable<PipeAndIteration> {

        private final HalyardTupleExprEvaluation.BindingSetPipe pipe;
        private final CloseableIteration<BindingSet, QueryEvaluationException> iter;
        private final int priority;

        /**
         * Constructor for the class with the supplied variables
         * @param pipe The pipe to return evaluations to
         * @param iter The iterator over the evaluation tree
         * @param priority the 'level' of the evaluation in the over-all tree
         */
		public PipeAndIteration(HalyardTupleExprEvaluation.BindingSetPipe pipe,
				CloseableIteration<BindingSet, QueryEvaluationException> iter,
				int priority) {
            this.pipe = pipe;
            this.iter = iter;
            this.priority = priority;
        }

		public boolean pushNext() {
        	try {
                if (pipe.isClosed()) {
                    iter.close();
                } else {
                	if(iter.hasNext()) {
                        BindingSet bs = iter.next();
                        if (pipe.push(bs)) { //true indicates more data is expected from this binding set, put it on the queue
                            if (bs != null) {
                            	return true;
                            }
                        } else { //no more data from this binding set close the iterator of this PipeAndIteration
                            iter.close();
                        }
                	} else {
                		iter.close();
                		pipe.push(null);
                	}
                }
            } catch (Exception e) {
                pipe.handleException(e);
            }
        	return false;
		}

		@Override
		public int compareTo(PipeAndIteration o) {
			return o.priority - this.priority;
		}
    }


    private final Dataset dataset;
    private final TripleSource tripleSource;
    //a map of query model nodes and their priority
    private static final Cache<QueryModelNode, Integer> PRIORITY_MAP_CACHE = CacheBuilder.newBuilder().weakKeys().build();
    private static final PriorityBlockingQueue<PipeAndIteration> PRIORITY_QUEUE = new PriorityBlockingQueue<>(64);

    /**
     * Queues a binding set and {@code QueryModelNode} for evaluation using the current priority.
     * @param pipe the pipe that evaluation results are returned on
     * @param iter
     * @param node an implementation of any {@QueryModelNode} sub-type, typically a {@code ValueExpression}, {@Code UpdateExpression} or {@TupleExpression}
     */
	static void enqueue(HalyardTupleExprEvaluation.BindingSetPipe pipe,
			CloseableIteration<BindingSet, QueryEvaluationException> iter,
			QueryModelNode node) {
        int priority = getPriorityForNode(node);
		PRIORITY_QUEUE.put(new PipeAndIteration(pipe, iter, priority));
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

    /**
     * Static Initializer, sets up the thread group for execution of queries
     */
    static {
        ThreadGroup tg = new ThreadGroup("Halyard Executors");
        for (int i = 0; i < THREADS; i++) {
            final int threadNum = i;
            Thread t = new Thread(tg, new Runnable() {

            		/**
            		 * Defines the behavior of every evaluation thread
            		 */
            		@Override
                public void run() {
                    try {
                        while (true) {
                            PipeAndIteration pai = PRIORITY_QUEUE.take(); //take the highest priority PipeAndIteration object
                            if (pai.priority % THREADS == threadNum) {
                                PRIORITY_QUEUE.put(pai); //always keep some threads out of execution to avoid thread exhaustion
                                Thread.sleep(1);
                            } else {
                            	if (pai.pushNext()) {
                                    PRIORITY_QUEUE.put(pai);
                            	}
                            }
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            t.setDaemon(true);
            t.start();
        }
    }

    /**
     * Constructor
     * @param dataset against which operations can be evaluated (e.g. INSERT, UPDATE)
     * @param tripleSource against which the query is evaluated
     */
	HalyardStatementPatternEvaluation(Dataset dataset, TripleSource tripleSource) {
        this.dataset = dataset;
        this.tripleSource = tripleSource;
    }

    /**
     * Evaluate the statement pattern using the supplied bindings
     * @param parent to push or enqueue evaluation results
     * @param sp the {@code StatementPattern} to evaluate
     * @param bindings the set of names to which values are bound. For example, select ?s, ?p, ?o has the names s, p and o and the values bound to them are the
     * results of the evaluation of this statement pattern
     */
    void evaluateStatementPattern(final HalyardTupleExprEvaluation.BindingSetPipe parent, final StatementPattern sp, final BindingSet bindings) {
        final Var subjVar = sp.getSubjectVar(); //subject
        final Var predVar = sp.getPredicateVar(); //predicate
        final Var objVar = sp.getObjectVar(); //object
        final Var conVar = sp.getContextVar(); //graph or target context

        final Value subjValue = getVarValue(subjVar, bindings);
        final Value predValue = getVarValue(predVar, bindings);
        final Value objValue = getVarValue(objVar, bindings);
        final Value contextValue = getVarValue(conVar, bindings);

        CloseableIteration<? extends Statement, QueryEvaluationException> stIter = null;
        try {
            if (isUnbound(subjVar, bindings) || isUnbound(predVar, bindings) || isUnbound(objVar, bindings) || isUnbound(conVar, bindings)) {
                // the variable must remain unbound for this solution see https://www.w3.org/TR/sparql11-query/#assignment
                parent.push(null);
                return;
            }
            try {
                Resource[] contexts;

                Set<IRI> graphs = null;
                boolean emptyGraph = false;

                if (dataset != null) {
                    if (sp.getScope() == StatementPattern.Scope.DEFAULT_CONTEXTS) { //evaluate against the default graph(s)
                        graphs = dataset.getDefaultGraphs();
                        emptyGraph = graphs.isEmpty() && !dataset.getNamedGraphs().isEmpty();
                    } else { //evaluate against the named graphs
                        graphs = dataset.getNamedGraphs();
                        emptyGraph = graphs.isEmpty() && !dataset.getDefaultGraphs().isEmpty();
                    }
                }

                if (emptyGraph) {
                    // Search zero contexts
                    parent.push(null); //no results from this statement pattern
                    return;
                } else if (graphs == null || graphs.isEmpty()) {
                    // store default behaivour
                    if (contextValue != null) {
                        contexts = new Resource[]{(Resource) contextValue};
                    } /* TODO activate this to have an exclusive (rather than inclusive) interpretation of the default graph in SPARQL querying.
                     else if (sp.getScope() == Scope.DEFAULT_CONTEXTS ) {
                     contexts = new Resource[] { (Resource)null };
                     }
                     */ else {
                        contexts = new Resource[0];
                    }
                } else if (contextValue != null) {
                    if (graphs.contains(contextValue)) {
                        contexts = new Resource[]{(Resource) contextValue};
                    } else {
                        // Statement pattern specifies a context that is not part of
                        // the dataset
                        parent.push(null); //no results possible because the context is not available
                        return;
                    }
                } else {
                    contexts = new Resource[graphs.size()];
                    int i = 0;
                    for (IRI graph : graphs) {
                        IRI context = null;
                        if (!SESAME.NIL.equals(graph)) {
                            context = graph;
                        }
                        contexts[i++] = context;
                    }
                }

                //get an iterator over all triple statements that match the s, p, o specification in the contexts
                stIter = tripleSource.getStatements((Resource) subjValue, (IRI) predValue, objValue, contexts);

                if (contexts.length == 0 && sp.getScope() == StatementPattern.Scope.NAMED_CONTEXTS) {
                    // Named contexts are matched by retrieving all statements from
                    // the store and filtering out the statements that do not have a
                    // context.
                    stIter = new FilterIteration<Statement, QueryEvaluationException>(stIter) {

                        @Override
                        protected boolean accept(Statement st) {
                            return st.getContext() != null;
                        }

                    }; // end anonymous class
                } else if (contexts.length == 0 && sp.getScope() == StatementPattern.Scope.DEFAULT_CONTEXTS) {
                    // Filter out contexts (quads -> triples) and de-duplicate triples
                    stIter = new FilterIteration<Statement, QueryEvaluationException>(stIter) {
                        private Resource lastSubj;
                        private IRI lastPred;
                        private Value lastObj;
                        private Long lastTS;
                        @Override
                        public Statement next() throws QueryEvaluationException {
                            Statement st = super.next();
                            //Filter out contexts
                            return st.getContext() == null ? st : tripleSource.getValueFactory().createStatement(st.getSubject(), st.getPredicate(), st.getObject());
                        }
                        @Override
                        protected boolean accept(Statement st) {
                            //de-duplicate triples
                            if (st.getSubject().equals(lastSubj) && st.getPredicate().equals(lastPred) && st.getObject().equals(lastObj) && Objects.equals(getTimestamp(st), lastTS)) {
                                return false;
                            } else {
                                lastSubj = st.getSubject();
                                lastPred = st.getPredicate();
                                lastObj = st.getObject();
                                lastTS = getTimestamp(st);
                                return true;
                            }
                        }

                        private Long getTimestamp(Statement st) {
                        	return (st instanceof Timestamped) ? ((Timestamped)st).getTimestamp() : null;
                        }
                    };
                } else if (graphs != null && graphs.contains(SESAME.NIL)) {
                    // usage of SESAME.NIL triggers query over all graphs, which must be filtered here
                    final Set<Resource> ctxSet = new HashSet<>(Arrays.asList(contexts));
                    stIter = new FilterIteration<Statement, QueryEvaluationException>(stIter) {
                        @Override
                        protected boolean accept(Statement st) {
                            return ctxSet.contains(st.getContext());
                        }
                    };                	
                }
            } catch (ClassCastException e) {
                // Invalid value type for subject, predicate and/or context
                parent.push(null);
                return;
            }
        } catch (InterruptedException | QueryEvaluationException e) {
            parent.handleException(e);
            return;
        }

        // The same variable might have been used multiple times in this
        // StatementPattern, verify value equality in those cases.
        // TODO: skip this filter if not necessary
        stIter = new FilterIteration<Statement, QueryEvaluationException>(stIter) {

            @Override
            protected boolean accept(Statement st) {
                Resource subj = st.getSubject();
                IRI pred = st.getPredicate();
                Value obj = st.getObject();
                Resource context = st.getContext();

                if (subjVar != null && subjValue == null) {
                    if (subjVar.equals(predVar) && !subj.equals(pred)) {
                        return false;
                    }
                    if (subjVar.equals(objVar) && !subj.equals(obj)) {
                        return false;
                    }
                    if (subjVar.equals(conVar) && !subj.equals(context)) {
                        return false;
                    }
                }

                if (predVar != null && predValue == null) {
                    if (predVar.equals(objVar) && !pred.equals(obj)) {
                        return false;
                    }
                    if (predVar.equals(conVar) && !pred.equals(context)) {
                        return false;
                    }
                }

                if (objVar != null && objValue == null) {
                    if (objVar.equals(conVar) && !obj.equals(context)) {
                        return false;
                    }
                }

                return true;
            }
        };

        // Return an iterator that converts the RDF statements (triples) to var bindings
        enqueue(parent, new ConvertingIteration<Statement, BindingSet, QueryEvaluationException>(stIter) {

            @Override
            protected BindingSet convert(Statement st) {
                QueryBindingSet result = new QueryBindingSet(bindings);
                if (subjVar != null && !subjVar.isConstant() && !result.hasBinding(subjVar.getName())) {
                    result.addBinding(subjVar.getName(), st.getSubject());
                }
                if (predVar != null && !predVar.isConstant() && !result.hasBinding(predVar.getName())) {
                    result.addBinding(predVar.getName(), st.getPredicate());
                }
                if (objVar != null && !objVar.isConstant()) {
                    Value val = result.getValue(objVar.getName());
                    // override Halyard search type object literals with real object value from the statement
                    if (!result.hasBinding(objVar.getName()) || ((val instanceof Literal) && HALYARD.SEARCH_TYPE.equals(((Literal)val).getDatatype()))) {
                        result.setBinding(objVar.getName(), st.getObject());
                    }
                }
                if (conVar != null && !conVar.isConstant() && !result.hasBinding(conVar.getName())
                        && st.getContext() != null) {
                    result.addBinding(conVar.getName(), st.getContext());
                }

                return result;
            }
		}, sp);
    }

    protected boolean isUnbound(Var var, BindingSet bindings) {
        if (var == null) {
            return false;
        } else {
            return bindings.hasBinding(var.getName()) && bindings.getValue(var.getName()) == null;
        }
    }

    /**
     * Gets a value from a {@code Var} if it has a {@code Value}. If it does not then the method will attempt to get it
     * from the bindings using the name of the Var
     * @param var
     * @param bindings
     * @return the matching {@code Value} or {@code null} if var is {@code null}
     */
    private static Value getVarValue(Var var, BindingSet bindings) {
        if (var == null) {
            return null;
        } else if (var.hasValue()) {
            return var.getValue();
        } else {
            return bindings.getValue(var.getName());
        }
    }
}
