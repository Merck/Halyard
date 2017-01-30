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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.ConvertingIteration;
import org.eclipse.rdf4j.common.iteration.FilterIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.SESAME;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

/**
 *
 * @author Adam Sotona (MSD)
 */
final class HalyardStatementPatternEvaluation {

    private static final int THREADS = 50;

    private static class PipeAndIteration {

        private final HalyardTupleExprEvaluation.BindingSetPipe pipe;
        private final CloseableIteration<BindingSet, QueryEvaluationException> iter;
        private final int priority;

        public PipeAndIteration(HalyardTupleExprEvaluation.BindingSetPipe pipe, CloseableIteration<BindingSet, QueryEvaluationException> iter, int priority) {
            this.pipe = pipe;
            this.iter = iter;
            this.priority = priority;
        }
    }

    private static class PriorityQueue<E> {

        @SuppressWarnings("unchecked")
        private LinkedList<E>[] q = new LinkedList[64];

        public synchronized void put(int level, E e) {
            if (q.length <= level) {
                q = Arrays.copyOf(q, level + 16);
            }
            LinkedList<E> subQueue = q[level];
            if (subQueue == null) {
                subQueue = new LinkedList<>();
                q[level] = subQueue;
            }
            subQueue.addLast(e);
            notify();
        }

        private E poll() {
            for (int i=q.length - 1; i >= 0; i--) {
                LinkedList<E> subQueue = q[i];
                E e = subQueue == null || subQueue.isEmpty() ? null : subQueue.removeFirst();
                if (e != null) return e;
            }
            return null;
        }

        public synchronized E take() throws InterruptedException {
            E e;
            while ((e = poll()) == null) {
                wait();
            }
            return e;
        }
    }

    private static class IdentityWrapper<T> {

        int hash;
        T o;

        IdentityWrapper(T o) {
            this.o = o;
            hash = System.identityHashCode(o);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            return (o instanceof IdentityWrapper) && (this.o == ((IdentityWrapper)o).o);
        }
    }


    private final Dataset dataset;
    private final TripleSource tripleSource;
    private static final Map<IdentityWrapper<QueryModelNode>, Integer> PRIORITY_MAP_CACHE = Collections.synchronizedMap(new WeakHashMap<>());
    private static final PriorityQueue<PipeAndIteration> PRIORITY_QUEUE = new PriorityQueue<>();

    static void enqueue(HalyardTupleExprEvaluation.BindingSetPipe pipe,  CloseableIteration<BindingSet, QueryEvaluationException> iter, QueryModelNode node) {
        int priority = getPriorityForNode(node);
        PRIORITY_QUEUE.put(priority, new PipeAndIteration(pipe, iter, priority));
    }

    private static int getPriorityForNode(final QueryModelNode node) {
        Integer p = PRIORITY_MAP_CACHE.get(new IdentityWrapper<>(node));
        if (p != null) {
            return p;
        } else {
            final AtomicInteger counter = new AtomicInteger();
            final AtomicInteger ret = new AtomicInteger();
            QueryModelNode root = node;
            while (root.getParentNode() != null) root = root.getParentNode();
            new AbstractQueryModelVisitor<RuntimeException>() {
                @Override
                protected void meetNode(QueryModelNode n) throws RuntimeException {
                    int pp = counter.getAndIncrement();
                    PRIORITY_MAP_CACHE.put(new IdentityWrapper<>(n), pp);
                    if (n == node) ret.set(pp);
                    super.meetNode(n);
                }

                @Override
                public void meet(Filter node) throws RuntimeException {
                    node.visitChildren(this);
                    node.getCondition().visit(this);
                }

            }.meetOther(root);
            return ret.get();
        }
    }

    static {
        ThreadGroup tg = new ThreadGroup("Halyard Executors");
        for (int i = 0; i < THREADS; i++) {
            final int threadNum = i;
            Thread t = new Thread(tg, new Runnable() {
                @Override
                public void run() {
                    try {
                        while (true) {
                            PipeAndIteration pai = PRIORITY_QUEUE.take();
                            if (pai.priority % THREADS == threadNum) {
                                PRIORITY_QUEUE.put(pai.priority, pai); //always keep some threads out of execution to avoid thread exhaustion
                                Thread.sleep(100);
                            } else try {
                                if (pai.pipe.isClosed()) {
                                    pai.iter.close();
                                } else {
                                    BindingSet bs = pai.iter.next();
                                    if (pai.pipe.push(bs)) {
                                        if (bs != null) {
                                            PRIORITY_QUEUE.put(pai.priority, pai);
                                        }
                                    } else {
                                        pai.iter.close();
                                    }
                                }
                            } catch (NoSuchElementException e) {
                                pai.pipe.push(null);
                            } catch (Exception e) {
                                pai.pipe.handleException(e);
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

    HalyardStatementPatternEvaluation(Dataset dataset, TripleSource tripleSource) {
        this.dataset = dataset;
        this.tripleSource = tripleSource;
    }

    void evaluateStatementPattern(final HalyardTupleExprEvaluation.BindingSetPipe parent, final StatementPattern sp, final BindingSet bindings) {
        final Var subjVar = sp.getSubjectVar();
        final Var predVar = sp.getPredicateVar();
        final Var objVar = sp.getObjectVar();
        final Var conVar = sp.getContextVar();

        final Value subjValue = getVarValue(subjVar, bindings);
        final Value predValue = getVarValue(predVar, bindings);
        final Value objValue = getVarValue(objVar, bindings);
        final Value contextValue = getVarValue(conVar, bindings);

        CloseableIteration<? extends Statement, QueryEvaluationException> stIter = null;

        try {
            try {
                Resource[] contexts;

                Set<IRI> graphs = null;
                boolean emptyGraph = false;

                if (dataset != null) {
                    if (sp.getScope() == StatementPattern.Scope.DEFAULT_CONTEXTS) {
                        graphs = dataset.getDefaultGraphs();
                        emptyGraph = graphs.isEmpty() && !dataset.getNamedGraphs().isEmpty();
                    } else {
                        graphs = dataset.getNamedGraphs();
                        emptyGraph = graphs.isEmpty() && !dataset.getDefaultGraphs().isEmpty();
                    }
                }

                if (emptyGraph) {
                    // Search zero contexts
                    parent.push(null);
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
                        parent.push(null);
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
                }
            } catch (ClassCastException e) {
                // Invalid value type for subject, predicate and/or context
                parent.push(null);
                return;
            }
        } catch (InterruptedException | QueryEvaluationException e) {
            parent.handleException(e);
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

        // Return an iterator that converts the statements to var bindings
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
                if (objVar != null && !objVar.isConstant() && !result.hasBinding(objVar.getName())) {
                    result.addBinding(objVar.getName(), st.getObject());
                }
                if (conVar != null && !conVar.isConstant() && !result.hasBinding(conVar.getName())
                        && st.getContext() != null) {
                    result.addBinding(conVar.getName(), st.getContext());
                }

                return result;
            }
        }, sp);
    }

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
