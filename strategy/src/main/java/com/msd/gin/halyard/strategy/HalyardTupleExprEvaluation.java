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

import com.msd.gin.halyard.strategy.HalyardTupleExprEvaluation.BindingSetPipe;
import com.msd.gin.halyard.strategy.collections.BigHashSet;
import com.msd.gin.halyard.strategy.collections.Sorter;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.CloseableIteratorIteration;
import org.eclipse.rdf4j.common.iteration.LookAheadIteration;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.query.algebra.AggregateOperator;
import org.eclipse.rdf4j.query.algebra.ArbitraryLengthPath;
import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.DescribeOperator;
import org.eclipse.rdf4j.query.algebra.Difference;
import org.eclipse.rdf4j.query.algebra.Distinct;
import org.eclipse.rdf4j.query.algebra.EmptySet;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.ExtensionElem;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Group;
import org.eclipse.rdf4j.query.algebra.Intersection;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.MultiProjection;
import org.eclipse.rdf4j.query.algebra.Order;
import org.eclipse.rdf4j.query.algebra.OrderElem;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.ProjectionElem;
import org.eclipse.rdf4j.query.algebra.ProjectionElemList;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.Reduced;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.Slice;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.SubQueryValueOperator;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.ZeroLengthPath;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ExternalSet;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.DescribeIteration;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.GroupIterator;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.PathIteration;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.ProjectionIterator;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.SilentIteration;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.ZeroLengthPathIteration;
import org.eclipse.rdf4j.query.algebra.evaluation.util.ValueComparator;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.VarNameCollector;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.query.impl.MapBindingSet;

/**
 * Evaluates {@link TupleExpression}s and it's sub-interfaces and implementations.
 * @author Adam Sotona (MSD)
 */
final class HalyardTupleExprEvaluation {

    private static final int MAX_QUEUE_SIZE = 1000;

    /**
     * Binding set pipes instances hold {@BindingSet}s (set of evaluated, un-evaluated and intermediate variables) that
     * form part of the query evaluation (a query generates an evaluation tree).
     */
    static abstract class BindingSetPipe {

        protected final BindingSetPipe parent;

        /**
         * Create a pipe
         * @param parent the parent of this part of the evaluation chain
         */
        protected BindingSetPipe(BindingSetPipe parent) {
            this.parent = parent;
        }

        /**
         * Pushes BindingSet up to the pipe, pushing null indicates end of data. In case you need to interrupt the tree data flow (when for example just a Slice
         * of data is expected), it is necessary to indicate that no more data are expected down the tree (to stop feeding this pipe) by returning false and
         * also to indicate up the tree that this is the end of data (by pushing null into the parent pipe in the evaluation tree).
         *
         * @param bs BindingSet or null if there are no more data
         * @return boolean indicating if more data are expected from the caller
         * @throws InterruptedException
         * @throws QueryEvaluationException
         */
        public abstract boolean push(BindingSet bs) throws InterruptedException;

        protected void handleException(Exception e) {
            if (parent != null) {
                parent.handleException(e);
            }
        }

        protected boolean isClosed() {
            if (parent != null) {
                return parent.isClosed();
            } else {
                return false;
            }
        }
    }

    private final HalyardEvaluationStrategy parentStrategy;
    private final HalyardStatementPatternEvaluation statementEvaluation;
    private final long startTime, timeout;

    /**
     * Constructor used by {@link HalyardEvaluationStrategy} to create this helper class
     * @param parentStrategy
     * @param tripleSource
     * @param dataset
     * @param timeout
     */
    HalyardTupleExprEvaluation(HalyardEvaluationStrategy parentStrategy, TripleSource tripleSource, Dataset dataset, long timeout) {
        this.parentStrategy = parentStrategy;
        this.statementEvaluation = new HalyardStatementPatternEvaluation(dataset, tripleSource);
        this.startTime = System.currentTimeMillis();
        this.timeout = timeout;
    }

    /**
     * Returns an iterator on the binding set pipe
     * @param expr supplied by HalyardEvaluationStrategy
     * @param bindings supplied by HalyardEvaluationStrategy
     * @return an iterator on the binding set pipe
     */
    CloseableIteration<BindingSet, QueryEvaluationException> evaluate(TupleExpr expr, BindingSet bindings) {
        BindingSetPipeIterator root = new BindingSetPipeIterator();
        evaluateTupleExpr(root.pipe, expr, bindings);
        return root;
    }

    /**
     * Switch logic appropriate for each type of {@link TupleExpr} query model node, sending each type to it's appropriate evaluation method. For example,
     * {@code UnaryTupleOperator} is sent to {@link evaluateUnaryTupleOperator()}.
     * @param parent
     * @param expr
     * @param bindings
     */
    private void evaluateTupleExpr(BindingSetPipe parent, TupleExpr expr, BindingSet bindings) {
        if (expr instanceof StatementPattern) {
            statementEvaluation.evaluateStatementPattern(parent, (StatementPattern) expr, bindings);
        } else if (expr instanceof UnaryTupleOperator) {
            evaluateUnaryTupleOperator(parent, (UnaryTupleOperator) expr, bindings);
        } else if (expr instanceof BinaryTupleOperator) {
            evaluateBinaryTupleOperator(parent, (BinaryTupleOperator) expr, bindings);
        } else if (expr instanceof SingletonSet) {
            evaluateSingletonSet(parent, (SingletonSet) expr, bindings);
        } else if (expr instanceof EmptySet) {
            evaluateEmptySet(parent, (EmptySet) expr, bindings);
        } else if (expr instanceof ExternalSet) {
            evaluateExternalSet(parent, (ExternalSet) expr, bindings);
        } else if (expr instanceof ZeroLengthPath) {
            evaluateZeroLengthPath(parent, (ZeroLengthPath) expr, bindings);
        } else if (expr instanceof ArbitraryLengthPath) {
            evaluateArbitraryLengthPath(parent, (ArbitraryLengthPath) expr, bindings);
        } else if (expr instanceof BindingSetAssignment) {
            evaluateBindingSetAssignment(parent, (BindingSetAssignment) expr, bindings);
        } else if (expr == null) {
            parent.handleException(new IllegalArgumentException("expr must not be null"));
        } else {
            parent.handleException(new QueryEvaluationException("Unsupported tuple expr type: " + expr.getClass()));
        }
    }

    /**
     * Switch logic for evaluation of any instance of a {@link UnaryTupleOperator} query model node
     * @param parent
     * @param expr
     * @param bindings
     */
    private void evaluateUnaryTupleOperator(BindingSetPipe parent, UnaryTupleOperator expr, BindingSet bindings) {
        if (expr instanceof Projection) {
            evaluateProjection(parent, (Projection) expr, bindings);
        } else if (expr instanceof MultiProjection) {
            evaluateMultiProjection(parent, (MultiProjection) expr, bindings);
        } else if (expr instanceof Filter) {
            evaluateFilter(parent, (Filter) expr, bindings);
        } else if (expr instanceof Service) {
            evaluateService(parent, (Service) expr, bindings);
        } else if (expr instanceof Slice) {
            evaluateSlice(parent, (Slice) expr, bindings);
        } else if (expr instanceof Extension) {
            evaluateExtension(parent, (Extension) expr, bindings);
        } else if (expr instanceof Distinct) {
            evaluateDistinct(parent, (Distinct) expr, bindings);
        } else if (expr instanceof Reduced) {
            evaluateReduced(parent, (Reduced) expr, bindings);
        } else if (expr instanceof Group) {
            evaluateGroup(parent, (Group) expr, bindings);
        } else if (expr instanceof Order) {
            evaluateOrder(parent, (Order) expr, bindings);
        } else if (expr instanceof QueryRoot) {
            parentStrategy.sharedValueOfNow = null;
            evaluateTupleExpr(parent, ((QueryRoot) expr).getArg(), bindings);
        } else if (expr instanceof DescribeOperator) {
            evaluateDescribeOperator(parent, (DescribeOperator) expr, bindings);
        } else if (expr == null) {
            parent.handleException(new IllegalArgumentException("expr must not be null"));
        } else {
            parent.handleException(new QueryEvaluationException("Unknown unary tuple operator type: " + expr.getClass()));
        }
    }

    /**
     * Evaluate a {@link Projection} query model nodes
     * @param parent
     * @param projection
     * @param bindings
     */
    private void evaluateProjection(BindingSetPipe parent, final Projection projection, final BindingSet bindings) {
        boolean outer = true;
        QueryModelNode ancestor = projection;
        while (ancestor.getParentNode() != null) {
                ancestor = ancestor.getParentNode();
                if (ancestor instanceof Projection || ancestor instanceof MultiProjection) {
                        outer = false;
                }
        }
        final boolean includeAll = !outer;
        BindingSet pushDownBindings;
        if (projection.isSubquery()) {
            pushDownBindings = new QueryBindingSet();
            for (ProjectionElem pe : projection.getProjectionElemList().getElements()) {
                    Value targetValue = bindings.getValue(pe.getSourceName());
                    if (targetValue != null) {
                            ((QueryBindingSet)pushDownBindings).setBinding(pe.getTargetName(), targetValue);
                    }
            }
        } else {
            pushDownBindings = bindings;
        }

        evaluateTupleExpr(new BindingSetPipe(parent) {
            @Override
            public boolean push(BindingSet bs) throws InterruptedException {
                return parent.push(bs == null ? null : ProjectionIterator.project(projection.getProjectionElemList(), bs, bindings, includeAll));
            }
        }, projection.getArg(), pushDownBindings);
    }

    /**
     * Evaluate a {@link MultiProjection} query model nodes
     * @param parent
     * @param multiProjection
     * @param bindings
     */
    private void evaluateMultiProjection(BindingSetPipe parent, final MultiProjection multiProjection, final BindingSet bindings) {
        final List<ProjectionElemList> projections = multiProjection.getProjections();
        final BindingSet prev[] = new BindingSet[projections.size()];
        evaluateTupleExpr(new BindingSetPipe(parent) {
            @Override
            public boolean push(BindingSet bs) throws InterruptedException {
                if (bs == null) {
                    return parent.push(null);
                }
                for (int i=0; i<prev.length; i++) {
                    BindingSet nb = ProjectionIterator.project(projections.get(i), bs, bindings);
                    //ignore duplicates
                    boolean push = false;
                    synchronized (prev) {
                        if (!nb.equals(prev[i])) {
                            prev[i] = nb;
                            push = true;
                        }
                    }
                    if (push) {
                        if (!parent.push(nb)) return false;
                    }
                }
                return true;
            }
        }, multiProjection.getArg(), bindings);
    }

    /**
     * Evaluates filter {@link ExpressionTuple}s query model nodes pushing the result to the parent BindingSetPipe.
     * @param parent the pipe to which results are pushed
     * @param filter holds the details of any FILTER expression in a SPARQL query and any sub-chains.
     * @param bindings
     */
    private void evaluateFilter(BindingSetPipe parent, final Filter filter, final BindingSet bindings) {
        final Set<String> scopeBindingNames = filter.getBindingNames();
        evaluateTupleExpr(new BindingSetPipe(parent) {
            @Override
            public boolean push(BindingSet bs) throws InterruptedException {
                if (bs == null) {
                    return parent.push(null);
                }
                try {
                    if (accept(bs)) {
                        return parent.push(bs); //push results that pass the filter.
                    } else {
                        return true; //nothing passes the filter but processing continues.
                    }
                } catch (QueryEvaluationException e) {
                    parent.handleException(e);
                }
                return false;
            }
            private boolean accept(BindingSet bindings) throws QueryEvaluationException {
                try {
                    // Limit the bindings to the ones that are in scope for this filter
                    QueryBindingSet scopeBindings = new QueryBindingSet(bindings);
                    // FIXME J1 scopeBindingNames should include bindings from superquery if the filter
                    // is part of a subquery. This is a workaround: we should fix the settings of scopeBindingNames,
                    // rather than skipping the limiting of bindings.
                    if (!isPartOfSubQuery(filter)) {
                        scopeBindings.retainAll(scopeBindingNames);
                    }
                    return parentStrategy.isTrue(filter.getCondition(), scopeBindings);
                } catch (ValueExprEvaluationException e) {
                    // failed to evaluate condition
                    return false;
                }
            }
        }, filter.getArg(), bindings);
    }

    /**
     * Evaluate {@link DescribeOperator} query model nodes
     * @param parent
     * @param operator
     * @param bindings
     */
    private void evaluateDescribeOperator(BindingSetPipe parent, DescribeOperator operator, BindingSet bindings) {
        HalyardStatementPatternEvaluation.enqueue(parent, new DescribeIteration(evaluate(operator.getArg(), bindings), parentStrategy, operator.getBindingNames(), bindings), operator);
    }

    /**
     * Makes a {@link BindingSet} comparable
     * @author schremar
     *
     */
    private static class ComparableBindingSetWrapper implements Comparable<ComparableBindingSetWrapper>, Serializable {

        private static final long serialVersionUID = -7341340704807086829L;

        private static final ValueComparator VC = new ValueComparator();

        private final BindingSet bs;
        private final Value values[];
        private final boolean ascending[];
        private final long minorOrder;


        public ComparableBindingSetWrapper(EvaluationStrategy strategy, BindingSet bs, List<OrderElem> elements, long minorOrder) throws QueryEvaluationException {
            this.bs = bs;
            this.values = new Value[elements.size()];
            this.ascending = new boolean[elements.size()];
            for (int i = 0; i < values.length; i++) {
                OrderElem oe = elements.get(i);
                try {
                    values[i] = strategy.evaluate(oe.getExpr(), bs);
                } catch (ValueExprEvaluationException exc) {
                    values[i] = null;
                }
                ascending[i] = oe.isAscending();
            }
            this.minorOrder = minorOrder;
        }

        @Override
        public int compareTo(ComparableBindingSetWrapper o) {
            if (equals(o)) return 0;
            for (int i=0; i<values.length; i++) {
                int cmp = ascending[i] ? VC.compare(values[i], o.values[i]) : VC.compare(o.values[i], values[i]);
                if (cmp != 0) {
                    return cmp;
                }
            }
            return Long.compare(minorOrder, o.minorOrder);
        }

        @Override
        public int hashCode() {
            return bs.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ComparableBindingSetWrapper && bs.equals(((ComparableBindingSetWrapper)obj).bs);
        }

    }

    /**
     * Evaluate {@link Order} query model nodes
     * @param parent
     * @param order
     * @param bindings
     */
    private void evaluateOrder(final BindingSetPipe parent, final Order order, BindingSet bindings) {
//        try {
            final Sorter<ComparableBindingSetWrapper> sorter = new Sorter<>(getLimit(order), isReducedOrDistinct(order));
            final AtomicLong minorOrder = new AtomicLong();
            evaluateTupleExpr(new BindingSetPipe(parent) {

                @Override
                protected void handleException(Exception e) {
                    sorter.close();
                    super.handleException(e);
                }

                @Override
                public boolean push(BindingSet bs) throws InterruptedException {
                    if (bs != null) try {
                        ComparableBindingSetWrapper cbsw = new ComparableBindingSetWrapper(parentStrategy, bs, order.getElements(), minorOrder.getAndIncrement());
                        synchronized (sorter) {
                            sorter.add(cbsw);
                        }
                        return true;
                    } catch (QueryEvaluationException | IOException e) {
                        handleException(e);
                        return false;
                    }
                    try {
                        for (Map.Entry<ComparableBindingSetWrapper, Long> me : sorter) {
                            for (long i = me.getValue(); i > 0; i--) {
                                if (!parent.push(me.getKey().bs)) {
                                    return false;
                                }
                            }
                        }
                        return parent.push(null);
                    } finally {
                        sorter.close();
                    }
                }
            }, order.getArg(), bindings);
//        } catch (IOException e) {
//            throw new QueryEvaluationException(e);
//        }
    }

    /**
     * Evaluate {@link Group} query model nodes
     * @param parent
     * @param group
     * @param bindings
     */
    private void evaluateGroup(BindingSetPipe parent, Group group, BindingSet bindings) {
        //temporary solution using copy of the original iterator
        //re-writing this to push model is a bit more complex task
        try {
            HalyardStatementPatternEvaluation.enqueue(parent, new GroupIterator(parentStrategy, group, bindings), group);
        } catch (QueryEvaluationException e) {
            parent.handleException(e);
        }
    }

    /**
     * Evaluate {@link Reduced} query model nodes
     * @param parent
     * @param reduced
     * @param bindings
     */
    private void evaluateReduced(BindingSetPipe parent, Reduced reduced, BindingSet bindings) {
        evaluateTupleExpr(new BindingSetPipe(parent) {
            private BindingSet previous = null;

            @Override
            public boolean push(BindingSet bs) throws InterruptedException {
                synchronized (this) {
                    if (bs != null && bs.equals(previous)) {
                        previous = bs;
                        return true;
                    }
                }
                return parent.push(bs);
            }
        }, reduced.getArg(), bindings);
    }

    /**
     * Evaluate {@link Distinct} query model nodes
     * @param parent
     * @param distinct
     * @param bindings
     */
    private void evaluateDistinct(BindingSetPipe parent, final Distinct distinct, BindingSet bindings) {
        evaluateTupleExpr(new BindingSetPipe(parent) {
            private final BigHashSet<BindingSet> set = new BigHashSet<>();
            @Override
            protected void handleException(Exception e) {
                set.close();
                super.handleException(e);
            }
            @Override
            public boolean push(BindingSet bs) throws InterruptedException {
                synchronized (set) {
                    if (bs == null) {
                        set.close();
                    } else try {
                        if (!set.add(bs)) {
                            return true;
                        }
                    } catch (IOException e) {
                        handleException(e);
                        return false;
                    }
                }
                return parent.push(bs);
            }
        }, distinct.getArg(), bindings);
    }

	/**
	 * Evaluate {@link Extension} query model nodes
	 * @param parent
	 * @param extension
	 * @param bindings
	 */
    private void evaluateExtension(BindingSetPipe parent, final Extension extension, BindingSet bindings) {
        evaluateTupleExpr(new BindingSetPipe(parent) {
            @Override
            public boolean push(BindingSet bs) throws InterruptedException {
                if (bs == null) {
                    return parent.push(null);
                }
                QueryBindingSet targetBindings = new QueryBindingSet(bs);
                for (ExtensionElem extElem : extension.getElements()) {
                    ValueExpr expr = extElem.getExpr();
                    if (!(expr instanceof AggregateOperator)) {
                        try {
                            // we evaluate each extension element over the targetbindings, so that bindings from
                            // a previous extension element in this same extension can be used by other extension elements.
                            // e.g. if a projection contains (?a + ?b as ?c) (?c * 2 as ?d)
                            Value targetValue = parentStrategy.evaluate(extElem.getExpr(), targetBindings);
                            if (targetValue != null) {
                                // Potentially overwrites bindings from super
                                targetBindings.setBinding(extElem.getName(), targetValue);
                            }
                        } catch (ValueExprEvaluationException e) {
                            // silently ignore type errors in extension arguments. They should not cause the
                            // query to fail but result in no bindings for this solution
                            // see https://www.w3.org/TR/sparql11-query/#assignment
                            // use null as place holder for unbound variables that must remain so
                            targetBindings.setBinding(extElem.getName(), null);
                        } catch (QueryEvaluationException e) {
                            parent.handleException(e);
                        }
                    }
                }
                return parent.push(targetBindings);
            }
        }, extension.getArg(), bindings);
    }

    /**
     * Evaluate {@link Slice} query model nodes.
     * @param parent
     * @param slice
     * @param bindings
     */
    private void evaluateSlice(BindingSetPipe parent, Slice slice, BindingSet bindings) {
        final long offset = slice.hasOffset() ? slice.getOffset() : 0;
        final long limit = slice.hasLimit() ? offset + slice.getLimit() : Long.MAX_VALUE;
        evaluateTupleExpr(new BindingSetPipe(parent) {
            private final AtomicLong ll = new AtomicLong(0);
            @Override
            public boolean push(BindingSet bs) throws InterruptedException {
                long l = ll.incrementAndGet();
                if (l > limit+1) {
                    return false;
                }
                if (bs == null) return parent.push(null);
                if (l <= offset) {
                    return true;
                } else if (l <= limit) {
                    return parent.push(bs);
                } else {
                    return parent.push(null);
                }
            }
        }, slice.getArg(), bindings);
    }

    /**
     * Evaluate {@link Service} query model nodes
     * @param parent
     * @param service
     * @param bindings
     */
    private void evaluateService(BindingSetPipe parent, Service service, BindingSet bindings) {
        Var serviceRef = service.getServiceRef();
        String serviceUri;
        if (serviceRef.hasValue()) {
            serviceUri = serviceRef.getValue().stringValue();
        } else if (bindings != null && bindings.getValue(serviceRef.getName()) != null) {
            serviceUri = bindings.getBinding(serviceRef.getName()).getValue().stringValue();
        } else {
            throw new QueryEvaluationException("SERVICE variables must be bound at evaluation time.");
        }
        try {
            try {
                FederatedService fs = parentStrategy.getService(serviceUri);
                // create a copy of the free variables, and remove those for which
                // bindings are available (we can set them as constraints!)
                Set<String> freeVars = new HashSet<>(service.getServiceVars());
                freeVars.removeAll(bindings.getBindingNames());
                // Get bindings from values pre-bound into variables.
                MapBindingSet allBindings = new MapBindingSet();
                for (Binding binding : bindings) {
                    allBindings.addBinding(binding.getName(), binding.getValue());
                }
                final Set<Var> boundVars = new HashSet<Var>();
                new AbstractQueryModelVisitor<RuntimeException> (){
                    @Override
                    public void meet(Var var) {
                        if (var.hasValue()) {
                            boundVars.add(var);
                        }
                    }
                }.meet(service);
                for (Var boundVar : boundVars) {
                    freeVars.remove(boundVar.getName());
                    allBindings.addBinding(boundVar.getName(), boundVar.getValue());
                }
                bindings = allBindings;
                String baseUri = service.getBaseURI();
                // special case: no free variables => perform ASK query
                if (freeVars.isEmpty()) {
                    boolean exists = fs.ask(service, bindings, baseUri);
                    // check if triples are available (with inserted bindings)
                    if (exists) {
                        parent.push(bindings);
                    }
                    parent.push(null);
                    return;

                }
                // otherwise: perform a SELECT query
                CloseableIteration<BindingSet, QueryEvaluationException> result = fs.select(service, freeVars, bindings, baseUri);
                HalyardStatementPatternEvaluation.enqueue(parent, service.isSilent() ? new SilentIteration(result) : result, service);
            } catch (QueryEvaluationException e) {
                // suppress exceptions if silent
                if (service.isSilent()) {
                    parent.push(bindings);
                    parent.push(null);
                } else {
                    throw e;
                }
            } catch (RuntimeException e) {
                // suppress special exceptions (e.g. UndeclaredThrowable with
                // wrapped
                // QueryEval) if silent
                if (service.isSilent()) {
                    parent.push(bindings);
                    parent.push(null);
                } else {
                    throw e;
                }
            }
        } catch (InterruptedException ie) {
            parent.handleException(ie);
        }
    }

    /**
     * Evaluate {@link BinaryTupleOperator} query model nodes
     * @param parent
     * @param expr
     * @param bindings
     */
    private void evaluateBinaryTupleOperator(BindingSetPipe parent, BinaryTupleOperator expr, BindingSet bindings) {
        if (expr instanceof Join) {
            evaluateJoin(parent, (Join) expr, bindings);
        } else if (expr instanceof LeftJoin) {
            evaluateLeftJoin(parent, (LeftJoin) expr, bindings);
        } else if (expr instanceof Union) {
            evaluateUnion(parent, (Union) expr, bindings);
        } else if (expr instanceof Intersection) {
            evaluateIntersection(parent, (Intersection) expr, bindings);
        } else if (expr instanceof Difference) {
            evaluateDifference(parent, (Difference) expr, bindings);
        } else if (expr == null) {
            parent.handleException(new IllegalArgumentException("expr must not be null"));
        } else {
            parent.handleException(new QueryEvaluationException("Unsupported binary tuple operator type: " + expr.getClass()));
        }
    }

    /**
     * Evaluate {@link Join} query model nodes.
     * @param topPipe
     * @param join
     * @param bindings
     */
    private void evaluateJoin(BindingSetPipe topPipe, final Join join, final BindingSet bindings) {
        final AtomicLong joinsInProgress = new AtomicLong(1);
        BindingSetPipe rightPipe = new BindingSetPipe(topPipe) {
            @Override
            public boolean push(BindingSet bs) throws InterruptedException {
                if (bs == null) {
                    if (joinsInProgress.decrementAndGet() == 0) {
                        parent.push(null);
                    }
                    return false;
                } else {
                    return parent.push(bs);
                }
            }
        };
        evaluateTupleExpr(new BindingSetPipe(rightPipe) {
            @Override
            public boolean push(BindingSet bs) throws InterruptedException {
                if (bs == null) {
                    return parent.push(null);
                } else {
                    joinsInProgress.incrementAndGet();
                    evaluateTupleExpr(parent, join.getRightArg(), bs);
                    return true;
                }
            }
        }, join.getLeftArg(), bindings);
    }

    /**
     * Evaluate {@link LeftJoin} query model nodes
     * @param parentPipe
     * @param leftJoin
     * @param bindings
     */
    private void evaluateLeftJoin(BindingSetPipe parentPipe, final LeftJoin leftJoin, final BindingSet bindings) {
        // Check whether optional join is "well designed" as defined in section
        // 4.2 of "Semantics and Complexity of SPARQL", 2006, Jorge Pérez et al.
        VarNameCollector optionalVarCollector = new VarNameCollector();
        leftJoin.getRightArg().visit(optionalVarCollector);
        if (leftJoin.hasCondition()) {
            leftJoin.getCondition().visit(optionalVarCollector);
        }
        final Set<String> problemVars = optionalVarCollector.getVarNames();
        problemVars.removeAll(leftJoin.getLeftArg().getBindingNames());
        problemVars.retainAll(bindings.getBindingNames());
        final AtomicLong joinsInProgress = new AtomicLong(1);
        final Set<String> scopeBindingNames = leftJoin.getBindingNames();
        final BindingSetPipe topPipe = problemVars.isEmpty() ? parentPipe : new BindingSetPipe(parentPipe) {
            //Handle badly designed left join
            @Override
            public boolean push(BindingSet bs) throws InterruptedException {
                if (bs == null) {
                    return parent.push(null);
                }
		if (QueryResults.bindingSetsCompatible(bindings, bs)) {
                    // Make sure the provided problemVars are part of the returned results
                    // (necessary in case of e.g. LeftJoin and Union arguments)
                    QueryBindingSet extendedResult = null;
                    for (String problemVar : problemVars) {
                            if (!bs.hasBinding(problemVar)) {
                                    if (extendedResult == null) {
                                            extendedResult = new QueryBindingSet(bs);
                                    }
                                    extendedResult.addBinding(problemVar, bindings.getValue(problemVar));
                            }
                    }
                    if (extendedResult != null) {
                            bs = extendedResult;
                    }
                    return parent.push(bs);
		}
                return true;
            }
        };
        evaluateTupleExpr(new BindingSetPipe(topPipe) {
            AtomicBoolean leftInProgress = new AtomicBoolean(true);
            @Override
            public boolean push(final BindingSet leftBindings) throws InterruptedException {
                if (leftBindings == null) {
                    if (leftInProgress.getAndSet(false)) {
                        if (joinsInProgress.decrementAndGet() == 0) {
                            parent.push(null);
                        }
                    }
                } else {
                    joinsInProgress.incrementAndGet();
                    evaluateTupleExpr(new BindingSetPipe(topPipe) {
                        private boolean failed = true;
                        @Override
                        public boolean push(BindingSet rightBindings) throws InterruptedException {
                            if (rightBindings == null) {
                                if (failed && leftBindings.size() > 0) {
                                    // Join failed, return left arg's bindings
                                    parent.push(leftBindings);
                                }
                                if (joinsInProgress.decrementAndGet() == 0) {
                                    parent.push(null);
                                }
                                return false;
                            } else try {
                                if (leftJoin.getCondition() == null) {
                                    failed = false;
                                    return parent.push(rightBindings);
                                } else {
                                    // Limit the bindings to the ones that are in scope for
                                    // this filter
                                    QueryBindingSet scopeBindings = new QueryBindingSet(rightBindings);
                                    scopeBindings.retainAll(scopeBindingNames);
                                    if (parentStrategy.isTrue(leftJoin.getCondition(), scopeBindings)) {
                                        failed = false;
                                        return parent.push(rightBindings);
                                    }
                                }
                            } catch (ValueExprEvaluationException ignore) {
                            } catch (QueryEvaluationException e) {
                                parent.handleException(e);
                            }
                            return true;
                        }
                    }, leftJoin.getRightArg(), leftBindings);
                }
                return true;
            }
        }, leftJoin.getLeftArg(), problemVars.isEmpty() ? bindings : getFilteredBindings(bindings, problemVars));
    }

    private static QueryBindingSet getFilteredBindings(BindingSet bindings, Set<String> problemVars) {
            QueryBindingSet filteredBindings = new QueryBindingSet(bindings);
            filteredBindings.removeAll(problemVars);
            return filteredBindings;
    }

    /**
     * Evaluate {@link Union} query model nodes.
     * @param parent
     * @param union
     * @param bindings
     */
    private void evaluateUnion(BindingSetPipe parent, Union union, BindingSet bindings) {
        BindingSetPipe pipe = new BindingSetPipe(parent) {
            AtomicInteger args = new AtomicInteger(2);
            @Override
            public boolean push(BindingSet bs) throws InterruptedException {
                if (bs == null) {
                    if (args.decrementAndGet() == 0) {
                        return parent.push(null);
                    } else {
                        return false;
                    }
                } else {
                    return parent.push(bs);
                }
            }
        };
        evaluateTupleExpr(pipe, union.getLeftArg(), bindings);
        evaluateTupleExpr(pipe, union.getRightArg(), bindings);
    }

    /**
     * Evaluate {@link Intersection} query model nodes
     * @param topPipe
     * @param intersection
     * @param bindings
     */
    private void evaluateIntersection(final BindingSetPipe topPipe, final Intersection intersection, final BindingSet bindings) {
        evaluateTupleExpr(new BindingSetPipe(topPipe) {
            private final BigHashSet<BindingSet> secondSet = new BigHashSet<>();
            @Override
            protected void handleException(Exception e) {
                secondSet.close();
                super.handleException(e);
            }
            @Override
            public boolean push(BindingSet bs) throws InterruptedException {
                if (bs != null) try {
                    secondSet.add(bs);
                    return true;
                } catch (IOException e) {
                    handleException(e);
                    return false;
                } else {
                    evaluateTupleExpr(new BindingSetPipe(parent) {
                        @Override
                        public boolean push(BindingSet bs) throws InterruptedException {
                            try {
                                if (bs == null) {
                                    secondSet.close();
                                    return parent.push(null);
                                }
                                return secondSet.contains(bs) ? parent.push(bs) : true;
                            } catch (IOException e) {
                                super.handleException(e);
                                return false;
                            }
                        }
                    }, intersection.getLeftArg(), bindings);
                    return false;
                }
            }
        }, intersection.getRightArg(), bindings);
    }

    /**
     * Evaluate {@link Difference} query model nodes
     * @param topPipe
     * @param difference
     * @param bindings
     */
    private void evaluateDifference(final BindingSetPipe topPipe, final Difference difference, final BindingSet bindings) {
        evaluateTupleExpr(new BindingSetPipe(topPipe) {
            private final BigHashSet<BindingSet> excludeSet = new BigHashSet<>();
            @Override
            protected void handleException(Exception e) {
                excludeSet.close();
                super.handleException(e);
            }
            @Override
            public boolean push(BindingSet bs) throws InterruptedException {
                if (bs != null) try {
                    excludeSet.add(bs);
                    return true;
                } catch (IOException e) {
                    handleException(e);
                    return false;
                } else {
                    evaluateTupleExpr(new BindingSetPipe(topPipe) {
                        @Override
                        public boolean push(BindingSet bs) throws InterruptedException {
                            if (bs == null) {
                                excludeSet.close();
                                return parent.push(null);
                            }
                            for (BindingSet excluded : excludeSet) {
                                // build set of shared variable names
                                Set<String> sharedBindingNames = new HashSet<>(excluded.getBindingNames());
                                sharedBindingNames.retainAll(bs.getBindingNames());
                                // two bindingsets that share no variables are compatible by
                                // definition, however, the formal
                                // definition of SPARQL MINUS indicates that such disjoint sets should
                                // be filtered out.
                                // See http://www.w3.org/TR/sparql11-query/#sparqlAlgebra
                                if (!sharedBindingNames.isEmpty()) {
                                    if (QueryResults.bindingSetsCompatible(excluded, bs)) {
                                        // at least one compatible bindingset has been found in the
                                        // exclude set, therefore the object is compatible, therefore it
                                        // should not be accepted.
                                        return true;
                                    }
                                }
                            }
                            return parent.push(bs);
                        }
                    }, difference.getLeftArg(), bindings);
                }
                return false;
            }

        }, difference.getRightArg(), bindings);
    }

    /**
     * Evaluate {@link SingletonSet} query model nodes
     * @param parent
     * @param singletonSet
     * @param bindings
     */
    private void evaluateSingletonSet(BindingSetPipe parent, SingletonSet singletonSet, BindingSet bindings) {
        try {
            if (parent.push(bindings)) {
                parent.push(null);
            }
        } catch (InterruptedException e) {
            parent.handleException(e);
        }
    }

    /**
     * Evaluate {@link EmptySet} query model nodes
     * @param parent
     * @param emptySet
     * @param bindings
     */
    private void evaluateEmptySet(BindingSetPipe parent, EmptySet emptySet, BindingSet bindings) {
        try {
            parent.push(null);
        } catch (InterruptedException e) {
            parent.handleException(e);
        }
    }

    /**
     * Evaluate {@link ExternalSet} query model nodes
     * @param parent
     * @param externalSet
     * @param bindings
     */
    private void evaluateExternalSet(BindingSetPipe parent, ExternalSet externalSet, BindingSet bindings) {
        try {
            HalyardStatementPatternEvaluation.enqueue(parent, externalSet.evaluate(bindings), externalSet);
        } catch (QueryEvaluationException e) {
            parent.handleException(e);
        }
    }

    /**
     * Evaluate {@link ZeroLengthPath} query model nodes
     * @param parent
     * @param zlp
     * @param bindings
     */
    private void evaluateZeroLengthPath(BindingSetPipe parent, ZeroLengthPath zlp, BindingSet bindings) {
        final Var subjectVar = zlp.getSubjectVar();
        final Var objVar = zlp.getObjectVar();
        final Var contextVar = zlp.getContextVar();
        Value subj = subjectVar.getValue() == null ? bindings.getValue(subjectVar.getName()) : subjectVar.getValue();
        Value obj = objVar.getValue() == null ? bindings.getValue(objVar.getName()) : objVar.getValue();
        if (subj != null && obj != null) {
            if (!subj.equals(obj)) {
                try {
                    parent.push(null);
                } catch (InterruptedException e) {
                    parent.handleException(e);
                }
                return;
            }
        }
        //temporary solution using copy of the original iterator
        //re-writing this to push model is a bit more complex task
        HalyardStatementPatternEvaluation.enqueue(parent, new ZeroLengthPathIteration(parentStrategy, subjectVar, objVar, subj, obj, contextVar, bindings), zlp);
    }

    /**
     * Evaluate {@link ArbitraryLengthPath} query model nodes
     * @param parent
     * @param alp
     * @param bindings
     */
    private void evaluateArbitraryLengthPath(BindingSetPipe parent, ArbitraryLengthPath alp, BindingSet bindings) {
        final StatementPattern.Scope scope = alp.getScope();
        final Var subjectVar = alp.getSubjectVar();
        final TupleExpr pathExpression = alp.getPathExpression();
        final Var objVar = alp.getObjectVar();
        final Var contextVar = alp.getContextVar();
        final long minLength = alp.getMinLength();
        //temporary solution using copy of the original iterator
        //re-writing this to push model is a bit more complex task
        try {
            HalyardStatementPatternEvaluation.enqueue(parent, new PathIteration(new StrictEvaluationStrategy(null, null) {
                @Override
                public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(ZeroLengthPath zlp, BindingSet bindings) throws QueryEvaluationException {
                    return parentStrategy.evaluate(zlp, bindings);
                }

                @Override
                public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(TupleExpr expr, BindingSet bindings) throws QueryEvaluationException {
                    return parentStrategy.evaluate(expr, bindings);
                }

            }, scope, subjectVar, pathExpression, objVar, contextVar, minLength, bindings), alp);
        } catch (QueryEvaluationException e) {
            parent.handleException(e);
        }
    }

    /**
     * Evaluate {@link BindingSetAssignment} query model nodes
     * @param parent
     * @param bsa
     * @param bindings
     */
    private void evaluateBindingSetAssignment(BindingSetPipe parent, BindingSetAssignment bsa, BindingSet bindings) {
        final Iterator<BindingSet> iter = bsa.getBindingSets().iterator();
        if (bindings.size() == 0) { // empty binding set
            HalyardStatementPatternEvaluation.enqueue(parent, new CloseableIteratorIteration<>(iter), bsa);
        } else {
            final QueryBindingSet b = new QueryBindingSet(bindings);
            HalyardStatementPatternEvaluation.enqueue(parent, new LookAheadIteration<BindingSet, QueryEvaluationException>() {
                @Override
                protected BindingSet getNextElement() throws QueryEvaluationException {
                    QueryBindingSet result = null;
                    while (result == null && iter.hasNext()) {
                        final BindingSet assignedBindings = iter.next();
                        for (String name : assignedBindings.getBindingNames()) {
                            final Value assignedValue = assignedBindings.getValue(name);
                            if (assignedValue != null) { // can be null if set to UNDEF
                                // check that the binding assignment does not overwrite
                                // existing bindings.
                                Value bValue = b.getValue(name);
                                if (bValue == null || assignedValue.equals(bValue)) {
                                    if (result == null) {
                                        result = new QueryBindingSet(b);
                                    }
                                    if (bValue == null) {
                                        // we are not overwriting an existing binding.
                                        result.addBinding(name, assignedValue);
                                    }
                                } else {
                                    // if values are not equal there is no compatible
                                    // merge and we should return no next element.
                                    result = null;
                                    break;
                                }
                            }
                        }
                    }
                    return result;
                }
            }, bsa);
        }
    }

    /**
     * Returns the limit of the current variable bindings before any further projection.
     */
    private static long getLimit(QueryModelNode node) {
        long offset = 0;
        if (node instanceof Slice) {
            Slice slice = (Slice) node;
            if (slice.hasOffset() && slice.hasLimit()) {
                return slice.getOffset() + slice.getLimit();
            } else if (slice.hasLimit()) {
                return slice.getLimit();
            } else if (slice.hasOffset()) {
                offset = slice.getOffset();
            }
        }
        QueryModelNode parent = node.getParentNode();
        if (parent instanceof Distinct || parent instanceof Reduced || parent instanceof Slice) {
            long limit = getLimit(parent);
            if (offset > 0L && limit < Long.MAX_VALUE) {
                return offset + limit;
            } else {
                return limit;
            }
        }
        return Long.MAX_VALUE;
    }

    /**
     * Determines if the parent of the node is an instance of {@link Distinct} or {@link Reduced}.
     * @param node the {@link QueryModelNode} to test
     * @return {@code true} if the parent is and instance of {@link Distinct} or {@link Reduced} and {@code false} otherwise. If the parent is
     * an instance of {@link Slice} then the parent is considered to be the first non-{@code Slice} node up the tree.
     */
    private static boolean isReducedOrDistinct(QueryModelNode node) {
        QueryModelNode parent = node.getParentNode();
        if (parent instanceof Slice) {
            return isReducedOrDistinct(parent);
        }
        return parent instanceof Distinct || parent instanceof Reduced;
    }

    /**
     * Determines if a {@link QueryModelNode} is a {@link SubQueryValueOperator} or if it's parent node is
     * @param node
     * @return
     */
    private boolean isPartOfSubQuery(QueryModelNode node) {
        if (node instanceof SubQueryValueOperator) {
            return true;
        }
        QueryModelNode parent = node.getParentNode();
        if (parent == null) {
            return false;
        } else {
            return isPartOfSubQuery(parent);
        }
    }

    private static final BindingSet NULL = new EmptyBindingSet();

    private final class BindingSetPipeIterator extends LookAheadIteration<BindingSet, QueryEvaluationException> {

        private final LinkedBlockingQueue<BindingSet> queue = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);
        private Exception exception = null;

        private final BindingSetPipe pipe = new BindingSetPipe(null) {
            @Override
            public boolean push(BindingSet bs) throws InterruptedException {
                if (isClosed()) return false;
                queue.put(bs == null ? NULL : bs);
                return bs != null;
            }

            @Override
            protected void handleException(Exception e) {
                if (exception != null) e.addSuppressed(exception);
                exception = e;
                try {
                    handleClose();
                } catch (QueryEvaluationException ex) {
                    exception.addSuppressed(ex);
                }
            }

            @Override
            protected boolean isClosed() {
                return exception != null || BindingSetPipeIterator.super.isClosed();
            }
        };



        @Override
        protected BindingSet getNextElement() throws QueryEvaluationException {
            try {
                while (true) {
                    BindingSet bs = queue.poll(1, TimeUnit.SECONDS);
                    if (exception != null) throw new QueryEvaluationException(exception);
                    if (timeout > 0 && System.currentTimeMillis() - startTime > 1000l * timeout) throw new QueryEvaluationException("Query evaluation exceeded specified timeout " + timeout + "s");
                    if (bs != null) {
                        return bs == NULL ? null : bs;
                    }
                }
            } catch (InterruptedException ex) {
                throw new QueryEvaluationException(ex);
            }
        }

        @Override
        protected void handleClose() throws QueryEvaluationException {
            super.handleClose();
            queue.clear();
            queue.add(NULL);
        }
    }
}
