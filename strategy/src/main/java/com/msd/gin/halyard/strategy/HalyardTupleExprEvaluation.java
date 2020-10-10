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

import static com.msd.gin.halyard.strategy.HalyardEvaluationExecutor.pullAndPush;
import static com.msd.gin.halyard.strategy.HalyardEvaluationExecutor.pullAndPushAsync;

import com.msd.gin.halyard.algebra.StarJoin;
import com.msd.gin.halyard.common.Timestamped;
import com.msd.gin.halyard.strategy.aggregators.Aggregator;
import com.msd.gin.halyard.strategy.aggregators.AvgAggregator;
import com.msd.gin.halyard.strategy.aggregators.ConcatAggregator;
import com.msd.gin.halyard.strategy.aggregators.CountAggregator;
import com.msd.gin.halyard.strategy.aggregators.MaxAggregator;
import com.msd.gin.halyard.strategy.aggregators.MinAggregator;
import com.msd.gin.halyard.strategy.aggregators.SampleAggregator;
import com.msd.gin.halyard.strategy.aggregators.SumAggregator;
import com.msd.gin.halyard.strategy.collections.BigHashSet;
import com.msd.gin.halyard.strategy.collections.Sorter;
import com.msd.gin.halyard.vocab.HALYARD;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.CloseableIteratorIteration;
import org.eclipse.rdf4j.common.iteration.ConvertingIteration;
import org.eclipse.rdf4j.common.iteration.FilterIteration;
import org.eclipse.rdf4j.common.iteration.LookAheadIteration;
import org.eclipse.rdf4j.common.iteration.SilentIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF4J;
import org.eclipse.rdf4j.model.vocabulary.SESAME;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.query.algebra.AggregateOperator;
import org.eclipse.rdf4j.query.algebra.ArbitraryLengthPath;
import org.eclipse.rdf4j.query.algebra.Avg;
import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.Count;
import org.eclipse.rdf4j.query.algebra.DescribeOperator;
import org.eclipse.rdf4j.query.algebra.Difference;
import org.eclipse.rdf4j.query.algebra.Distinct;
import org.eclipse.rdf4j.query.algebra.EmptySet;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.ExtensionElem;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Group;
import org.eclipse.rdf4j.query.algebra.GroupConcat;
import org.eclipse.rdf4j.query.algebra.GroupElem;
import org.eclipse.rdf4j.query.algebra.Intersection;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.Max;
import org.eclipse.rdf4j.query.algebra.Min;
import org.eclipse.rdf4j.query.algebra.MultiProjection;
import org.eclipse.rdf4j.query.algebra.Order;
import org.eclipse.rdf4j.query.algebra.OrderElem;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.ProjectionElem;
import org.eclipse.rdf4j.query.algebra.ProjectionElemList;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.Reduced;
import org.eclipse.rdf4j.query.algebra.Sample;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.Slice;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.SubQueryValueOperator;
import org.eclipse.rdf4j.query.algebra.Sum;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.TupleFunctionCall;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.ZeroLengthPath;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryContext;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ExternalSet;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.TupleFunctionEvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.DescribeIteration;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.GroupIterator;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.HashJoinIteration;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.PathIteration;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.ProjectionIterator;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.QueryContextIteration;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.ZeroLengthPathIteration;
import org.eclipse.rdf4j.query.algebra.evaluation.util.ValueComparator;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.TupleExprs;
import org.eclipse.rdf4j.query.algebra.helpers.VarNameCollector;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.query.impl.MapBindingSet;

import net.sf.saxon.expr.flwor.TupleExpression;

/**
 * Evaluates {@link TupleExpression}s and it's sub-interfaces and implementations.
 * @author Adam Sotona (MSD)
 */
final class HalyardTupleExprEvaluation {

    private final HalyardEvaluationStrategy parentStrategy;
	private final TupleFunctionRegistry tupleFunctionRegistry;
	private final TripleSource tripleSource;
	private final QueryContext queryContext;
    private final Dataset dataset;

    /**
	 * Constructor used by {@link HalyardEvaluationStrategy} to create this helper class
	 * 
	 * @param parentStrategy
	 * @param queryContext
	 * @param tupleFunctionRegistry
	 * @param tripleSource
	 * @param dataset
	 * @param timeoutSecs seconds
	 */
	HalyardTupleExprEvaluation(HalyardEvaluationStrategy parentStrategy, QueryContext queryContext,
			TupleFunctionRegistry tupleFunctionRegistry, TripleSource tripleSource, Dataset dataset) {
        this.parentStrategy = parentStrategy;
		this.queryContext = queryContext;
		this.tupleFunctionRegistry = tupleFunctionRegistry;
		this.tripleSource = tripleSource;
		this.dataset = dataset;
    }

    /**
     * Returns an iterator on the binding set pipe
     * @param expr supplied by HalyardEvaluationStrategy
     * @param bindings supplied by HalyardEvaluationStrategy
     * @return an iterator on the binding set pipe
     */
    CloseableIteration<BindingSet, QueryEvaluationException> evaluate(TupleExpr expr, BindingSet bindings) {
    	return HalyardEvaluationExecutor.consumeAndQueue(pipe -> evaluateTupleExpr(pipe, expr, bindings), expr);
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
            evaluateStatementPattern(parent, (StatementPattern) expr, bindings);
        } else if (expr instanceof UnaryTupleOperator) {
            evaluateUnaryTupleOperator(parent, (UnaryTupleOperator) expr, bindings);
        } else if (expr instanceof BinaryTupleOperator) {
            evaluateBinaryTupleOperator(parent, (BinaryTupleOperator) expr, bindings);
        } else if (expr instanceof StarJoin) {
        	evaluateStarJoin(parent, (StarJoin) expr, bindings);
        } else if (expr instanceof SingletonSet) {
            evaluateSingletonSet(parent, (SingletonSet) expr, bindings);
        } else if (expr instanceof EmptySet) {
            evaluateEmptySet(parent, (EmptySet) expr, bindings);
        } else if (expr instanceof ZeroLengthPath) {
            evaluateZeroLengthPath(parent, (ZeroLengthPath) expr, bindings);
        } else if (expr instanceof ArbitraryLengthPath) {
            evaluateArbitraryLengthPath(parent, (ArbitraryLengthPath) expr, bindings);
        } else if (expr instanceof BindingSetAssignment) {
            evaluateBindingSetAssignment(parent, (BindingSetAssignment) expr, bindings);
		} else if (expr instanceof TupleFunctionCall) {
			evaluateTupleFunctionCall(parent, (TupleFunctionCall) expr, bindings);
        } else if (expr instanceof ExternalSet) {
            evaluateExternalSet(parent, (ExternalSet) expr, bindings);
        } else if (expr == null) {
            parent.handleException(new IllegalArgumentException("expr must not be null"));
        } else {
            parent.handleException(new QueryEvaluationException("Unsupported tuple expr type: " + expr.getClass()));
        }
    }

    /**
     * Evaluate the statement pattern using the supplied bindings
     * @param parent to push or enqueue evaluation results
     * @param sp the {@code StatementPattern} to evaluate
     * @param bindings the set of names to which values are bound. For example, select ?s, ?p, ?o has the names s, p and o and the values bound to them are the
     * results of the evaluation of this statement pattern
     */
    private void evaluateStatementPattern(final BindingSetPipe parent, final StatementPattern sp, final BindingSet bindings) {
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
                parent.close();
                return;
            }
            try {
                final Set<IRI> graphs;
                final boolean emptyGraph;
                if (dataset != null) {
                    if (sp.getScope() == StatementPattern.Scope.DEFAULT_CONTEXTS) { //evaluate against the default graph(s)
                        graphs = dataset.getDefaultGraphs();
                        emptyGraph = graphs.isEmpty() && !dataset.getNamedGraphs().isEmpty();
                    } else { //evaluate against the named graphs
                        graphs = dataset.getNamedGraphs();
                        emptyGraph = graphs.isEmpty() && !dataset.getDefaultGraphs().isEmpty();
                    }
                } else {
                	graphs = null;
                	emptyGraph = false;
                }

                final Resource[] contexts;
                if (emptyGraph) {
                    // Search zero contexts
                    parent.close(); //no results from this statement pattern
                    return;
                } else if (graphs == null || graphs.isEmpty()) {
                    // store default behaivour
                    if (contextValue != null) {
						if (RDF4J.NIL.equals(contextValue) || SESAME.NIL.equals(contextValue)) {
							contexts = new Resource[] { (Resource) null };
						} else {
							contexts = new Resource[] { (Resource) contextValue };
						}
                    }
					/*
					 * TODO activate this to have an exclusive (rather than inclusive) interpretation of the default
					 * graph in SPARQL querying. else if (statementPattern.getScope() == Scope.DEFAULT_CONTEXTS ) {
					 * contexts = new Resource[] { (Resource)null }; }
					 */
					else {
						contexts = new Resource[0];
					}
                } else if (contextValue != null) {
                    if (graphs.contains(contextValue)) {
                        contexts = new Resource[]{(Resource) contextValue};
                    } else {
                        // Statement pattern specifies a context that is not part of
                        // the dataset
                        parent.close(); //no results possible because the context is not available
                        return;
                    }
                } else {
                    contexts = new Resource[graphs.size()];
                    int i = 0;
                    for (IRI graph : graphs) {
                        IRI context;
						if (RDF4J.NIL.equals(graph) || SESAME.NIL.equals(graph)) {
                            context = null;
                        } else {
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
                }
            } catch (ClassCastException e) {
                // Invalid value type for subject, predicate and/or context
                parent.close();
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
        pullAndPushAsync(parent, new ConvertingIteration<Statement, BindingSet, QueryEvaluationException>(stIter) {

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

    private boolean isUnbound(Var var, BindingSet bindings) {
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
        if (projection.isSubquery() && bindings.size() > 0) {
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
            protected boolean next(BindingSet bs) throws InterruptedException {
                return parent.push(ProjectionIterator.project(projection.getProjectionElemList(), bs, bindings, includeAll));
            }
            @Override
            public String toString() {
            	return "ProjectionBindingSetPipe";
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
        evaluateTupleExpr(new BindingSetPipe(parent) {
            final List<ProjectionElemList> projections = multiProjection.getProjections();
            final BindingSet prev[] = new BindingSet[projections.size()];

            @Override
            protected boolean next(BindingSet bs) throws InterruptedException {
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
            @Override
            public String toString() {
            	return "MultiProjectionBindingSetPipe";
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
        evaluateTupleExpr(new BindingSetPipe(parent) {
            final Set<String> scopeBindingNames = filter.getBindingNames();

            @Override
            protected boolean next(BindingSet bs) throws InterruptedException {
                try {
                    if (accept(bs)) {
                        return parent.push(bs); //push results that pass the filter.
                    } else {
                        return true; //nothing passes the filter but processing continues.
                    }
                } catch (QueryEvaluationException e) {
                    return handleException(e);
                }
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
            @Override
            public String toString() {
            	return "FilterBindingSetPipe";
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
		pullAndPushAsync(parent, new DescribeIteration(evaluate(operator.getArg(), bindings), parentStrategy,
				operator.getBindingNames(), bindings), operator);
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
            evaluateTupleExpr(new BindingSetPipe(parent) {
                final AtomicLong minorOrder = new AtomicLong();

                @Override
                protected boolean handleException(Exception e) {
                    sorter.close();
                    return parent.handleException(e);
                }

                @Override
                protected boolean next(BindingSet bs) throws InterruptedException {
                    try {
                        ComparableBindingSetWrapper cbsw = new ComparableBindingSetWrapper(parentStrategy, bs, order.getElements(), minorOrder.getAndIncrement());
                        sorter.add(cbsw);
                        return true;
                    } catch (QueryEvaluationException | IOException e) {
                        return handleException(e);
                    }
                }

                @Override
                public void close() throws InterruptedException {
                    try {
                        for (Map.Entry<ComparableBindingSetWrapper, Long> me : sorter) {
                            for (long i = me.getValue(); i > 0; i--) {
                                if (!parent.push(me.getKey().bs)) {
                                    return;
                                }
                            }
                        }
                        parent.close();
                    } finally {
                        sorter.close();
                    }
                }

                @Override
                public String toString() {
                	return "OrderBindingSetPipe";
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
    	if (group.getGroupBindingNames().isEmpty()) {
    		// no GROUP BY present
			final Map<String,Aggregator> aggregators = new LinkedHashMap<>();
			for (GroupElem ge : group.getGroupElements()) {
				Aggregator agg = createAggregator(ge.getOperator(), bindings);
				if (agg != null) {
					aggregators.put(ge.getName(), agg);
				}
			}
    		evaluateTupleExpr(new BindingSetPipe(parent) {
				@Override
				protected boolean next(BindingSet bs) throws InterruptedException {
					for(Aggregator agg : aggregators.values()) {
						agg.process(bs);
					}
					return true;
				}
				@Override
				public void close() throws InterruptedException {
					QueryBindingSet result = new QueryBindingSet();
					for(Map.Entry<String,Aggregator> entry : aggregators.entrySet()) {
						try(Aggregator agg = entry.getValue()) {
							Value v = agg.getValue();
							if (v != null) {
								result.setBinding(entry.getKey(), v);
							}
						} catch (ValueExprEvaluationException ignore) {
							// There was a type error when calculating the value of the
							// aggregate.
							// We silently ignore the error, resulting in no result value
							// being bound.
						}
					}
					parent.pushLast(result);
				}
				@Override
				public String toString() {
					return "AggregateBindingSetPipe";
				}
    		}, group.getArg(), bindings);
    	} else {
	        //temporary solution using copy of the original iterator
	        //re-writing this to push model is a bit more complex task
	        try {
				pullAndPushAsync(parent, new GroupIterator(parentStrategy, group, bindings), group);
	        } catch (QueryEvaluationException e) {
	            parent.handleException(e);
	        }
    	}
    }

    private Aggregator createAggregator(AggregateOperator operator, BindingSet bindings) {
		if (operator instanceof Count) {
			return new CountAggregator((Count) operator, parentStrategy, tripleSource.getValueFactory());
		} else if (operator instanceof Min) {
			return new MinAggregator((Min) operator, parentStrategy);
		} else if (operator instanceof Max) {
			return new MaxAggregator((Max) operator, parentStrategy);
		} else if (operator instanceof Sum) {
			return new SumAggregator((Sum) operator, parentStrategy);
		} else if (operator instanceof Avg) {
			return new AvgAggregator((Avg) operator, parentStrategy, tripleSource.getValueFactory());
		} else if (operator instanceof Sample) {
			return new SampleAggregator((Sample) operator, parentStrategy);
		} else if (operator instanceof GroupConcat) {
			return new ConcatAggregator((GroupConcat) operator, parentStrategy, bindings, tripleSource.getValueFactory());
		} else {
			return null;
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
            protected boolean next(BindingSet bs) throws InterruptedException {
                synchronized (this) {
                    if (bs.equals(previous)) {
                        return true;
                    }
                    previous = bs;
                }
                return parent.push(bs);
            }
            @Override
            public String toString() {
            	return "ReducedBindingSetPipe";
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
            private final BigHashSet<BindingSet> set = BigHashSet.create();
            @Override
            protected boolean handleException(Exception e) {
                set.close();
                return parent.handleException(e);
            }
            @Override
            protected boolean next(BindingSet bs) throws InterruptedException {
                try {
                    if (!set.add(bs)) {
                        return true;
                    }
                } catch (IOException e) {
                    return handleException(e);
                }
                return parent.push(bs);
            }
            @Override
            public void close() throws InterruptedException {
               	set.close();
                parent.close();
            }
            @Override
            public String toString() {
            	return "DistinctBindingSetPipe";
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
            protected boolean next(BindingSet bs) throws InterruptedException {
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
                            handleException(e);
                        }
                    }
                }
                return parent.push(targetBindings);
            }
            @Override
            public String toString() {
            	return "ExtensionBindingSetPipe";
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
            protected boolean next(BindingSet bs) throws InterruptedException {
                long l = ll.incrementAndGet();
                if (l <= offset) {
                    return true;
                } else if (l < limit) {
                    return parent.push(bs);
                } else if (l == limit) {
                    return parent.pushLast(bs);
                } else {
                	return false;
                }
            }
            @Override
            public String toString() {
            	return "SliceBindingSetPipe";
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
                    parent.close();
                    return;

                }
                // otherwise: perform a SELECT query
                CloseableIteration<BindingSet, QueryEvaluationException> result = fs.select(service, freeVars, bindings, baseUri);
				pullAndPushAsync(parent, service.isSilent() ? new SilentIteration(result) : result, service);
            } catch (QueryEvaluationException e) {
                // suppress exceptions if silent
                if (service.isSilent()) {
                    parent.pushLast(bindings);
                } else {
                    throw e;
                }
            } catch (RuntimeException e) {
                // suppress special exceptions (e.g. UndeclaredThrowable with
                // wrapped
                // QueryEval) if silent
                if (service.isSilent()) {
                    parent.pushLast(bindings);
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
    	if (isOutOfScopeForLeftArgBindings(join.getRightArg())) {
            //re-writing this to push model is a bit more complex task
            try {
    			pullAndPushAsync(topPipe, new HashJoinIteration(parentStrategy, join, bindings), join);
            } catch (QueryEvaluationException e) {
            	topPipe.handleException(e);
            }
            return;
    	}

        evaluateTupleExpr(new BindingSetPipe(topPipe) {
        	final AtomicLong joinsInProgress = new AtomicLong();
        	final AtomicBoolean joinsFinished = new AtomicBoolean();

            @Override
            protected boolean next(BindingSet bs) throws InterruptedException {
            	joinsInProgress.incrementAndGet();
                evaluateTupleExpr(new BindingSetPipe(parent) {
                    @Override
                    public void close() throws InterruptedException {
                    	joinsInProgress.decrementAndGet();
                    	if (joinsFinished.get() && joinsInProgress.compareAndSet(0L, -1L)) {
                    		parent.close();
                    	}
                    }
                    @Override
                    public String toString() {
                    	return "JoinBindingSetPipe(right)";
                    }
                }, join.getRightArg(), bs);
                return true;
            }
            @Override
            public void close() throws InterruptedException {
            	joinsFinished.set(true);
            	if(joinsInProgress.compareAndSet(0L, -1L)) {
            		parent.close();
            	}
            }
            @Override
            public String toString() {
            	return "JoinBindingSetPipe(left)";
            }
        }, join.getLeftArg(), bindings);
    }

    private boolean isOutOfScopeForLeftArgBindings(TupleExpr expr) {
		return (TupleExprs.isVariableScopeChange(expr) || TupleExprs.containsSubquery(expr));
	}

    /**
     * Evaluate {@link LeftJoin} query model nodes
     * @param parentPipe
     * @param leftJoin
     * @param bindings
     */
    private void evaluateLeftJoin(BindingSetPipe parentPipe, final LeftJoin leftJoin, final BindingSet bindings) {
    	if (TupleExprs.containsSubquery(leftJoin.getRightArg())) {
            //re-writing this to push model is a bit more complex task
            try {
    			pullAndPushAsync(parentPipe, new HashJoinIteration(parentStrategy, leftJoin, bindings), leftJoin);
            } catch (QueryEvaluationException e) {
            	parentPipe.handleException(e);
            }
            return;
    	}

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
        final Set<String> scopeBindingNames = leftJoin.getBindingNames();
        final BindingSetPipe topPipe = problemVars.isEmpty() ? parentPipe : new BindingSetPipe(parentPipe) {
            //Handle badly designed left join
            @Override
            protected boolean next(BindingSet bs) throws InterruptedException {
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
            @Override
            public String toString() {
            	return "LeftJoinBindingSetPipe(top)";
            }
        };
        evaluateTupleExpr(new BindingSetPipe(topPipe) {
        	final AtomicLong joinsInProgress = new AtomicLong();
        	final AtomicBoolean joinsFinished = new AtomicBoolean();

        	@Override
            protected boolean next(final BindingSet leftBindings) throws InterruptedException {
            	joinsInProgress.incrementAndGet();
                evaluateTupleExpr(new BindingSetPipe(parent) {
                    private boolean failed = true;
                    @Override
                    protected boolean next(BindingSet rightBindings) throws InterruptedException {
                    	try {
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
                    @Override
                    public void close() throws InterruptedException {
                        if (failed) {
                            // Join failed, return left arg's bindings
                            parent.push(leftBindings);
                        }
                    	joinsInProgress.decrementAndGet();
                    	if (joinsFinished.get() && joinsInProgress.compareAndSet(0L, -1L)) {
                    		parent.close();
                    	}
                    }
                    @Override
                    public String toString() {
                    	return "LeftJoinBindingSetPipe(right)";
                    }
                }, leftJoin.getRightArg(), leftBindings);
                return true;
            }
            @Override
            public void close() throws InterruptedException {
            	joinsFinished.set(true);
            	if(joinsInProgress.compareAndSet(0L, -1L)) {
            		parent.close();
            	}
            }
            @Override
            public String toString() {
            	return "LeftJoinBindingSetPipe(left)";
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
            final AtomicInteger args = new AtomicInteger(2);
            @Override
            public void close() throws InterruptedException {
                if (args.decrementAndGet() == 0) {
                    parent.close();
                }
            }
            @Override
            public String toString() {
            	return "UnionBindingSetPipe";
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
            private final BigHashSet<BindingSet> secondSet = BigHashSet.create();
            @Override
            protected boolean handleException(Exception e) {
                secondSet.close();
                return parent.handleException(e);
            }
            @Override
            protected boolean next(BindingSet bs) throws InterruptedException {
                try {
                    secondSet.add(bs);
                    return true;
                } catch (IOException e) {
                    return handleException(e);
                }
            }
            @Override
            public void close() throws InterruptedException {
                evaluateTupleExpr(new BindingSetPipe(parent) {
                    @Override
                    protected boolean next(BindingSet bs) throws InterruptedException {
                        try {
                            return secondSet.contains(bs) ? parent.push(bs) : true;
                        } catch (IOException e) {
                            return parent.handleException(e);
                        }
                    }
                    @Override
                    public void close() throws InterruptedException {
                        secondSet.close();
                        parent.close();
                    }
                    @Override
                    public String toString() {
                    	return "IntersectionBindingSetPipe(left)";
                    }
                }, intersection.getLeftArg(), bindings);
            }
            @Override
            public String toString() {
            	return "IntersectionBindingSetPipe(right)";
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
            private final BigHashSet<BindingSet> excludeSet = BigHashSet.create();
            @Override
            protected boolean handleException(Exception e) {
                excludeSet.close();
                return parent.handleException(e);
            }
            @Override
            protected boolean next(BindingSet bs) throws InterruptedException {
                try {
                    excludeSet.add(bs);
                    return true;
                } catch (IOException e) {
                    return handleException(e);
                }
            }
            @Override
            public void close() throws InterruptedException {
                evaluateTupleExpr(new BindingSetPipe(topPipe) {
                    @Override
                    protected boolean next(BindingSet bs) throws InterruptedException {
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
                    @Override
                    public void close() throws InterruptedException {
                        excludeSet.close();
                        parent.close();
                    }
                    @Override
                    public String toString() {
                    	return "DifferenceBindingSetPipe(left)";
                    }
                }, difference.getLeftArg(), bindings);
            }
            @Override
            public String toString() {
            	return "DifferenceBindingSetPipe(right)";
            }
        }, difference.getRightArg(), bindings);
    }

    private void evaluateStarJoin(BindingSetPipe parent, StarJoin starJoin, BindingSet bindings) {
    	evaluateTupleExpr(parent, starJoin.toJoins(), bindings);
    }

    /**
     * Evaluate {@link SingletonSet} query model nodes
     * @param parent
     * @param singletonSet
     * @param bindings
     */
    private void evaluateSingletonSet(BindingSetPipe parent, SingletonSet singletonSet, BindingSet bindings) {
        try {
            parent.pushLast(bindings);
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
            parent.close();
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
			pullAndPushAsync(parent, externalSet.evaluate(bindings), externalSet);
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
                    parent.close();
                } catch (InterruptedException e) {
                    parent.handleException(e);
                }
                return;
            }
        }
        //temporary solution using copy of the original iterator
        //re-writing this to push model is a bit more complex task
		pullAndPushAsync(parent,
				new ZeroLengthPathIteration(parentStrategy, subjectVar, objVar, subj, obj, contextVar, bindings), zlp);
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
			pullAndPushAsync(parent, new PathIteration(new StrictEvaluationStrategy(null, null) {
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
			pullAndPush(parent, new CloseableIteratorIteration<>(iter));
        } else {
            final QueryBindingSet b = new QueryBindingSet(bindings);
			pullAndPush(parent, new LookAheadIteration<BindingSet, QueryEvaluationException>() {
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
            });
        }
    }

    /**
	 * Evaluate {@link TupleFunctionCall} query model nodes
	 * 
	 * @param parent
	 * @param tfc
	 * @param bindings
	 */
	private void evaluateTupleFunctionCall(BindingSetPipe parent, TupleFunctionCall tfc, BindingSet bindings)
			throws QueryEvaluationException {
		TupleFunction func = tupleFunctionRegistry.get(tfc.getURI())
				.orElseThrow(() -> new QueryEvaluationException("Unknown tuple function '" + tfc.getURI() + "'"));

		List<ValueExpr> args = tfc.getArgs();

		try {
			Value[] argValues = new Value[args.size()];
			for (int i = 0; i < args.size(); i++) {
				argValues[i] = parentStrategy.evaluate(args.get(i), bindings);
			}

			CloseableIteration<BindingSet, QueryEvaluationException> iter;
			queryContext.begin();
			try {
				iter = TupleFunctionEvaluationStrategy.evaluate(func, tfc.getResultVars(), bindings, tripleSource.getValueFactory(), argValues);
			} finally {
				queryContext.end();
			}
			pullAndPushAsync(parent, new QueryContextIteration(iter, queryContext), tfc);
		} catch (ValueExprEvaluationException veee) {
			// can't evaluate arguments
	        try {
	            parent.close();
	        } catch (InterruptedException ex) {
	            parent.handleException(ex);
	        }
		}
	}

	/**
	 * Returns the limit of the current variable bindings before any further
	 * projection.
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
    private static boolean isPartOfSubQuery(QueryModelNode node) {
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
}
