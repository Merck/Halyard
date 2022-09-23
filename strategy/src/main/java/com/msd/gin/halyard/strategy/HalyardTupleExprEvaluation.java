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

import com.msd.gin.halyard.algebra.Algebra;
import com.msd.gin.halyard.algebra.Algorithms;
import com.msd.gin.halyard.algebra.ConstrainedStatementPattern;
import com.msd.gin.halyard.algebra.ExtendedTupleFunctionCall;
import com.msd.gin.halyard.algebra.StarJoin;
import com.msd.gin.halyard.common.LiteralConstraint;
import com.msd.gin.halyard.common.ValueConstraint;
import com.msd.gin.halyard.common.ValueType;
import com.msd.gin.halyard.optimizers.JoinAlgorithmOptimizer;
import com.msd.gin.halyard.query.ConstrainedTripleSourceFactory;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.ConvertingIteration;
import org.eclipse.rdf4j.common.iteration.FilterIteration;
import org.eclipse.rdf4j.common.iteration.LookAheadIteration;
import org.eclipse.rdf4j.common.iteration.SilentIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.BooleanLiteral;
import org.eclipse.rdf4j.model.vocabulary.RDF;
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
import org.eclipse.rdf4j.query.algebra.Datatype;
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
import org.eclipse.rdf4j.query.algebra.IsNumeric;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.Lang;
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
import org.eclipse.rdf4j.query.algebra.StatementPattern.Scope;
import org.eclipse.rdf4j.query.algebra.SubQueryValueOperator;
import org.eclipse.rdf4j.query.algebra.Sum;
import org.eclipse.rdf4j.query.algebra.TripleRef;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.TupleFunctionCall;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.UnaryValueOperator;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.ZeroLengthPath;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryContext;
import org.eclipse.rdf4j.query.algebra.evaluation.RDFStarTripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.TupleFunctionEvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.DescribeIteration;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.PathIteration;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.ProjectionIterator;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.QueryContextIteration;
import org.eclipse.rdf4j.query.algebra.evaluation.util.ValueComparator;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.TupleExprs;
import org.eclipse.rdf4j.query.algebra.helpers.VarNameCollector;
import org.eclipse.rdf4j.query.impl.MapBindingSet;

/**
 * Evaluates {@link TupleExpr}s and it's sub-interfaces and implementations.
 * @author Adam Sotona (MSD)
 */
final class HalyardTupleExprEvaluation {
	private static final String ANON_SUBJECT_VAR = "__subj";
	private static final String ANON_PREDICATE_VAR = "__pred";
	private static final String ANON_OBJECT_VAR = "__obj";
	private static final int DEFAULT_HASH_JOIN_LIMIT = 10000;
	private static final int MAX_INITIAL_HASH_JOIN_TABLE_SIZE = 5000;

	private final HalyardEvaluationExecutor executor = HalyardEvaluationExecutor.INSTANCE;
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
     * Returns an iteration of binding sets.
     * @param expr supplied by HalyardEvaluationStrategy
     * @param bindings supplied by HalyardEvaluationStrategy
     * @return an iteration of binding sets
     */
    CloseableIteration<BindingSet, QueryEvaluationException> evaluate(TupleExpr expr, BindingSet bindings) {
    	return executor.pushAndPull(pipe -> evaluateTupleExpr(pipe, expr, bindings), expr, parentStrategy);
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
        } else if (expr instanceof TripleRef) {
        	evaluateTripleRef(parent, (TripleRef) expr, bindings);
		} else if (expr instanceof TupleFunctionCall) {
			// all TupleFunctionCalls are expected to be ExtendedTupleFunctionCalls
			evaluateTupleFunctionCall(parent, (ExtendedTupleFunctionCall) expr, bindings);
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
    	TripleSource ts = null;
    	if ((sp instanceof ConstrainedStatementPattern) && (tripleSource instanceof ConstrainedTripleSourceFactory)) {
    		ConstrainedStatementPattern csp = (ConstrainedStatementPattern) sp;
    		ValueConstraint subjConstraint = null;
    		if (csp.getSubjectType() != null) {
    			subjConstraint = new ValueConstraint(csp.getSubjectType());
    		}
    		ValueConstraint objConstraint = null;
    		if (csp.getObjectType() != null) {
				if (csp.getObjectType() == ValueType.LITERAL) {
					UnaryValueOperator constraintFunc = csp.getLiteralConstraintFunction();
					if (constraintFunc != null) {
						ValueExpr constraintValue = csp.getLiteralConstraintValue();
						Value v;
						if (constraintValue instanceof ValueConstant) {
							v = ((ValueConstant) constraintValue).getValue();
						} else if (constraintValue instanceof Var) {
							v = ((Var) constraintValue).getValue();
						} else {
							v = null;
						}
						if (v != null) {
							if (constraintFunc instanceof Datatype) {
								if (!v.isIRI()) {
									parent.empty();
									return;
								}
								IRI dt = (IRI) v;
				    			objConstraint = new LiteralConstraint(dt);
							} else if (constraintFunc instanceof Lang) {
								if (!v.isLiteral()) {
									parent.empty();
									return;
								}
								Literal langValue = (Literal) v;
								String lang = langValue.getLabel();
								if (!lang.isEmpty()) {
					    			objConstraint = new LiteralConstraint(lang);
								} else {
					    			objConstraint = new ValueConstraint(ValueType.LITERAL);
								}
							} else if ((constraintFunc instanceof IsNumeric) && BooleanLiteral.TRUE.equals(v)) {
				    			objConstraint = new LiteralConstraint(HALYARD.ANY_NUMERIC_TYPE);
							}
						}
	    			} else {
		    			objConstraint = new ValueConstraint(ValueType.LITERAL);
	    			}
	    		} else {
	    			objConstraint = new ValueConstraint(csp.getObjectType());
	    		}
    		}
			ts = ((ConstrainedTripleSourceFactory)tripleSource).getTripleSource(subjConstraint, objConstraint);
    	}
		if (ts == null) {
			ts = tripleSource;
		}
    	evaluateStatementPattern(parent, sp, bindings, ts);
    }

    private void evaluateStatementPattern(final BindingSetPipe parent, final StatementPattern sp, final BindingSet bindings, TripleSource tripleSource) {
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
	            parent.empty();
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
	                parent.empty(); //no results from this statement pattern
	                return;
	            } else if (graphs == null || graphs.isEmpty()) {
	                // store default behaviour
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
	                    parent.empty(); //no results possible because the context is not available
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
	                // the store and filtering out the statements that do not have a context.
	                stIter = new FilterIteration<Statement, QueryEvaluationException>(stIter) {
	
	                    @Override
	                    protected boolean accept(Statement st) {
	                        return st.getContext() != null;
	                    }
	
	                }; // end anonymous class
	            }
	        } catch (ClassCastException e) {
	            // Invalid value type for subject, predicate and/or context
	            parent.empty();
	            return;
	        }
        } catch (QueryEvaluationException e) {
            parent.handleException(e);
            return;
        }

        // The same variable might have been used multiple times in this
        // StatementPattern, verify value equality in those cases.
        int distinctVarCount = sp.getBindingNames().size();
        boolean allVarsDistinct = (conVar != null && distinctVarCount == 4) || (conVar == null && distinctVarCount == 3);
        if (!allVarsDistinct) {
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
        }

        // Return an iterator that converts the RDF statements (triples) to var bindings
        executor.pullAndPushAsync(parent, new ConvertingIteration<Statement, BindingSet, QueryEvaluationException>(stIter) {

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
                    if (!result.hasBinding(objVar.getName()) || ((val instanceof Literal) && HALYARD.SEARCH.equals(((Literal)val).getDatatype()))) {
                        result.setBinding(objVar.getName(), st.getObject());
                    }
                }
                if (conVar != null && !conVar.isConstant() && !result.hasBinding(conVar.getName())
                        && st.getContext() != null) {
                    result.addBinding(conVar.getName(), st.getContext());
                }

                return result;
            }
		}, sp, parentStrategy);
    }

    private boolean isUnbound(Var var, BindingSet bindings) {
        if (var == null) {
            return false;
        } else {
            return bindings.hasBinding(var.getName()) && bindings.getValue(var.getName()) == null;
        }
    }

	/**
	 * evaluates a TripleRef node returning bindingsets from the matched Triple nodes in the dataset (or explore
	 * standard reification)
	 */
	private void evaluateTripleRef(BindingSetPipe parent, TripleRef ref, BindingSet bindings) {
		// Naive implementation that walks over all statements matching (x rdf:type rdf:Statement)
		// and filter those that do not match the bindings for subject, predicate and object vars (if bound)
		final org.eclipse.rdf4j.query.algebra.Var subjVar = ref.getSubjectVar();
		final org.eclipse.rdf4j.query.algebra.Var predVar = ref.getPredicateVar();
		final org.eclipse.rdf4j.query.algebra.Var objVar = ref.getObjectVar();
		final org.eclipse.rdf4j.query.algebra.Var extVar = ref.getExprVar();

		final Value subjValue = getVarValue(subjVar, bindings);
		final Value predValue = getVarValue(predVar, bindings);
		final Value objValue = getVarValue(objVar, bindings);
		final Value extValue = getVarValue(extVar, bindings);

		// case1: when we have a binding for extVar we use it in the reified nodes lookup
		// case2: in which we have unbound extVar
		// in both cases:
		// 1. iterate over all statements matching ((* | extValue), rdf:type, rdf:Statement)
		// 2. construct a look ahead iteration and filter these solutions that do not match the
		// bindings for the subject, predicate and object vars (if these are bound)
		// return set of solution where the values of the statements (extVar, rdf:subject/predicate/object, value)
		// are bound to the variables of the respective TripleRef variables for subject, predicate, object
		// NOTE: if the tripleSource is extended to allow for lookup over asserted Triple values in the underlying sail
		// the evaluation of the TripleRef should be suitably forwarded down the sail and filter/construct
		// the correct solution out of the results of that call
		if (extValue != null && !(extValue instanceof Resource)) {
			parent.empty();
			return;
		}

		// whether the TripleSouce support access to RDF star
		final boolean sourceSupportsRdfStar = tripleSource instanceof RDFStarTripleSource;

		final CloseableIteration<BindingSet, QueryEvaluationException> results;
		// in case the
		if (sourceSupportsRdfStar) {
			CloseableIteration<? extends Triple, QueryEvaluationException> sourceIter = ((RDFStarTripleSource) tripleSource)
					.getRdfStarTriples((Resource) subjValue, (IRI) predValue, objValue);

			FilterIteration<Triple, QueryEvaluationException> filterIter = new FilterIteration<Triple, QueryEvaluationException>(
					sourceIter) {
				@Override
				protected boolean accept(Triple triple) throws QueryEvaluationException {
					if (subjValue != null && !subjValue.equals(triple.getSubject())) {
						return false;
					}
					if (predValue != null && !predValue.equals(triple.getPredicate())) {
						return false;
					}
					if (objValue != null && !objValue.equals(triple.getObject())) {
						return false;
					}
					if (extValue != null && !extValue.equals(triple)) {
						return false;
					}
					return true;
				}
			};

			results = new ConvertingIteration<Triple, BindingSet, QueryEvaluationException>(filterIter) {
				@Override
				protected BindingSet convert(Triple triple) {
					QueryBindingSet result = new QueryBindingSet(bindings);
					if (subjValue == null) {
						result.addBinding(subjVar.getName(), triple.getSubject());
					}
					if (predValue == null) {
						result.addBinding(predVar.getName(), triple.getPredicate());
					}
					if (objValue == null) {
						result.addBinding(objVar.getName(), triple.getObject());
					}
					// add the extVar binding if we do not have a value bound.
					if (extValue == null) {
						result.addBinding(extVar.getName(), triple);
					}
					return result;
				}
			};
		} else {
			// standard reification iteration
			// 1. walk over resources used as subjects of (x rdf:type rdf:Statement)
			final CloseableIteration<? extends Resource, QueryEvaluationException> iter = new ConvertingIteration<Statement, Resource, QueryEvaluationException>(
					tripleSource.getStatements((Resource) extValue, RDF.TYPE, RDF.STATEMENT)) {

				@Override
				protected Resource convert(Statement sourceObject) {
					return sourceObject.getSubject();
				}
			};
			// for each reification node, fetch and check the subject, predicate and object values against
			// the expected values from TripleRef pattern and supplied bindings collection
			results = new LookAheadIteration<BindingSet, QueryEvaluationException>() {
				@Override
				protected void handleClose()
						throws QueryEvaluationException {
					super.handleClose();
					iter.close();
				}

				@Override
				protected BindingSet getNextElement()
						throws QueryEvaluationException {
					while (iter.hasNext()) {
						Resource theNode = iter.next();
						QueryBindingSet result = new QueryBindingSet(bindings);
						// does it match the subjectValue/subjVar
						if (!matchValue(theNode, subjValue, subjVar, result, RDF.SUBJECT)) {
							continue;
						}
						// the predicate, if not, remove the binding that hass been added
						// when the subjValue has been checked and its value added to the solution
						if (!matchValue(theNode, predValue, predVar, result, RDF.PREDICATE)) {
							continue;
						}
						// check the object, if it do not match
						// remove the bindings added for subj and pred
						if (!matchValue(theNode, objValue, objVar, result, RDF.OBJECT)) {
							continue;
						}
						// add the extVar binding if we do not have a value bound.
						if (extValue == null) {
							result.addBinding(extVar.getName(), theNode);
						} else if (!extValue.equals(theNode)) {
							// the extVar value do not match theNode
							continue;
						}
						return result;
					}
					return null;
				}

				private boolean matchValue(Resource theNode, Value value, Var var, QueryBindingSet result,
						IRI predicate) {
					try (CloseableIteration<? extends Statement, QueryEvaluationException> valueiter = tripleSource
							.getStatements(theNode, predicate, null)) {
						while (valueiter.hasNext()) {
							Statement valueStatement = valueiter.next();
							if (theNode.equals(valueStatement.getSubject())) {
								if (value == null || value.equals(valueStatement.getObject())) {
									if (value == null) {
										result.addBinding(var.getName(), valueStatement.getObject());
									}
									return true;
								}
							}
						}
						return false;
					}
				}

			};
		} // else standard reification iteration
		executor.pullAndPushAsync(parent, results, ref, parentStrategy);
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
                        if (!parent.push(nb)) {
                            return false;
                        }
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
        parentStrategy.initTracking(filter);
        evaluateTupleExpr(new BindingSetPipe(parent) {
            final Set<String> scopeBindingNames = filter.getBindingNames();

            @Override
            protected boolean next(BindingSet bs) throws InterruptedException {
                try {
                    if (accept(bs)) {
                        parentStrategy.incrementResultSizeActual(filter);
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
    	executor.pullAndPushAsync(parent, new DescribeIteration(evaluate(operator.getArg(), bindings), parentStrategy,
				operator.getBindingNames(), bindings), operator, parentStrategy);
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
        final Sorter<ComparableBindingSetWrapper> sorter = new Sorter<>(getLimit(order), isReducedOrDistinct(order));
        evaluateTupleExpr(new BindingSetPipe(parent) {
            final AtomicLong minorOrder = new AtomicLong();

            @Override
            protected boolean handleException(Throwable e) {
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
    }

    /**
     * Evaluate {@link Group} query model nodes
     * @param parent
     * @param group
     * @param bindings
     */
    private void evaluateGroup(BindingSetPipe parent, Group group, BindingSet bindings) {
    	parentStrategy.initTracking(group);
    	if (group.getGroupBindingNames().isEmpty()) {
    		// no GROUP BY present
			final GroupValue aggregators = createGroupValue(group, bindings);
    		evaluateTupleExpr(new BindingSetPipe(parent) {
				@Override
				protected boolean next(BindingSet bs) throws InterruptedException {
					aggregators.addValues(bs);
					return true;
				}
				@Override
				public void close() throws InterruptedException {
					QueryBindingSet result = new QueryBindingSet(bindings);
					aggregators.bindResult(result);
					parentStrategy.incrementResultSizeActual(group);
					parent.pushLast(result);
				}
				@Override
				public String toString() {
					return "AggregateBindingSetPipe-noGroupBy";
				}
    		}, group.getArg(), bindings);
    	} else {
			final Map<GroupKey,GroupValue> groupByMap = new ConcurrentHashMap<>();
    		evaluateTupleExpr(new BindingSetPipe(parent) {
				@Override
				protected boolean next(BindingSet bs) throws InterruptedException {
					GroupValue aggregators = groupByMap.computeIfAbsent(new GroupKey(bs, group), k -> createGroupValue(group, bindings));
					aggregators.addValues(bs);
					return true;
				}
				@Override
				public void close() throws InterruptedException {
					if (!groupByMap.isEmpty()) {
						for(Map.Entry<GroupKey,GroupValue> aggEntry : groupByMap.entrySet()) {
							QueryBindingSet result = new QueryBindingSet(bindings);
							for (String name : group.getGroupBindingNames()) {
								Value v = aggEntry.getKey().bindingSet.getValue(name);
								if (v != null) {
									result.setBinding(name, v);
								}
							}
							aggEntry.getValue().bindResult(result);
							parentStrategy.incrementResultSizeActual(group);
							parent.push(result);
						}
					} else {
						QueryBindingSet result = new QueryBindingSet(bindings);
						for (GroupElem ge : group.getGroupElements()) {
							Aggregator agg = createAggregator(ge.getOperator(), bindings);
							if (agg != null) {
								Value v = agg.getValue();
								if (v != null) {
									result.setBinding(ge.getName(), v);
								}
							}
						}
						if (result.size() > 0) {
							parent.push(result);
						}
					}
					parent.close();
				}
				@Override
				public String toString() {
					return "AggregateBindingSetPipe";
				}
    		}, group.getArg(), bindings);
    	}
    }

    private GroupValue createGroupValue(Group group, BindingSet parentBindings) {
		Map<String,Aggregator> aggregators = new HashMap<>();
		for (GroupElem ge : group.getGroupElements()) {
			Aggregator agg = createAggregator(ge.getOperator(), parentBindings);
			if (agg != null) {
				aggregators.put(ge.getName(), agg);
			}
		}
		return new GroupValue(aggregators);
    }

    private static final class GroupKey implements Serializable {
		private static final long serialVersionUID = 2483566881092991496L;
		private final BindingSet bindingSet;
		private final Group group;
		private final int hash;

		GroupKey(BindingSet bindingSet, Group group) {
			this.bindingSet = bindingSet;
			this.group = group;

			int nextHash = 0;
			for (String name : group.getGroupBindingNames()) {
				Value value = bindingSet.getValue(name);
				if (value != null) {
					nextHash ^= value.hashCode();
				}
			}
			this.hash = nextHash;
		}

		@Override
		public int hashCode() {
			return hash;
		}

		@Override
		public boolean equals(Object other) {
			if (other instanceof GroupKey && other.hashCode() == hash) {
				BindingSet otherSolution = ((GroupKey) other).bindingSet;
				for (String name : group.getGroupBindingNames()) {
					Value v1 = bindingSet.getValue(name);
					Value v2 = otherSolution.getValue(name);
					if (!Objects.equals(v1, v2)) {
						return false;
					}
				}
				return true;
			}
			return false;
		}

		@Override
		public String toString() {
			QueryBindingSet keybs = new QueryBindingSet();
			for (String name : group.getGroupBindingNames()) {
				Value value = bindingSet.getValue(name);
				if (value != null) {
					keybs.addBinding(name, value);
				}
			}
			return keybs.toString();
		}
    }

    private static final class GroupValue {
		private final Map<String, Aggregator> aggregators;

		GroupValue(Map<String, Aggregator> aggregators) {
			this.aggregators = aggregators;
		}

		void addValues(BindingSet bindingSet) {
			for(Aggregator agg : aggregators.values()) {
				agg.process(bindingSet);
			}
		}

		void bindResult(QueryBindingSet bs) {
			for(Map.Entry<String,Aggregator> entry : aggregators.entrySet()) {
				try(Aggregator agg = entry.getValue()) {
					Value v = agg.getValue();
					if (v != null) {
						bs.setBinding(entry.getKey(), v);
					}
				} catch (ValueExprEvaluationException ignore) {
					// There was a type error when calculating the value of the
					// aggregate.
					// We silently ignore the error, resulting in no result value
					// being bound.
				}
			}
		}

		@Override
		public String toString() {
			return aggregators.toString();
		}
    }

    private Aggregator createAggregator(AggregateOperator operator, BindingSet parentBindings) {
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
			return new ConcatAggregator((GroupConcat) operator, parentStrategy, parentBindings, tripleSource.getValueFactory());
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
    	parentStrategy.initTracking(reduced);
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
				parentStrategy.incrementResultSizeActual(reduced);
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
    	parentStrategy.initTracking(distinct);
        evaluateTupleExpr(new BindingSetPipe(parent) {
            private final BigHashSet<BindingSet> set = BigHashSet.create();
            @Override
            protected boolean handleException(Throwable e) {
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
				parentStrategy.incrementResultSizeActual(distinct);
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
                            return handleException(e);
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
                executor.pullAndPushAsync(parent, service.isSilent() ? new SilentIteration<>(result) : result, service, parentStrategy);
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
    	String algorithm = join.getAlgorithmName();
    	if (isOutOfScopeForLeftArgBindings(join.getRightArg())) {
    		algorithm = Algorithms.HASH_JOIN;
    	}

    	if (Algorithms.HASH_JOIN.equals(algorithm)) {
    		evaluateHashJoin(topPipe, join, bindings);
    	} else {
    		evaluateNestedLoopsJoin(topPipe, join, bindings);
    	}
    }

    private static boolean isOutOfScopeForLeftArgBindings(TupleExpr expr) {
		return (TupleExprs.isVariableScopeChange(expr) || TupleExprs.containsSubquery(expr));
	}

    private void evaluateNestedLoopsJoin(BindingSetPipe topPipe, final Join join, final BindingSet bindings) {
    	join.setAlgorithm(Algorithms.NESTED_LOOPS);
    	TupleExpr outerExpr = join.getLeftArg();
    	TupleExpr innerExpr = join.getRightArg();
    	parentStrategy.initTracking(join);
        evaluateTupleExpr(new BindingSetPipe(topPipe) {
        	final AtomicLong joinsInProgress = new AtomicLong();
        	final AtomicBoolean joinsFinished = new AtomicBoolean();

            @Override
            protected boolean next(BindingSet bs) throws InterruptedException {
            	joinsInProgress.incrementAndGet();
                evaluateTupleExpr(new BindingSetPipe(parent) {
                	@Override
                	protected boolean next(BindingSet bs) throws InterruptedException {
                        parentStrategy.incrementResultSizeActual(join);
                		return parent.push(bs);
                	}
                    @Override
                    public void close() throws InterruptedException {
                    	joinsInProgress.decrementAndGet();
                    	if (joinsFinished.get() && joinsInProgress.compareAndSet(0L, -1L)) {
                    		parent.close();
                    	}
                    }
                    @Override
                    public String toString() {
                    	return "JoinBindingSetPipe(inner)";
                    }
                }, innerExpr, bs);
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
            	return "JoinBindingSetPipe(outer)";
            }
        }, outerExpr, bindings);
    }

    private void evaluateHashJoin(BindingSetPipe topPipe, final Join join, final BindingSet bindings) {
    	evaluateHashJoin(topPipe, join, bindings, false);
    }

    /**
     * Evaluate {@link LeftJoin} query model nodes
     * @param parentPipe
     * @param leftJoin
     * @param bindings
     */
    private void evaluateLeftJoin(BindingSetPipe parentPipe, final LeftJoin leftJoin, final BindingSet bindings) {
    	String algorithm = leftJoin.getAlgorithmName();
    	if (TupleExprs.containsSubquery(leftJoin.getRightArg())) {
    		algorithm = Algorithms.HASH_JOIN;
    	}

    	if (Algorithms.HASH_JOIN.equals(algorithm)) {
    		evaluateHashLeftJoin(parentPipe, leftJoin, bindings);
    	} else {
    		evaluateNestedLoopsLeftJoin(parentPipe, leftJoin, bindings);
    	}
    }

    private static QueryBindingSet getFilteredBindings(BindingSet bindings, Set<String> problemVars) {
        QueryBindingSet filteredBindings = new QueryBindingSet(bindings);
        filteredBindings.removeAll(problemVars);
        return filteredBindings;
    }

    private void evaluateNestedLoopsLeftJoin(BindingSetPipe parentPipe, final LeftJoin leftJoin, final BindingSet bindings) {
    	leftJoin.setAlgorithm(Algorithms.NESTED_LOOPS);
    	parentStrategy.initTracking(leftJoin);
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
                                return pushToParent(rightBindings);
                            } else {
                                // Limit the bindings to the ones that are in scope for
                                // this filter
                                QueryBindingSet scopeBindings = new QueryBindingSet(rightBindings);
                                scopeBindings.retainAll(scopeBindingNames);
                                if (parentStrategy.isTrue(leftJoin.getCondition(), scopeBindings)) {
                                    failed = false;
                                    return pushToParent(rightBindings);
                                }
                            }
                        } catch (ValueExprEvaluationException ignore) {
                        } catch (QueryEvaluationException e) {
                            return handleException(e);
                        }
                        return true;
                    }
                	private boolean pushToParent(BindingSet bs) throws InterruptedException {
                		parentStrategy.incrementResultSizeActual(leftJoin);
                		return parent.push(bs);
                	}
                    @Override
                    public void close() throws InterruptedException {
                        if (failed) {
                            // Join failed, return left arg's bindings
                        	pushToParent(leftBindings);
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

    private void evaluateHashLeftJoin(BindingSetPipe parentPipe, final LeftJoin leftJoin, final BindingSet bindings) {
    	evaluateHashJoin(parentPipe, leftJoin, bindings, true);
    }

    private void evaluateHashJoin(BindingSetPipe topPipe, final BinaryTupleOperator join, final BindingSet bindings, boolean isLeftJoin) {
    	final int hashJoinLimit;
    	{
    		JoinAlgorithmOptimizer algoOpt = parentStrategy.getJoinAlgorithmOptimizer();
        	if (algoOpt != null) {
        		hashJoinLimit = algoOpt.getHashJoinLimit();
        	} else {
        		hashJoinLimit = DEFAULT_HASH_JOIN_LIMIT;
        	}
    	}
    	AbstractHashTableJoiner joiner = isLeftJoin ? new LeftHashTableJoiner(topPipe, join, bindings) : new HashTableJoiner(topPipe, join, bindings);
    	buildHashTable(joiner, bindings, isLeftJoin ? Integer.MAX_VALUE : hashJoinLimit);
    }

    private void buildHashTable(AbstractHashTableJoiner joiner, final BindingSet bindings, final int hashJoinLimit) {
    	joiner.join.setAlgorithm(Algorithms.HASH_JOIN);
    	parentStrategy.initTracking(joiner.join);
		evaluateTupleExpr(new BindingSetPipe(null) {
	    	HashJoinTable hashTable = joiner.createHashTable();
            @Override
            protected boolean next(BindingSet buildBs) throws InterruptedException {
            	HashJoinTable partition;
            	synchronized (this) {
                	if (hashTable.entryCount() >= hashJoinLimit) {
                		partition = hashTable;
                		hashTable = joiner.createHashTable();
                	} else {
                		partition = null;
                	}
            		hashTable.put(buildBs);
            	}
            	if (partition != null) {
            		joiner.doJoin(partition, false);
            	}
            	return true;
            }
            @Override
            public void close() throws InterruptedException {
            	synchronized (this) {
            		if (hashTable != null) {
                   		joiner.doJoin(hashTable, true);
            			hashTable = null;
            		}
            	}
            }
            @Override
            public String toString() {
            	return "HashTableBindingSetPipe";
            }
    	}, joiner.buildExpr, bindings);
    }


	private static Set<String> getJoinAttributes(TupleExpr probeExpr,  TupleExpr buildExpr) {
		Set<String> joinAttributeNames;
		// for statement patterns can ignore equal common constant vars
		if (probeExpr instanceof StatementPattern && buildExpr instanceof StatementPattern) {
			Map<String,Var> probeVars = getVars((StatementPattern) probeExpr);
			Map<String,Var> buildVars = getVars((StatementPattern) buildExpr);
			joinAttributeNames = new HashSet<>(5);
			for (Map.Entry<String,Var> entry : probeVars.entrySet()) {
				String name = entry.getKey();
				Var otherVar = buildVars.get(name);
				if (otherVar != null) {
					if (!isSameConstant(entry.getValue(), otherVar)) {
						joinAttributeNames.add(name);
					}
				}
			}
		} else {
    		joinAttributeNames = probeExpr.getBindingNames();
    		if (!joinAttributeNames.isEmpty()) {
    			// make modifiable
    			joinAttributeNames = new HashSet<>(joinAttributeNames);
    			joinAttributeNames.retainAll(buildExpr.getBindingNames());
    		}
		}
		return joinAttributeNames;
	}

	private static boolean isSameConstant(Var v1, Var v2) {
		return v1.isConstant() && v2.isConstant() && v1.getValue().equals(v2.getValue());
	}

	private static Map<String,Var> getVars(StatementPattern sp) {
		Map<String,Var> vars = new HashMap<>(5);
		Var subjVar = sp.getSubjectVar();
		vars.put(subjVar.getName(), subjVar);
		Var predVar = sp.getPredicateVar();
		vars.put(predVar.getName(), predVar);
		Var objVar = sp.getObjectVar();
		vars.put(objVar.getName(), objVar);
		Var ctxVar = sp.getContextVar();
		if (ctxVar != null) {
			vars.put(ctxVar.getName(), ctxVar);
		}
		return vars;
	}

    private static <E> String[] toStringArray(Collection<E> c) {
        return c.toArray(new String[c.size()]);
    }

    private abstract class AbstractHashTableJoiner {
    	private final AtomicLong joinsInProgress = new AtomicLong();
    	private final AtomicBoolean joinsFinished = new AtomicBoolean();
    	private final BindingSetPipe parent;
    	private final BinaryTupleOperator join;
    	private final TupleExpr probeExpr;
    	private final TupleExpr buildExpr;
    	private final BindingSet bindings;
    	private final Set<String> joinAttributeSet;
    	private final String[] joinAttributes;
		private final String[] buildAttributes;
		private final int initialSize;

    	protected AbstractHashTableJoiner(BindingSetPipe parent, BinaryTupleOperator join, BindingSet bindings) {
    		this.parent = parent;
    		this.join = join;
    		this.bindings = bindings;
        	this.probeExpr = join.getLeftArg();
        	this.buildExpr = join.getRightArg();
        	this.joinAttributeSet = getJoinAttributes(probeExpr, buildExpr);
        	this.joinAttributes = toStringArray(joinAttributeSet);
    		this.buildAttributes = toStringArray(buildExpr.getBindingNames());
    		this.initialSize = (int) Math.min(MAX_INITIAL_HASH_JOIN_TABLE_SIZE, Math.max(0, buildExpr.getResultSizeEstimate()));
    	}

    	final HashJoinTable createHashTable() {
	    	return new HashJoinTable(initialSize, joinAttributes, buildAttributes);
    	}

		boolean hasUnboundOptionalValue(BindingSet probeBs) {
	    	for (String name : joinAttributes) {
	    		if (probeBs.getValue(name) == null) {
	    			return true;
	    		}
	    	}
	    	return false;
		}

		boolean canJoin(BindingSetValues buildBsv, BindingSet probeBs) {
			return buildBsv.canJoin(buildAttributes, joinAttributeSet, probeBs);
		}

		BindingSet join(BindingSetValues buildBsv, BindingSet probeBs) {
			return buildBsv.joinTo(buildAttributes, probeBs);
		}

    	/**
    	 * Performs a hash-join.
    	 * @param hashTablePartition hash table to join against.
    	 * @param isLast true if this is the last time doJoin() will be called for the current join operation.
    	 * @throws InterruptedException
    	 */
    	public final void doJoin(HashJoinTable hashTablePartition, boolean isLast) throws InterruptedException {
    		if (hashTablePartition.entryCount() == 0) {
    			if (isLast) {
    				parent.close();
    			}
    		} else {
    			joinsInProgress.incrementAndGet();
    			if (isLast) {
            		joinsFinished.set(true);
    			}
    			// NB: this part may execute asynchronously
            	evaluateTupleExpr(createPipe(parent, hashTablePartition), join.getLeftArg(), bindings);
    		}
    	}

    	protected abstract BindingSetPipe createPipe(BindingSetPipe parent, HashJoinTable hashTablePartition);

    	abstract class AbstractHashJoinBindingSetPipe extends BindingSetPipe {
    		protected final HashJoinTable hashTablePartition;
    		protected AbstractHashJoinBindingSetPipe(BindingSetPipe parent, HashJoinTable hashTablePartition) {
				super(parent);
				this.hashTablePartition = hashTablePartition;
			}
			protected final boolean pushToParent(BindingSet bs) throws InterruptedException {
        		parentStrategy.incrementResultSizeActual(join);
        		return parent.push(bs);
        	}
            @Override
        	public void close() throws InterruptedException {
            	joinsInProgress.decrementAndGet();
            	if (joinsFinished.get() && joinsInProgress.compareAndSet(0L, -1L)) {
            		parent.close();
            	}
        	}
    	}
    }

	final class HashTableJoiner extends AbstractHashTableJoiner {
		HashTableJoiner(BindingSetPipe parent, BinaryTupleOperator join, BindingSet bindings) {
			super(parent, join, bindings);
		}

		final class HashJoinBindingSetPipe extends AbstractHashJoinBindingSetPipe {
			protected HashJoinBindingSetPipe(BindingSetPipe parent, HashJoinTable hashTablePartition) {
				super(parent, hashTablePartition);
			}
			@Override
			protected boolean next(BindingSet probeBs) throws InterruptedException {
				if (probeBs.size() == 0) {
					// the empty binding set should be merged with all binding sets in the hash table
					Collection<? extends List<BindingSetValues>> hashValues = hashTablePartition.all();
					for (List<BindingSetValues> hashValue : hashValues) {
						for (BindingSetValues bsv : hashValue) {
							if (!pushToParent(join(bsv, probeBs))) {
								return false;
							}
						}
					}
				} else if (hasUnboundOptionalValue(probeBs)) {
					// have to manually search through all the binding sets
					Collection<? extends List<BindingSetValues>> hashValues = hashTablePartition.all();
					for (List<BindingSetValues> hashValue : hashValues) {
						for (BindingSetValues bsv : hashValue) {
							if (canJoin(bsv, probeBs)) {
								if (!pushToParent(join(bsv, probeBs))) {
									return false;
								}
							}
						}
					}
				} else {
					List<BindingSetValues> hashValue = hashTablePartition.get(probeBs);
					if (hashValue != null && !hashValue.isEmpty()) {
						for (BindingSetValues bsv : hashValue) {
							if (!pushToParent(join(bsv, probeBs))) {
								return false;
							}
						}
					}
				}
				return true;
			}
		    @Override
		    public String toString() {
		    	return "HashJoinBindingSetPipe";
		    }
		}

		@Override
		protected BindingSetPipe createPipe(BindingSetPipe parent, HashJoinTable hashTablePartition) {
			return new HashJoinBindingSetPipe(parent, hashTablePartition);
		}
	}

	final class LeftHashTableJoiner extends AbstractHashTableJoiner {
		LeftHashTableJoiner(BindingSetPipe parent, BinaryTupleOperator join, BindingSet bindings) {
			super(parent, join, bindings);
		}

		final class LeftHashJoinBindingSetPipe extends AbstractHashJoinBindingSetPipe {
			protected LeftHashJoinBindingSetPipe(BindingSetPipe parent, HashJoinTable hashTablePartition) {
				super(parent, hashTablePartition);
			}
			@Override
			protected boolean next(BindingSet probeBs) throws InterruptedException {
				if (probeBs.size() == 0) {
					// the empty binding set should be merged with all binding sets in the hash table
					Collection<? extends List<BindingSetValues>> hashValues = hashTablePartition.all();
					for (List<BindingSetValues> hashValue : hashValues) {
						for (BindingSetValues bsv : hashValue) {
							if (!pushToParent(join(bsv, probeBs))) {
								return false;
							}
						}
					}
				} else if (hasUnboundOptionalValue(probeBs)) {
					// have to manually search through all the binding sets
					Collection<? extends List<BindingSetValues>> hashValues = hashTablePartition.all();
					boolean foundJoin = false;
					for (List<BindingSetValues> hashValue : hashValues) {
						for (BindingSetValues bsv : hashValue) {
							if (canJoin(bsv, probeBs)) {
								foundJoin = true;
								if (!pushToParent(join(bsv, probeBs))) {
									return false;
								}
							}
						}
					}
					if (!foundJoin) {
						if (!pushToParent(probeBs)) {
							return false;
						}
					}
				} else {
					List<BindingSetValues> hashValue = hashTablePartition.get(probeBs);
					if (hashValue != null && !hashValue.isEmpty()) {
						for (BindingSetValues bsv : hashValue) {
							if (!pushToParent(join(bsv, probeBs))) {
								return false;
							}
						}
					} else {
						if (!pushToParent(probeBs)) {
							return false;
						}
					}
				}
				return true;
			}
		    @Override
		    public String toString() {
		    	return "LeftHashJoinBindingSetPipe";
		    }
		}

		@Override
		protected BindingSetPipe createPipe(BindingSetPipe parent, HashJoinTable hashTablePartition) {
			return new LeftHashJoinBindingSetPipe(parent, hashTablePartition);
		}
	}

    private static final class HashJoinTable {
    	private final String[] joinAttributes;
    	private final String[] buildAttributes;
    	private final Map<BindingSetValues, List<BindingSetValues>> hashTable;
		private int keyCount;
		private int bsCount;

		HashJoinTable(int initialSize, String[] joinAttributes, String[] buildAttributes) {
			this.joinAttributes = joinAttributes;
			this.buildAttributes = buildAttributes;
    		if (joinAttributes.length > 0) {
    			hashTable = new HashMap<>(initialSize);
    		} else {
    			hashTable = Collections.<BindingSetValues, List<BindingSetValues>>singletonMap(BindingSetValues.EMPTY, new ArrayList<>(initialSize));
    		}
    	}

		void put(BindingSet bs) {
			BindingSetValues hashKey = BindingSetValues.create(joinAttributes, bs);
			List<BindingSetValues> hashValue = hashTable.get(hashKey);
			boolean newEntry = (hashValue == null);
			if (newEntry) {
				int averageSize = (keyCount > 0) ? (int) (bsCount/keyCount) : 0;
				hashValue = new ArrayList<>(averageSize + 1);
				hashTable.put(hashKey, hashValue);
				keyCount++;
			}
			hashValue.add(BindingSetValues.create(buildAttributes, bs));
			bsCount++;
		}

		int entryCount() {
			return bsCount;
		}

    	List<BindingSetValues> get(BindingSet bs) {
    		BindingSetValues key = BindingSetValues.create(joinAttributes, bs);
			return hashTable.get(key);
    	}

    	Collection<? extends List<BindingSetValues>> all() {
    		return hashTable.values();
    	}
    }

    /**
     * Evaluate {@link Union} query model nodes.
     * @param parent
     * @param union
     * @param bindings
     */
    private void evaluateUnion(BindingSetPipe parent, Union union, BindingSet bindings) {
    	parentStrategy.initTracking(union);
        BindingSetPipe pipe = new BindingSetPipe(parent) {
            final AtomicInteger args = new AtomicInteger(2);
        	@Override
        	protected boolean next(BindingSet bs) throws InterruptedException {
                parentStrategy.incrementResultSizeActual(union);
        		return parent.push(bs);
        	}
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
            protected boolean handleException(Throwable e) {
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
                            return handleException(e);
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
            protected boolean handleException(Throwable e) {
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
                evaluateTupleExpr(new BindingSetPipe(parent) {
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
    	Join nestedJoins = starJoin.toJoins();
    	Join topJoin = new StarJoin.TopJoin(starJoin, nestedJoins.getLeftArg(), nestedJoins.getRightArg());
		JoinAlgorithmOptimizer algoOpt = parentStrategy.getJoinAlgorithmOptimizer();
    	if (algoOpt != null) {
    		algoOpt.optimize(topJoin, null, null);
    	}
    	evaluateTupleExpr(parent, topJoin, bindings);
    }

    /**
     * Evaluate {@link SingletonSet} query model nodes
     * @param parent
     * @param singletonSet
     * @param bindings
     */
    private void evaluateSingletonSet(BindingSetPipe parent, SingletonSet singletonSet, BindingSet bindings) {
		parentStrategy.initTracking(singletonSet);
        try {
            parent.pushLast(bindings);
            parentStrategy.incrementResultSizeActual(singletonSet);
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
		parentStrategy.initTracking(emptySet);
		parent.empty();
	}

	/**
	 * Evaluate {@link ZeroLengthPath} query model nodes
	 * @param parent
	 * @param zlp
	 * @param bindings
	 */
	private void evaluateZeroLengthPath(BindingSetPipe parent, ZeroLengthPath zlp, BindingSet bindings) {
		final Var subjVar = zlp.getSubjectVar();
		final Var objVar = zlp.getObjectVar();
		final Var contextVar = zlp.getContextVar();
		Value subj = subjVar.getValue() == null ? bindings.getValue(subjVar.getName()) : subjVar.getValue();
		Value obj = objVar.getValue() == null ? bindings.getValue(objVar.getName()) : objVar.getValue();
		if (subj != null && obj != null) {
			if (!subj.equals(obj)) {
				parent.empty();
				return;
			}
		}

		if (subj == null && obj == null) {
			Var allSubjVar = Algebra.createAnonVar(ANON_SUBJECT_VAR);
			Var allPredVar = Algebra.createAnonVar(ANON_PREDICATE_VAR);
			Var allObjVar = Algebra.createAnonVar(ANON_OBJECT_VAR);
			StatementPattern sp;
			if (contextVar != null) {
				sp = new StatementPattern(Scope.NAMED_CONTEXTS, allSubjVar, allPredVar, allObjVar, contextVar.clone());
			} else {
				sp = new StatementPattern(allSubjVar, allPredVar, allObjVar);
			}
			evaluateStatementPattern(new BindingSetPipe(parent) {
				private final BigHashSet<Value> set = BigHashSet.create();
				@Override
				protected boolean next(BindingSet bs) throws InterruptedException {
					Value ctx = (contextVar != null) ? bs.getValue(contextVar.getName()) : null;
					Value v = bs.getValue(ANON_SUBJECT_VAR);
					if (!nextValue(v, ctx)) {
						return false;
					}
					v = bs.getValue(ANON_OBJECT_VAR);
					if (!nextValue(v, ctx)) {
						return false;
					}
					return true;
				}
				private boolean nextValue(Value v, Value ctx) throws InterruptedException {
					try {
						if (set.add(v)) {
							QueryBindingSet result = new QueryBindingSet(bindings);
							result.addBinding(subjVar.getName(), v);
							result.addBinding(objVar.getName(), v);
							if (ctx != null) {
								result.addBinding(contextVar.getName(), ctx);
							}
							return parent.push(result);
						} else {
							return true;
						}
					} catch (IOException ioe) {
						return handleException(ioe);
					}
				}
			}, sp, bindings);
		} else {
			QueryBindingSet result = new QueryBindingSet(bindings);
			if (obj == null && subj != null) {
				result.addBinding(objVar.getName(), subj);
			} else if (subj == null && obj != null) {
				result.addBinding(subjVar.getName(), obj);
			} else if (subj != null && subj.equals(obj)) {
				// empty bindings
				// (result but nothing to bind as subjectVar and objVar are both fixed)
			} else {
				result = null;
			}
			if (result != null) {
				try {
					parent.pushLast(result);
				} catch (InterruptedException e) {
					parent.handleException(e);
				}
			} else {
				parent.empty();
			}
		}
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
        	executor.pullAndPushAsync(parent, new PathIteration(new StrictEvaluationStrategy(null, null) {
                @Override
                public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(ZeroLengthPath zlp, BindingSet bindings) throws QueryEvaluationException {
                    zlp.setParentNode(alp);
                    return parentStrategy.evaluate(zlp, bindings);
                }

                @Override
                public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(TupleExpr expr, BindingSet bindings) throws QueryEvaluationException {
                    expr.setParentNode(alp);
                    return parentStrategy.evaluate(expr, bindings);
                }

            }, scope, subjectVar, pathExpression, objVar, contextVar, minLength, bindings), alp, parentStrategy);
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
        if (bindings.size() == 0) { // empty binding set
        	try {
        		try {
		        	for (BindingSet b : bsa.getBindingSets()) {
		        		if (!parent.push(b)) {
		        			return;
		        		}
		        	}
                } finally {
                	parent.close();
                }
            } catch (InterruptedException e) {
                parent.handleException(e);
            }
        } else {
        	try {
        		try {
		        	for (BindingSet assignedBindings : bsa.getBindingSets()) {
	                    QueryBindingSet result;
	                    if (assignedBindings.size() == 0) {
	                    	result = new QueryBindingSet(bindings);
	                    } else {
	                    	result = null;
		                    for (String name : assignedBindings.getBindingNames()) {
		                        final Value assignedValue = assignedBindings.getValue(name);
		                        if (assignedValue != null) { // can be null if set to UNDEF
		                            // check that the binding assignment does not overwrite
		                            // existing bindings.
		                            Value bValue = bindings.getValue(name);
		                            if (bValue == null || assignedValue.equals(bValue)) {
		                            	// values are compatible - create a result if it doesn't already exist
	                                    if (result == null) {
	                                        result = new QueryBindingSet(bindings);
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
	
	                    if (result != null) {
			        		if (!parent.push(result)) {
			        			return;
			        		}
		        		}
		        	}
	            } finally {
	            	parent.close();
	            }
            } catch (InterruptedException e) {
                parent.handleException(e);
            }
        }
    }

    /**
	 * Evaluate {@link TupleFunctionCall} query model nodes
	 * 
	 * @param topPipe
	 * @param tfc
	 * @param bindings
	 */
	private void evaluateTupleFunctionCall(BindingSetPipe topPipe, ExtendedTupleFunctionCall tfc, BindingSet bindings)
			throws QueryEvaluationException {
		TupleFunction func = tupleFunctionRegistry.get(tfc.getURI())
				.orElseThrow(() -> new QueryEvaluationException("Unknown tuple function '" + tfc.getURI() + "'"));

		evaluateTupleExpr(new BindingSetPipe(topPipe) {
			@Override
			protected boolean next(BindingSet bs) throws InterruptedException {
				try {
					List<ValueExpr> args = tfc.getArgs();
					Value[] argValues = new Value[args.size()];
					for (int i = 0; i < args.size(); i++) {
						argValues[i] = parentStrategy.evaluate(args.get(i), bs);
					}

					CloseableIteration<BindingSet, QueryEvaluationException> iter;
					queryContext.begin();
					try {
						iter = TupleFunctionEvaluationStrategy.evaluate(func, tfc.getResultVars(), bs, tripleSource.getValueFactory(), argValues);
					} finally {
						queryContext.end();
					}
					iter = new QueryContextIteration(iter, queryContext);
					try {
						while (iter.hasNext()) {
							if(!parent.push(iter.next())) {
								return false;
							}
						}
					} finally {
						iter.close();
					}
				} catch (ValueExprEvaluationException ignore) {
					// can't evaluate arguments
				}
				return true;
			}
			@Override
			public String toString() {
				return "TupleFunctionCallBindingSetPipe";
			}
		}, tfc.getDependentExpression(), bindings);
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
