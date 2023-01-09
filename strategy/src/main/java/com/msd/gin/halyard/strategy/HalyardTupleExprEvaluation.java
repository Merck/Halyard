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

import com.msd.gin.halyard.algebra.AbstractExtendedQueryModelVisitor;
import com.msd.gin.halyard.algebra.Algebra;
import com.msd.gin.halyard.algebra.Algorithms;
import com.msd.gin.halyard.algebra.ConstrainedStatementPattern;
import com.msd.gin.halyard.algebra.ExtendedTupleFunctionCall;
import com.msd.gin.halyard.algebra.StarJoin;
import com.msd.gin.halyard.algebra.evaluation.ConstrainedTripleSourceFactory;
import com.msd.gin.halyard.common.LiteralConstraint;
import com.msd.gin.halyard.common.ValueConstraint;
import com.msd.gin.halyard.common.ValueType;
import com.msd.gin.halyard.federation.BindingSetPipeFederatedService;
import com.msd.gin.halyard.optimizers.JoinAlgorithmOptimizer;
import com.msd.gin.halyard.query.BindingSetPipe;
import com.msd.gin.halyard.query.BindingSetPipeQueryEvaluationStep;
import com.msd.gin.halyard.query.ValuePipe;
import com.msd.gin.halyard.query.ValuePipeQueryValueEvaluationStep;
import com.msd.gin.halyard.strategy.aggregators.AvgAggregateFunction;
import com.msd.gin.halyard.strategy.aggregators.AvgCollector;
import com.msd.gin.halyard.strategy.aggregators.CSVCollector;
import com.msd.gin.halyard.strategy.aggregators.ConcatAggregateFunction;
import com.msd.gin.halyard.strategy.aggregators.CountAggregateFunction;
import com.msd.gin.halyard.strategy.aggregators.LongCollector;
import com.msd.gin.halyard.strategy.aggregators.MaxAggregateFunction;
import com.msd.gin.halyard.strategy.aggregators.MinAggregateFunction;
import com.msd.gin.halyard.strategy.aggregators.NumberCollector;
import com.msd.gin.halyard.strategy.aggregators.SampleAggregateFunction;
import com.msd.gin.halyard.strategy.aggregators.SampleCollector;
import com.msd.gin.halyard.strategy.aggregators.SumAggregateFunction;
import com.msd.gin.halyard.strategy.aggregators.ValueCollector;
import com.msd.gin.halyard.strategy.aggregators.WildcardCountAggregateFunction;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.ConvertingIteration;
import org.eclipse.rdf4j.common.iteration.FilterIteration;
import org.eclipse.rdf4j.common.iteration.LookAheadIteration;
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
import org.eclipse.rdf4j.query.MutableBindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.query.algebra.AggregateFunctionCall;
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
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.RDFStarTripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.TupleFunctionEvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.DescribeIteration;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.PathIteration;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.ProjectionIterator;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.QueryContextIteration;
import org.eclipse.rdf4j.query.algebra.evaluation.util.QueryEvaluationUtility;
import org.eclipse.rdf4j.query.algebra.evaluation.util.ValueComparator;
import org.eclipse.rdf4j.query.algebra.helpers.TupleExprs;
import org.eclipse.rdf4j.query.algebra.helpers.collectors.VarNameCollector;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateCollector;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunction;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunctionFactory;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.CustomAggregateFunctionRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evaluates {@link TupleExpr}s and it's sub-interfaces and implementations.
 * @author Adam Sotona (MSD)
 */
final class HalyardTupleExprEvaluation {
	private static final Logger LOGGER = LoggerFactory.getLogger(HalyardTupleExprEvaluation.class);
	private static final String ANON_SUBJECT_VAR = "__subj";
	private static final String ANON_PREDICATE_VAR = "__pred";
	private static final String ANON_OBJECT_VAR = "__obj";
	private static final int MAX_INITIAL_HASH_JOIN_TABLE_SIZE = 5000;

    private final HalyardEvaluationStrategy parentStrategy;
	private final TupleFunctionRegistry tupleFunctionRegistry;
	private final CustomAggregateFunctionRegistry aggregateFunctionRegistry = CustomAggregateFunctionRegistry.getInstance();
	private final TripleSource tripleSource;
	private final QueryContext queryContext;
    private final Dataset dataset;
    private final QueryEvaluationContext evalContext = null;
    private final int hashJoinLimit;
    private final int collectionMemoryThreshold;

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
		Configuration conf = parentStrategy.getConfiguration();
		JoinAlgorithmOptimizer algoOpt = parentStrategy.getJoinAlgorithmOptimizer();
    	if (algoOpt != null) {
    		hashJoinLimit = algoOpt.getHashJoinLimit();
    	} else {
    		hashJoinLimit = conf.getInt(StrategyConfig.HASH_JOIN_LIMIT, StrategyConfig.DEFAULT_HASH_JOIN_LIMIT);
    	}
    	collectionMemoryThreshold = conf.getInt(StrategyConfig.MEMORY_THRESHOLD, StrategyConfig.DEFAULT_MEMORY_THRESHOLD);
    }

    /**
     * Precompiles the given expression.
     * @param expr supplied by HalyardEvaluationStrategy
     * @return a QueryEvaluationStep
     */
	BindingSetPipeQueryEvaluationStep precompile(TupleExpr expr) {
    	BindingSetPipeEvaluationStep step = precompileTupleExpr(expr);
    	return new BindingSetPipeQueryEvaluationStep() {
			@Override
			public void evaluate(BindingSetPipe parent, BindingSet bindings) {
				step.evaluate(parent, bindings);
			}

			@Override
			public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bindings) {
				return parentStrategy.executor.pushAndPull(step, expr, bindings, parentStrategy);
			}
    	};
    }

    /**
     * Switch logic appropriate for each type of {@link TupleExpr} query model node, sending each type to it's appropriate precompile method. For example,
     * {@code UnaryTupleOperator} is sent to {@link precompileUnaryTupleOperator()}.
     */
    private BindingSetPipeEvaluationStep precompileTupleExpr(TupleExpr expr) {
        if (expr instanceof StatementPattern) {
            return precompileStatementPattern((StatementPattern) expr);
        } else if (expr instanceof UnaryTupleOperator) {
        	return precompileUnaryTupleOperator((UnaryTupleOperator) expr);
        } else if (expr instanceof BinaryTupleOperator) {
        	return precompileBinaryTupleOperator((BinaryTupleOperator) expr);
        } else if (expr instanceof StarJoin) {
        	return precompileStarJoin((StarJoin) expr);
        } else if (expr instanceof SingletonSet) {
        	return precompileSingletonSet((SingletonSet) expr);
        } else if (expr instanceof EmptySet) {
        	return precompileEmptySet((EmptySet) expr);
        } else if (expr instanceof ZeroLengthPath) {
        	return precompileZeroLengthPath((ZeroLengthPath) expr);
        } else if (expr instanceof ArbitraryLengthPath) {
        	return precompileArbitraryLengthPath((ArbitraryLengthPath) expr);
        } else if (expr instanceof BindingSetAssignment) {
        	return precompileBindingSetAssignment((BindingSetAssignment) expr);
        } else if (expr instanceof TripleRef) {
        	return precompileTripleRef((TripleRef) expr);
		} else if (expr instanceof TupleFunctionCall) {
			// all TupleFunctionCalls are expected to be ExtendedTupleFunctionCalls
			return precompileTupleFunctionCall((ExtendedTupleFunctionCall) expr);
        } else if (expr == null) {
            throw new IllegalArgumentException("expr must not be null");
        } else {
            throw new QueryEvaluationException("Unsupported tuple expr type: " + expr.getClass());
        }
    }

    private BindingSetPipeEvaluationStep precompileStatementPattern(StatementPattern sp) {
    	return (parent, bindings) -> evaluateStatementPattern(parent, sp, bindings);
    }

    private TripleSource getTripleSource(StatementPattern sp, BindingSet bindings) {
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
							v = Algebra.getVarValue((Var) constraintValue, bindings);
						} else {
							v = null;
						}
						if (v != null) {
							if (constraintFunc instanceof Datatype) {
								if (!v.isIRI()) {
									return null; // nothing to push
								}
								IRI dt = (IRI) v;
				    			objConstraint = new LiteralConstraint(dt);
							} else if (constraintFunc instanceof Lang) {
								if (!v.isLiteral()) {
									return null; // nothing to push
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
			return ((ConstrainedTripleSourceFactory)tripleSource).getTripleSource(subjConstraint, objConstraint);
    	}
    	return tripleSource;
    }

    /**
     * Evaluate the statement pattern using the supplied bindings
     * @param parent to push or enqueue evaluation results
     * @param sp the {@code StatementPattern} to evaluate
     * @param bindings the set of names to which values are bound. For example, select ?s, ?p, ?o has the names s, p and o and the values bound to them are the
     * results of the evaluation of this statement pattern
     */
    private void evaluateStatementPattern(final BindingSetPipe parent, final StatementPattern sp, final BindingSet bindings) {
		TripleSource ts = getTripleSource(sp, bindings);
		if (ts == null) {
			parent.close(); // nothing to push
		}

		try {
			QueryEvaluationStep evalStep = evaluateStatementPattern(sp, bindings, ts);
			if (evalStep != null) {
				parentStrategy.executor.pullAndPushAsync(parent, evalStep, sp, bindings, parentStrategy);
			} else {
				parent.close(); // nothing to push
			}
        } catch (QueryEvaluationException e) {
            parent.handleException(e);
        }
    }

    static final class NamedQuads {
    	final Resource subj;
    	final IRI pred;
    	final Value obj;
    	final Resource[] ctxs;

    	public NamedQuads(Resource subj, IRI pred, Value obj, Resource[] ctxs) {
			this.subj = subj;
			this.pred = pred;
			this.obj = obj;
			this.ctxs = ctxs;
		}
    }

    private NamedQuads getNamedQuads(StatementPattern sp, BindingSet bindings) {
        final Var subjVar = sp.getSubjectVar(); //subject
        final Var predVar = sp.getPredicateVar(); //predicate
        final Var objVar = sp.getObjectVar(); //object
        final Var conVar = sp.getContextVar(); //graph or target context

        final Resource subjValue;
        final IRI predValue;
        final Value objValue;
        final Resource contextValue;
    	try {
	        subjValue = (Resource) Algebra.getVarValue(subjVar, bindings);
	        predValue = (IRI) Algebra.getVarValue(predVar, bindings);
	        objValue = Algebra.getVarValue(objVar, bindings);
	        contextValue = (Resource) Algebra.getVarValue(conVar, bindings);
        } catch (ClassCastException e) {
            // Invalid value type for subject, predicate and/or context
            return null;
        }

        if (isUnbound(subjVar, bindings) || isUnbound(predVar, bindings) || isUnbound(objVar, bindings) || isUnbound(conVar, bindings)) {
            // the variable must remain unbound for this solution see https://www.w3.org/TR/sparql11-query/#assignment
            return null;
        }

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
            contexts = null; //no results from this statement pattern
        } else if (graphs == null || graphs.isEmpty()) {
            // store default behaviour
            if (contextValue != null) {
				if (RDF4J.NIL.equals(contextValue) || SESAME.NIL.equals(contextValue)) {
					contexts = new Resource[] { null };
				} else {
					contexts = new Resource[] { contextValue };
				}
            }
			/*
			 * TODO activate this to have an exclusive (rather than inclusive) interpretation of the default
			 * graph in SPARQL querying. else if (statementPattern.getScope() == Scope.DEFAULT_CONTEXTS ) {
			 * contexts = new Resource[] { null }; }
			 */
			else {
				contexts = new Resource[0];
			}
        } else if (contextValue != null) {
            if (graphs.contains(contextValue)) {
                contexts = new Resource[]{contextValue};
            } else {
                // Statement pattern specifies a context that is not part of
                // the dataset
            	contexts = null; //no results possible because the context is not available
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

        if (contexts == null) {
        	return null;
        }

        return new NamedQuads(subjValue, predValue, objValue, contexts);
    }

    private QueryEvaluationStep evaluateStatementPattern(final StatementPattern sp, final BindingSet bindings, TripleSource tripleSource) {
        final Var subjVar = sp.getSubjectVar(); //subject
        final Var predVar = sp.getPredicateVar(); //predicate
        final Var objVar = sp.getObjectVar(); //object
        final Var conVar = sp.getContextVar(); //graph or target context

        NamedQuads nq = getNamedQuads(sp, bindings);
        if (nq == null) {
        	return null;
        }

        return bs -> {
	        //get an iterator over all triple statements that match the s, p, o specification in the contexts
	        CloseableIteration<? extends Statement, QueryEvaluationException> stIter = tripleSource.getStatements(nq.subj, nq.pred, nq.obj, nq.ctxs);
	
	        if (nq.ctxs.length == 0 && sp.getScope() == StatementPattern.Scope.NAMED_CONTEXTS) {
	            // Named contexts are matched by retrieving all statements from
	            // the store and filtering out the statements that do not have a context.
	            stIter = new FilterIteration<Statement, QueryEvaluationException>(stIter) {
	
	                @Override
	                protected boolean accept(Statement st) {
	                    return st.getContext() != null;
	                }
	
	            }; // end anonymous class
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
		
		                if (subjVar != null && nq.subj == null) {
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
		
		                if (predVar != null && nq.pred == null) {
		                    if (predVar.equals(objVar) && !pred.equals(obj)) {
		                        return false;
		                    }
		                    if (predVar.equals(conVar) && !pred.equals(context)) {
		                        return false;
		                    }
		                }
		
		                if (objVar != null && nq.obj == null) {
		                    if (objVar.equals(conVar) && !obj.equals(context)) {
		                        return false;
		                    }
		                }
		
		                return true;
		            }
		        };
	        }
	
	        // Return an iterator that converts the RDF statements (triples) to var bindings
	        return new ConvertingIteration<Statement, BindingSet, QueryEvaluationException>(stIter) {
	
	            @Override
	            protected BindingSet convert(Statement st) {
	                QueryBindingSet result = new QueryBindingSet(bs);
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
			};
        };
    }

    private boolean isUnbound(Var var, BindingSet bindings) {
        if (var == null) {
            return false;
        } else {
            return bindings.hasBinding(var.getName()) && bindings.getValue(var.getName()) == null;
        }
    }

	/**
	 * Precompiles a TripleRef node returning bindingsets from the matched Triple nodes in the dataset (or explore
	 * standard reification)
	 */
	private BindingSetPipeEvaluationStep precompileTripleRef(TripleRef ref) {
		// Naive implementation that walks over all statements matching (x rdf:type rdf:Statement)
		// and filter those that do not match the bindings for subject, predicate and object vars (if bound)
		final Var subjVar = ref.getSubjectVar();
		final Var predVar = ref.getPredicateVar();
		final Var objVar = ref.getObjectVar();
		final Var extVar = ref.getExprVar();
		// whether the TripleSouce support access to RDF star
		final boolean sourceSupportsRdfStar = tripleSource instanceof RDFStarTripleSource;
		if (sourceSupportsRdfStar) {
			return (parent, bindings) -> {
				final Value subjValue = Algebra.getVarValue(subjVar, bindings);
				final Value predValue = Algebra.getVarValue(predVar, bindings);
				final Value objValue = Algebra.getVarValue(objVar, bindings);
				final Value extValue = Algebra.getVarValue(extVar, bindings);

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
					parent.close(); // nothing to push
					return;
				}

				final QueryEvaluationStep evalStep;
				evalStep = bs -> {
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
		
					return new ConvertingIteration<Triple, BindingSet, QueryEvaluationException>(filterIter) {
						@Override
						protected BindingSet convert(Triple triple) {
							QueryBindingSet result = new QueryBindingSet(bs);
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
				};
				parentStrategy.executor.pullAndPushAsync(parent, evalStep, ref, bindings, parentStrategy);
			};
		} else {
			return (parent, bindings) -> {
				final Value subjValue = Algebra.getVarValue(subjVar, bindings);
				final Value predValue = Algebra.getVarValue(predVar, bindings);
				final Value objValue = Algebra.getVarValue(objVar, bindings);
				final Value extValue = Algebra.getVarValue(extVar, bindings);

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
					parent.close(); // nothing to push
					return;
				}

				final QueryEvaluationStep evalStep;
				// standard reification iteration
				evalStep = bs -> {
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
					return new LookAheadIteration<BindingSet, QueryEvaluationException>() {
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
								QueryBindingSet result = new QueryBindingSet(bs);
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
				};
				parentStrategy.executor.pullAndPushAsync(parent, evalStep, ref, bindings, parentStrategy);
			};
		}
	}

    /**
     * Switch logic for precompilation of any instance of a {@link UnaryTupleOperator} query model node
     */
    private BindingSetPipeEvaluationStep precompileUnaryTupleOperator(UnaryTupleOperator expr) {
        if (expr instanceof Projection) {
        	return precompileProjection((Projection) expr);
        } else if (expr instanceof MultiProjection) {
        	return precompileMultiProjection((MultiProjection) expr);
        } else if (expr instanceof Filter) {
        	return precompileFilter((Filter) expr);
        } else if (expr instanceof Service) {
        	return precompileService((Service) expr);
        } else if (expr instanceof Slice) {
        	return precompileSlice((Slice) expr);
        } else if (expr instanceof Extension) {
        	return precompileExtension((Extension) expr);
        } else if (expr instanceof Distinct) {
        	return precompileDistinct((Distinct) expr);
        } else if (expr instanceof Reduced) {
        	return precompileReduced((Reduced) expr);
        } else if (expr instanceof Group) {
        	return precompileGroup((Group) expr);
        } else if (expr instanceof Order) {
        	return precompileOrder((Order) expr);
        } else if (expr instanceof QueryRoot) {
        	QueryRoot root = (QueryRoot) expr;
        	BindingSetPipeEvaluationStep step = precompileTupleExpr(root.getArg());
        	return (parent, bindings) -> {
                parentStrategy.sharedValueOfNow = null;
        		step.evaluate(parent, bindings);
        	};
        } else if (expr instanceof DescribeOperator) {
        	return precompileDescribeOperator((DescribeOperator) expr);
        } else if (expr == null) {
            throw new IllegalArgumentException("expr must not be null");
        } else {
            throw new QueryEvaluationException("Unknown unary tuple operator type: " + expr.getClass());
        }
    }

    /**
     * Precompile a {@link Projection} query model nodes
     * @param projection
     */
    private BindingSetPipeEvaluationStep precompileProjection(final Projection projection) {
        BindingSetPipeEvaluationStep step = precompileTupleExpr(projection.getArg());
        boolean outer = true;
        QueryModelNode ancestor = projection;
        while (ancestor.getParentNode() != null) {
                ancestor = ancestor.getParentNode();
                if (ancestor instanceof Projection || ancestor instanceof MultiProjection) {
                        outer = false;
                }
        }
        final boolean includeAll = !outer;
    	return (parent, bindings) -> {
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
	
	        step.evaluate(new BindingSetPipe(parent) {
	            @Override
	            protected boolean next(BindingSet bs) {
	                return parent.push(ProjectionIterator.project(projection.getProjectionElemList(), bs, bindings, includeAll));
	            }
	            @Override
	            public String toString() {
	            	return "ProjectionBindingSetPipe";
	            }
	        }, pushDownBindings);
    	};
    }

    /**
     * Precompile a {@link MultiProjection} query model nodes
     * @param multiProjection
     */
    private BindingSetPipeEvaluationStep precompileMultiProjection(final MultiProjection multiProjection) {
        BindingSetPipeEvaluationStep step = precompileTupleExpr(multiProjection.getArg());
        return (parent, bindings) -> {
	        step.evaluate(new BindingSetPipe(parent) {
	            final List<ProjectionElemList> projections = multiProjection.getProjections();
	            final BindingSet prev[] = new BindingSet[projections.size()];
	
	            @Override
	            protected boolean next(BindingSet bs) {
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
	        }, bindings);
        };
    }

    /**
     * Precompile filter {@link ExpressionTuple}s query model nodes pushing the result to the parent BindingSetPipe.
     * @param filter holds the details of any FILTER expression in a SPARQL query and any sub-chains.
     */
    private BindingSetPipeEvaluationStep precompileFilter(final Filter filter) {
        BindingSetPipeEvaluationStep argStep = precompileTupleExpr(filter.getArg());
        ValuePipeQueryValueEvaluationStep conditionStep = parentStrategy.precompile(filter.getCondition(), evalContext);
        boolean isInSubQuery = isPartOfSubQuery(filter);
        return (parent, bindings) -> {
	        argStep.evaluate(new PipeJoin(parent, new ResultTracker(filter)) {
	            final Set<String> scopeBindingNames = filter.getBindingNames();
	
	            @Override
	            protected boolean next(BindingSet bs) {
                    // Limit the bindings to the ones that are in scope for this filter
                    QueryBindingSet scopeBindings = new QueryBindingSet(bs);
                    // FIXME J1 scopeBindingNames should include bindings from superquery if the filter
                    // is part of a subquery. This is a workaround: we should fix the settings of scopeBindingNames,
                    // rather than skipping the limiting of bindings.
                    if (!isInSubQuery) {
                        scopeBindings.retainAll(scopeBindingNames);
                    }
                    startSecondaryPipe();
	            	conditionStep.evaluate(new ValuePipe(null) {
	            		@Override
	            		protected void next(Value v) {
	            			if (QueryEvaluationUtility.getEffectiveBooleanValue(v).orElse(false)) {
		                        pushToParent(bs);
	            			}
	            			endSecondaryPipe();
	            		}
	            		@Override
	            		public void handleValueError(String msg) {
	            			// ignore - failed to evaluate condition
	            			endSecondaryPipe();
	            		}
	            	}, scopeBindings);
	            	return !parent.isClosed();
	            }
	            @Override
	            public String toString() {
	            	return "FilterBindingSetPipe";
	            }
	        }, bindings);
	    };
    }

    /**
     * Precompile {@link DescribeOperator} query model nodes
     * @param operator
     */
    private BindingSetPipeEvaluationStep precompileDescribeOperator(DescribeOperator operator) {
		BindingSetPipeQueryEvaluationStep argStep = precompile(operator.getArg());
		return (parent, bindings) -> {
			parentStrategy.executor.pullAndPushAsync(parent, bs -> new DescribeIteration(argStep.evaluate(bs), parentStrategy,
				operator.getBindingNames(), bs), operator, bindings, parentStrategy);
        };
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

        ComparableBindingSetWrapper(EvaluationStrategy strategy, BindingSet bs, List<OrderElem> elements, long minorOrder) throws QueryEvaluationException {
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
            if (bs.equals(o.bs)) return 0;
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
     * Precompile {@link Order} query model nodes
     * @param order
     */
    private BindingSetPipeEvaluationStep precompileOrder(final Order order) {
        final Sorter<ComparableBindingSetWrapper> sorter = new Sorter<>(getLimit(order), isReducedOrDistinct(order), collectionMemoryThreshold);
        BindingSetPipeEvaluationStep step = precompileTupleExpr(order.getArg());
        return (parent, bindings) -> {
	        step.evaluate(new BindingSetPipe(parent) {
	            final AtomicLong minorOrder = new AtomicLong();
	
	            @Override
	            public boolean handleException(Throwable e) {
	                sorter.close();
	                return parent.handleException(e);
	            }
	
	            @Override
	            protected boolean next(BindingSet bs) {
	                try {
	                    ComparableBindingSetWrapper cbsw = new ComparableBindingSetWrapper(parentStrategy, bs, order.getElements(), minorOrder.getAndIncrement());
	                    sorter.add(cbsw);
	                    return true;
	                } catch (QueryEvaluationException | IOException e) {
	                    return handleException(e);
	                }
	            }

	        	
	            private void pushOrdered() {
                    for (Map.Entry<ComparableBindingSetWrapper, Long> me : sorter) {
                        for (long i = me.getValue(); i > 0; i--) {
                            if (!parent.push(me.getKey().bs)) {
                                return;
                            }
                        }
                    }
	            }

	            @Override
	            protected void doClose() {
	            	pushOrdered();
                    sorter.close();
                    parent.close();
	            }
	
	            @Override
	            public String toString() {
	            	return "OrderBindingSetPipe";
	            }
	        }, bindings);
        };
    }

    /**
     * Precompile {@link Group} query model nodes
     * @param group
     */
    private BindingSetPipeEvaluationStep precompileGroup(Group group) {
        BindingSetPipeEvaluationStep step = precompileTupleExpr(group.getArg());
        List<GroupElem> elems = group.getGroupElements();
        ValuePipeQueryValueEvaluationStep[] opArgSteps = new ValuePipeQueryValueEvaluationStep[elems.size()];
        for (int i=0; i<opArgSteps.length; i++) {
        	AggregateOperator op = elems.get(i).getOperator();
        	if (op instanceof UnaryValueOperator) {
        		ValueExpr arg = ((UnaryValueOperator)op).getArg();
        		if (arg != null) {
        			opArgSteps[i] = parentStrategy.precompile(arg, evalContext);
        		}
        	}
		}
    	if (group.getGroupBindingNames().isEmpty()) {
    		// no GROUP BY present
    		return (parent, bindings) -> {
    	    	parentStrategy.initTracking(group);
	    		step.evaluate(new BindingSetPipe(parent) {
	    			final GroupValue aggregators = createGroupValue(group, opArgSteps, bindings);
					@Override
					protected boolean next(BindingSet bs) {
						if (parent.isClosed()) {
							return false;
						}
						aggregators.addValues(bs);
						return true;
					}
					@Override
					protected void doClose() {
						QueryBindingSet result = new QueryBindingSet(bindings);
						aggregators.bindResult(result);
						parentStrategy.incrementResultSizeActual(group);
						parent.pushLast(result);
						aggregators.close();
					}
					@Override
					public String toString() {
						return "AggregateBindingSetPipe-noGroupBy";
					}
	    		}, bindings);
    		};
    	} else {
    		return (parent, bindings) -> {
    	    	parentStrategy.initTracking(group);
	    		step.evaluate(new BindingSetPipe(parent) {
	    			final Map<BindingSetValues,GroupValue> groupByMap = new ConcurrentHashMap<>();
	    			final String[] groupNames = toStringArray(group.getGroupBindingNames());
					@Override
					protected boolean next(BindingSet bs) {
						GroupValue aggregators = groupByMap.computeIfAbsent(BindingSetValues.create(groupNames, bs), k -> createGroupValue(group, opArgSteps, bindings));
						aggregators.addValues(bs);
						return true;
					}
					@Override
					protected void doClose() {
						if (!groupByMap.isEmpty()) {
							for(Map.Entry<BindingSetValues,GroupValue> aggEntry : groupByMap.entrySet()) {
								BindingSetValues groupKey = aggEntry.getKey();
								MutableBindingSet result = groupKey.setBindings(groupNames, bindings);
								try (GroupValue aggregators = aggEntry.getValue()) {
									aggregators.bindResult(result);
								}
								parentStrategy.incrementResultSizeActual(group);
								parent.push(result);
							}
						} else {
							QueryBindingSet result = new QueryBindingSet(bindings);
							for (int i=0; i<opArgSteps.length; i++) {
								GroupElem ge = elems.get(i);
								Aggregator<?,?> agg = createAggregator(ge.getOperator(), opArgSteps[i], bindings);
								if (agg != null) {
									try {
										Value v = agg.getValue();
										if (v != null) {
											result.setBinding(ge.getName(), v);
										}
									} catch (ValueExprEvaluationException ignore) {
										// There was a type error when calculating the value of the aggregate. We silently ignore the error,
										// resulting in no result value being bound.
									} finally {
										agg.close();
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
	    		}, bindings);
    		};
    	}
    }

    private GroupValue createGroupValue(Group group, ValuePipeQueryValueEvaluationStep[] opArgSteps, BindingSet parentBindings) {
		Map<String,Aggregator<?,?>> aggregators = new HashMap<>();
        List<GroupElem> elems = group.getGroupElements();
		for (int i=0; i<opArgSteps.length; i++) {
			GroupElem ge = elems.get(i);
			Aggregator<?,?> agg = createAggregator(ge.getOperator(), opArgSteps[i], parentBindings);
			if (agg != null) {
				aggregators.put(ge.getName(), agg);
			}
		}
		return new GroupValue(aggregators);
    }

    private static final class GroupValue implements AutoCloseable {
		private final Map<String, Aggregator<?,?>> aggregators;

		GroupValue(Map<String, Aggregator<?,?>> aggregators) {
			this.aggregators = aggregators;
		}

		void addValues(BindingSet bindingSet) {
			for(Aggregator<?,?> agg : aggregators.values()) {
				agg.process(bindingSet);
			}
		}

		void bindResult(MutableBindingSet bs) {
			for(Map.Entry<String,Aggregator<?,?>> entry : aggregators.entrySet()) {
				try(Aggregator<?,?> agg = entry.getValue()) {
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
		public void close() {
			for (Aggregator<?,?> agg : aggregators.values()) {
				agg.close();
			}
		}

		@Override
		public String toString() {
			return aggregators.toString();
		}
    }

	private static final Predicate<?> ALWAYS_TRUE = (v) -> true;

	private Aggregator<?,?> createAggregator(AggregateOperator operator, ValuePipeQueryValueEvaluationStep opArgStep, BindingSet parentBindings) {
    	boolean isDistinct = operator.isDistinct();
		if (operator instanceof Count) {
			Count count = (Count) operator;
			if (count.getArg() != null) {
				QueryValueStepEvaluator eval = new QueryValueStepEvaluator(opArgStep);
				Predicate<Value> distinct = isDistinct ? createDistinctValues() : (Predicate<Value>) ALWAYS_TRUE;
				return Aggregator.create(new CountAggregateFunction(eval), distinct, new LongCollector(tripleSource.getValueFactory()));
			} else {
				Predicate<BindingSet> distinct = isDistinct ? createDistinctBindingSets() : (Predicate<BindingSet>) ALWAYS_TRUE;
				return Aggregator.create(new WildcardCountAggregateFunction(), distinct, new LongCollector(tripleSource.getValueFactory()));
			}
		} else if (operator instanceof Min) {
			QueryValueStepEvaluator eval = new QueryValueStepEvaluator(opArgStep);
			Predicate<Value> distinct = isDistinct ? createDistinctValues() : (Predicate<Value>) ALWAYS_TRUE;
			return Aggregator.create(new MinAggregateFunction(eval), distinct, new ValueCollector(parentStrategy.isStrict()));
		} else if (operator instanceof Max) {
			QueryValueStepEvaluator eval = new QueryValueStepEvaluator(opArgStep);
			Predicate<Value> distinct = isDistinct ? createDistinctValues() : (Predicate<Value>) ALWAYS_TRUE;
			return Aggregator.create(new MaxAggregateFunction(eval), distinct, new ValueCollector(parentStrategy.isStrict()));
		} else if (operator instanceof Sum) {
			QueryValueStepEvaluator eval = new QueryValueStepEvaluator(opArgStep);
			Predicate<Value> distinct = isDistinct ? createDistinctValues() : (Predicate<Value>) ALWAYS_TRUE;
			return Aggregator.create(new SumAggregateFunction(eval), distinct, new NumberCollector());
		} else if (operator instanceof Avg) {
			QueryValueStepEvaluator eval = new QueryValueStepEvaluator(opArgStep);
			Predicate<Value> distinct = isDistinct ? createDistinctValues() : (Predicate<Value>) ALWAYS_TRUE;
			return Aggregator.create(new AvgAggregateFunction(eval), distinct, new AvgCollector(tripleSource.getValueFactory()));
		} else if (operator instanceof Sample) {
			QueryValueStepEvaluator eval = new QueryValueStepEvaluator(opArgStep);
			Predicate<Value> distinct = (Predicate<Value>) ALWAYS_TRUE;
			return Aggregator.create(new SampleAggregateFunction(eval), distinct, new SampleCollector());
		} else if (operator instanceof GroupConcat) {
			GroupConcat grpConcat = (GroupConcat) operator;
			QueryValueStepEvaluator eval = new QueryValueStepEvaluator(opArgStep);
			Predicate<Value> distinct = isDistinct ? createDistinctValues() : (Predicate<Value>) ALWAYS_TRUE;
			String sep;
			ValueExpr sepExpr = grpConcat.getSeparator();
			if (sepExpr != null) {
				sep = parentStrategy.precompile(sepExpr, evalContext).evaluate(parentBindings).stringValue();
			} else {
				sep = " ";
			}
			return Aggregator.create(new ConcatAggregateFunction(eval), distinct, new CSVCollector(sep, tripleSource.getValueFactory()));
		} else if (operator instanceof AggregateFunctionCall) {
			AggregateFunctionCall aggFuncCall = (AggregateFunctionCall) operator;
			AggregateFunctionFactory aggFuncFactory = aggregateFunctionRegistry.get(aggFuncCall.getIRI())
				.orElseThrow(() -> new QueryEvaluationException("Unknown aggregate function '" + aggFuncCall.getIRI() + "'"));
			QueryValueStepEvaluator eval = new QueryValueStepEvaluator(opArgStep);
			AggregateFunction aggFunc = aggFuncFactory.buildFunction(eval);
			Predicate<Value> distinct = isDistinct ? createDistinctValues() : (Predicate<Value>) ALWAYS_TRUE;
			if (aggFunc.getClass().getAnnotation(ThreadSafe.class) != null) {
				return Aggregator.create(aggFunc, distinct, aggFuncFactory.getCollector());
			} else {
				return SynchronizedAggregator.create(aggFunc, distinct, aggFuncFactory.getCollector());
			}
		} else {
			return null;
		}
    }

	private DistinctValues createDistinctValues() {
		return new DistinctValues(collectionMemoryThreshold);
	}

	private DistinctBindingSets createDistinctBindingSets() {
		return new DistinctBindingSets(collectionMemoryThreshold);
	}

	private static class Aggregator<T extends AggregateCollector, D> implements AutoCloseable {
    	private final Predicate<D> distinctPredicate;
    	private final AggregateFunction<T, D> aggFunc;
    	private final T valueCollector;

    	static <T extends AggregateCollector,D> Aggregator<T,D> create(AggregateFunction<T,D> aggFunc, Predicate<D> distinctPredicate, T valueCollector) {
    		return new Aggregator<T,D>(aggFunc, distinctPredicate, valueCollector);
    	}

    	private Aggregator(AggregateFunction<T, D> aggFunc, Predicate<D> distinctPredicate, T valueCollector) {
			this.distinctPredicate = distinctPredicate;
			this.aggFunc = aggFunc;
			this.valueCollector = valueCollector;
		}

    	void process(BindingSet bs) {
    		aggFunc.processAggregate(bs, distinctPredicate, valueCollector);
    	}

    	Value getValue() {
    		return valueCollector.getFinalValue();
    	}

    	@Override
		public void close() {
			if (distinctPredicate instanceof AutoCloseable) {
				try {
					((AutoCloseable)distinctPredicate).close();
				} catch (Exception ignore) {
				}
			}
		}
    }

    private static class SynchronizedAggregator<T extends AggregateCollector, D> extends Aggregator<T, D> {
    	static <T extends AggregateCollector,D> SynchronizedAggregator<T,D> create(AggregateFunction<T,D> aggFunc, Predicate<D> distinctPredicate, T valueCollector) {
    		return new SynchronizedAggregator<T,D>(aggFunc, distinctPredicate, valueCollector);
    	}

    	private SynchronizedAggregator(AggregateFunction<T, D> aggFunc, Predicate<D> distinctPredicate, T valueCollector) {
    		super(aggFunc, distinctPredicate, valueCollector);
		}

    	@Override
    	void process(BindingSet bs) {
    		synchronized (this) {
    			super.process(bs);
    		}
    	}

    	@Override
    	Value getValue() {
    		synchronized (this) {
    			return super.getValue();
    		}
    	}
    }

	private static class DistinctValues implements Predicate<Value>, AutoCloseable {
		private final int threshold;
		private BigHashSet<Value> distinctValues;

		DistinctValues(int threshold) {
			this.threshold = threshold;
		}

		@Override
		public boolean test(Value v) {
			if (distinctValues == null) {
				distinctValues = BigHashSet.create(threshold);
			}
			try {
				return distinctValues.add(v);
			} catch (IOException e) {
				throw new QueryEvaluationException(e);
			}
		}

		@Override
		public void close() {
			if (distinctValues != null) {
				distinctValues.close();
				distinctValues = null;
			}
		}
	}

	private static class DistinctBindingSets implements Predicate<BindingSet>, AutoCloseable {
		private final int threshold;
		private BigHashSet<BindingSet> distinctBindingSets;

		DistinctBindingSets(int threshold) {
			this.threshold = threshold;
		}

		@Override
		public boolean test(BindingSet v) {
			if (distinctBindingSets == null) {
				distinctBindingSets = BigHashSet.create(threshold);
			}
			try {
				return distinctBindingSets.add(v);
			} catch (IOException e) {
				throw new QueryEvaluationException(e);
			}
		}

		@Override
		public void close() {
			if (distinctBindingSets != null) {
				distinctBindingSets.close();
				distinctBindingSets = null;
			}
		}
	}

    /**
     * Precompile {@link Reduced} query model nodes
     * @param reduced
     */
    private BindingSetPipeEvaluationStep precompileReduced(Reduced reduced) {
        BindingSetPipeEvaluationStep step = precompileTupleExpr(reduced.getArg());
        return (parent, bindings) -> {
	    	parentStrategy.initTracking(reduced);
	        step.evaluate(new BindingSetPipe(parent) {
	            private BindingSet previous = null;
	
	            @Override
	            protected boolean next(BindingSet bs) {
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
	        }, bindings);
        };
    }

    /**
     * Precompile {@link Distinct} query model nodes
     * @param distinct
     */
    private BindingSetPipeEvaluationStep precompileDistinct(final Distinct distinct) {
        BindingSetPipeEvaluationStep step = precompileTupleExpr(distinct.getArg());
        return (parent, bindings) -> {
	    	parentStrategy.initTracking(distinct);
	        step.evaluate(new BindingSetPipe(parent) {
	            private final BigHashSet<BindingSet> set = BigHashSet.create(collectionMemoryThreshold);
	            @Override
	            public boolean handleException(Throwable e) {
	                set.close();
	                return parent.handleException(e);
	            }
	            @Override
	            protected boolean next(BindingSet bs) {
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
				protected void doClose() {
	               	set.close();
	                parent.close();
	            }
	            @Override
	            public String toString() {
	            	return "DistinctBindingSetPipe";
	            }
	        }, bindings);
        };
    }

	/**
	 * Precompile {@link Extension} query model nodes
	 * @param extension
	 */
    private BindingSetPipeEvaluationStep precompileExtension(final Extension extension) {
        BindingSetPipeEvaluationStep argStep = precompileTupleExpr(extension.getArg());
        List<ExtensionElem> extElems = extension.getElements();
        List<org.apache.commons.lang3.tuple.Triple<String,ValuePipeQueryValueEvaluationStep,QueryEvaluationException>> nonAggs = new ArrayList<>(extElems.size());
        for (ExtensionElem extElem : extElems) {
        	ValueExpr expr = extElem.getExpr();
        	if (!(expr instanceof AggregateOperator)) {
        		ValuePipeQueryValueEvaluationStep elemStep;
        		QueryEvaluationException ex;
        		try {
        			elemStep = parentStrategy.precompile(expr, evalContext);
        			ex = null;
        		} catch (QueryEvaluationException e) {
        			elemStep = null;
        			ex = e;
        		}
        		nonAggs.add(org.apache.commons.lang3.tuple.Triple.of(extElem.getName(), elemStep, ex));
        	}
        }

        return (parent, bindings) -> {
	        argStep.evaluate(new BindingSetPipe(parent) {
	            @Override
	            protected boolean next(BindingSet bs) {
	                QueryBindingSet targetBindings = new QueryBindingSet(bs);
	                for (org.apache.commons.lang3.tuple.Triple<String,ValuePipeQueryValueEvaluationStep,QueryEvaluationException> nonAgg : nonAggs) {
	                	QueryEvaluationException ex = nonAgg.getRight();
	                	if (ex != null) {
	                		return handleException(ex);
	                	}
                		String extElemName = nonAgg.getLeft();
                		ValuePipeQueryValueEvaluationStep elemStep = nonAgg.getMiddle();
                        try {
                            // we evaluate each extension element over the targetbindings, so that bindings from
                            // a previous extension element in this same extension can be used by other extension elements.
                            // e.g. if a projection contains (?a + ?b as ?c) (?c * 2 as ?d)
                            Value targetValue = elemStep.evaluate(targetBindings);
                            if (targetValue != null) {
                                // Potentially overwrites bindings from super
                                targetBindings.setBinding(extElemName, targetValue);
                            }
                        } catch (ValueExprEvaluationException e) {
                            // silently ignore type errors in extension arguments. They should not cause the
                            // query to fail but result in no bindings for this solution
                            // see https://www.w3.org/TR/sparql11-query/#assignment
                            // use null as place holder for unbound variables that must remain so
                            targetBindings.setBinding(extElemName, null);
                        } catch (QueryEvaluationException e) {
                            return handleException(e);
                        }
	                }
	                return parent.push(targetBindings);
	            }
	            @Override
	            public String toString() {
	            	return "ExtensionBindingSetPipe";
	            }
	        }, bindings);
        };
    }

    /**
     * Precompile {@link Slice} query model nodes.
     * @param slice
     */
    private BindingSetPipeEvaluationStep precompileSlice(Slice slice) {
        final long offset = slice.hasOffset() ? slice.getOffset() : 0;
        final long limit = slice.hasLimit() ? offset + slice.getLimit() : Long.MAX_VALUE;
        BindingSetPipeEvaluationStep step = precompileTupleExpr(slice.getArg());
        return (parent, bindings) -> {
        	parentStrategy.initTracking(slice);
	        step.evaluate(new BindingSetPipe(parent) {
	            private final AtomicLong counter = new AtomicLong(0);
	            private final AtomicLong remaining = new AtomicLong(limit-offset);
	            @Override
	            protected boolean next(BindingSet bs) {
	                long l = counter.incrementAndGet();
	                if (l <= offset) {
	                    return true;
	                } else if (l <= limit) {
		            	parentStrategy.incrementResultSizeActual(slice);
		            	boolean pushMore = parent.push(bs);
		            	if (remaining.decrementAndGet() == 0) {
		            		// we're the last one so close
		            		close();
		            		pushMore = false;
		            	}
		            	return pushMore;
	                } else {
	                	return false;
	                }
	            }
	            @Override
	            public String toString() {
	            	return "SliceBindingSetPipe";
	            }
	        }, bindings);
        };
    }

    /**
     * Precompiles {@link Service} query model nodes
     * @param service
     */
    private BindingSetPipeEvaluationStep precompileService(Service service) {
        Var serviceRef = service.getServiceRef();
        Value serviceRefValue = serviceRef.getValue();
        if (serviceRefValue != null) {
        	// service endpoint known ahead of time
        	FederatedService fs = parentStrategy.getService(serviceRefValue.stringValue());
        	return (topPipe, bindings) -> evaluateService(topPipe, service, fs, bindings);
        } else {
			return (topPipe, bindings) -> {
				Value boundServiceRefValue = bindings.getValue(serviceRef.getName());
			    if (boundServiceRefValue != null) {
			        String serviceUri = boundServiceRefValue.stringValue();
		        	FederatedService fs = parentStrategy.getService(serviceUri);
					evaluateService(topPipe, service, fs, bindings);
			    } else {
			        topPipe.handleException(new QueryEvaluationException("SERVICE variables must be bound at evaluation time."));
			    }
			};
        }
    }

    private void evaluateService(BindingSetPipe topPipe, Service service, FederatedService fs, BindingSet bindings) {
        // create a copy of the free variables, and remove those for which
        // bindings are available (we can set them as constraints!)
        Set<String> freeVars = new HashSet<>(service.getServiceVars());
        freeVars.removeAll(bindings.getBindingNames());
        // Get bindings from values pre-bound into variables.
        MapBindingSet fsBindings = new MapBindingSet(bindings.size() + 1);
        for (Binding binding : bindings) {
            fsBindings.addBinding(binding.getName(), binding.getValue());
        }
        final Set<Var> boundVars = new HashSet<Var>();
        new AbstractExtendedQueryModelVisitor<RuntimeException> (){
            @Override
            public void meet(Var var) {
                if (var.hasValue()) {
                    boundVars.add(var);
                }
            }
        }.meet(service);
        for (Var boundVar : boundVars) {
            freeVars.remove(boundVar.getName());
            fsBindings.addBinding(boundVar.getName(), boundVar.getValue());
        }
        String baseUri = service.getBaseURI();
	    try {
	        // special case: no free variables => perform ASK query
	        if (freeVars.isEmpty()) {
	            // check if triples are available (with inserted bindings)
	            if (fs.ask(service, fsBindings, baseUri)) {
	                topPipe.push(fsBindings);
	            }
            	topPipe.close();
	        } else {
		        // otherwise: perform a SELECT query
		        BindingSetPipe pipe;
		    	if (service.isSilent()) {
		    		pipe = new BindingSetPipe(topPipe) {
		    			@Override
		    			public boolean handleException(Throwable thr) {
		    				close();
		    				return false;
		    			}
		    			@Override
		    			public String toString() {
		    				return "SilentBindingSetPipe";
		    			}
		    		};
		    	} else {
		    		pipe = topPipe;
		    	}
		        if (fs instanceof BindingSetPipeFederatedService) {
		        	((BindingSetPipeFederatedService)fs).select(pipe, service, freeVars, fsBindings, baseUri);
		        } else {
		            QueryEvaluationStep evalStep = bs -> fs.select(service, freeVars, bs, baseUri);
		            parentStrategy.executor.pullAndPushAsync(pipe, evalStep, service, fsBindings, parentStrategy);
		        }
	        }
	    } catch (QueryEvaluationException e) {
	        // suppress exceptions if silent
	        if (service.isSilent()) {
	            topPipe.pushLast(fsBindings);
	        } else {
	            topPipe.handleException(e);
	        }
	    } catch (RuntimeException e) {
	        // suppress special exceptions (e.g. UndeclaredThrowable with
	        // wrapped
	        // QueryEval) if silent
	        if (service.isSilent()) {
	            topPipe.pushLast(fsBindings);
	        } else {
	            topPipe.handleException(e);
	        }
	    }
    }

    /**
     * Precompiles {@link BinaryTupleOperator} query model nodes
     */
    private BindingSetPipeEvaluationStep precompileBinaryTupleOperator(BinaryTupleOperator expr) {
        if (expr instanceof Join) {
            return precompileJoin((Join) expr);
        } else if (expr instanceof LeftJoin) {
        	return precompileLeftJoin((LeftJoin) expr);
        } else if (expr instanceof Union) {
        	return precompileUnion((Union) expr);
        } else if (expr instanceof Intersection) {
        	return precompileIntersection((Intersection) expr);
        } else if (expr instanceof Difference) {
        	return precompileDifference((Difference) expr);
        } else if (expr == null) {
            throw new IllegalArgumentException("expr must not be null");
        } else {
            throw new QueryEvaluationException("Unsupported binary tuple operator type: " + expr.getClass());
        }
    }

    /**
     * Precompiles {@link Join} query model nodes.
     */
    private BindingSetPipeEvaluationStep precompileJoin(Join join) {
    	BindingSetPipeEvaluationStep step;
    	String algorithm = join.getAlgorithmName();
    	if (isOutOfScopeForLeftArgBindings(join.getRightArg())) {
    		algorithm = Algorithms.HASH_JOIN;
    	}

    	if (Algorithms.HASH_JOIN.equals(algorithm)) {
    		step = new HashJoinEvaluationStep(join);
    	} else {
    		step = precompileNestedLoopsJoin(join);
    	}
    	return step;
    }

    private static boolean isOutOfScopeForLeftArgBindings(TupleExpr expr) {
		return (TupleExprs.isVariableScopeChange(expr) || TupleExprs.containsSubquery(expr));
	}

    private BindingSetPipeEvaluationStep precompileNestedLoopsJoin(Join join) {
    	join.setAlgorithm(Algorithms.NESTED_LOOPS);
        BindingSetPipeEvaluationStep outerStep = precompileTupleExpr(join.getLeftArg());
        BindingSetPipeEvaluationStep innerStep = precompileTupleExpr(join.getRightArg());
        return precompileNestedLoopsJoin(outerStep, innerStep, new ResultTracker(join));
    }

    private BindingSetPipeEvaluationStep precompileNestedLoopsJoin(BindingSetPipeEvaluationStep outerStep, BindingSetPipeEvaluationStep innerStep, ResultTracker resultTracker) {
        return (topPipe, bindings) -> {
	        outerStep.evaluate(new PipeJoin(topPipe, resultTracker) {
	            @Override
	            protected boolean next(BindingSet bs) {
	            	startSecondaryPipe();
	                innerStep.evaluate(new BindingSetPipe(parent) {
	                	@Override
	                	protected boolean next(BindingSet bs) {
	                		return pushToParent(bs);
	                	}
	                    @Override
	    				protected void doClose() {
	                    	endSecondaryPipe();
	                    }
	                    @Override
	                    public String toString() {
	                    	return "JoinBindingSetPipe(inner)";
	                    }
	                }, bs);
	                return !parent.isClosed(); // innerStep is async, check if we've been closed
	            }
	            @Override
	            public String toString() {
	            	return "JoinBindingSetPipe(outer)";
	            }
	        }, bindings);
        };
    }

    /**
     * Precompiles {@link LeftJoin} query model nodes
     */
    private BindingSetPipeEvaluationStep precompileLeftJoin(LeftJoin leftJoin) {
    	BindingSetPipeEvaluationStep step;
    	String algorithm = leftJoin.getAlgorithmName();
    	if (TupleExprs.containsSubquery(leftJoin.getRightArg())) {
    		algorithm = Algorithms.HASH_JOIN;
    	}

    	if (Algorithms.HASH_JOIN.equals(algorithm)) {
    		step = new LeftHashJoinEvaluationStep(leftJoin);
    	} else {
    		step = precompileNestedLoopsLeftJoin(leftJoin);
    	}
    	return step;
    }

    private static QueryBindingSet getFilteredBindings(BindingSet bindings, Set<String> problemVars) {
        QueryBindingSet filteredBindings = new QueryBindingSet(bindings);
        filteredBindings.removeAll(problemVars);
        return filteredBindings;
    }

    private BindingSetPipeEvaluationStep precompileNestedLoopsLeftJoin(LeftJoin leftJoin) {
    	leftJoin.setAlgorithm(Algorithms.NESTED_LOOPS);
        BindingSetPipeEvaluationStep leftStep = precompileTupleExpr(leftJoin.getLeftArg());
        BindingSetPipeEvaluationStep rightStep = precompileTupleExpr(leftJoin.getRightArg());
        ValuePipeQueryValueEvaluationStep conditionStep = leftJoin.hasCondition() ? parentStrategy.precompile(leftJoin.getCondition(), evalContext) : null;
    	return (parentPipe, bindings) -> {
	    	// Check whether optional join is "well designed" as defined in section
	        // 4.2 of "Semantics and Complexity of SPARQL", 2006, Jorge Pérez et al.
	        VarNameCollector optionalVarCollector = new VarNameCollector();
	        leftJoin.getRightArg().visit(optionalVarCollector);
	        if (leftJoin.hasCondition()) {
	            leftJoin.getCondition().visit(optionalVarCollector);
	        }
	        final Set<String> problemVars = new HashSet<>(optionalVarCollector.getVarNames());
	        problemVars.removeAll(leftJoin.getLeftArg().getBindingNames());
	        problemVars.retainAll(bindings.getBindingNames());
	        final Set<String> scopeBindingNames = leftJoin.getBindingNames();
	        final BindingSetPipe topPipe;
	        final ResultTracker joinTracker;
	        if (problemVars.isEmpty()) {
	        	topPipe = parentPipe;
		        joinTracker = new ResultTracker(leftJoin);
	        } else {
	        	parentStrategy.initTracking(leftJoin);
	        	topPipe = new BindingSetPipe(parentPipe) {
		            //Handle badly designed left join
		            @Override
		            protected boolean next(BindingSet bs) {
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
		                    parentStrategy.incrementResultSizeActual(leftJoin);
		                    return parent.push(bs);
		            	} else {
		            		return true;
		            	}
		            }
		            @Override
		            public String toString() {
		            	return "LeftJoinBindingSetPipe(top)";
		            }
		        };
		        joinTracker = null;
	        }
	        leftStep.evaluate(new PipeJoin(topPipe, joinTracker) {
	        	@Override
	            protected boolean next(final BindingSet leftBindings) {
	        		startSecondaryPipe();
	                rightStep.evaluate(new BindingSetPipe(parent) {
	                    private boolean failed = true;
	                    @Override
	                    protected boolean next(BindingSet rightBindings) {
	                    	try {
	                            if (conditionStep == null) {
	                                failed = false;
	                                return pushToParent(rightBindings);
	                            } else {
	                                // Limit the bindings to the ones that are in scope for
	                                // this filter
	                                QueryBindingSet scopeBindings = new QueryBindingSet(rightBindings);
	                                scopeBindings.retainAll(scopeBindingNames);
	                                if (parentStrategy.isTrue(conditionStep, scopeBindings)) {
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
	                    @Override
	    				protected void doClose() {
	                        if (failed) {
	                            // Join failed, return left arg's bindings
	                        	pushToParent(leftBindings);
	                        }
	                        endSecondaryPipe();
	                    }
	                    @Override
	                    public String toString() {
	                    	return "LeftJoinBindingSetPipe(right)";
	                    }
	                }, leftBindings);
	                return !parent.isClosed(); // rightStep is async, check if we've been closed
	            }
	            @Override
	            public String toString() {
	            	return "LeftJoinBindingSetPipe(left)";
	            }
	        }, problemVars.isEmpty() ? bindings : getFilteredBindings(bindings, problemVars));
    	};
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

    private abstract class AbstractHashJoinEvaluationStep implements BindingSetPipeEvaluationStep {
    	private final BinaryTupleOperator join;
    	private final TupleExpr probeExpr;
    	private final TupleExpr buildExpr;
    	private final BindingSetPipeEvaluationStep probeStep;
    	private final BindingSetPipeEvaluationStep buildStep;
    	private final int hashTableLimit;
    	private final Set<String> joinAttributeSet;
    	private final String[] joinAttributes;
		private final String[] buildAttributes;
		private final int initialSize;

    	protected AbstractHashJoinEvaluationStep(BinaryTupleOperator join, int hashTableLimit) {
	    	join.setAlgorithm(Algorithms.HASH_JOIN);
    		this.join = join;
        	this.probeExpr = join.getLeftArg();
        	this.buildExpr = join.getRightArg();
        	this.probeStep = precompileTupleExpr(probeExpr);
        	this.buildStep = precompileTupleExpr(buildExpr);
        	this.hashTableLimit = hashTableLimit;
        	this.joinAttributeSet = getJoinAttributes();
        	this.joinAttributes = toStringArray(joinAttributeSet);
    		this.buildAttributes = toStringArray(buildExpr.getBindingNames());
    		this.initialSize = (int) Math.min(MAX_INITIAL_HASH_JOIN_TABLE_SIZE, Math.max(0, buildExpr.getResultSizeEstimate()));
    	}

    	private Set<String> getJoinAttributes() {
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

		@Override
		public void evaluate(BindingSetPipe parent, BindingSet bindings) {
			buildStep.evaluate(new PipeJoin(parent, new ResultTracker(join)) {
		    	HashJoinTable hashTable = createHashTable();
	            @Override
	            protected boolean next(BindingSet buildBs) {
					if (parent.isClosed()) {
						return false;
					}
	            	HashJoinTable partition;
	            	synchronized (this) {
	                	if (hashTable.entryCount() >= hashTableLimit) {
	                		partition = hashTable;
	                		hashTable = createHashTable();
	                	} else {
	                		partition = null;
	                	}
	            		hashTable.put(buildBs);
	            	}
	            	if (partition != null) {
	            		startJoin(partition, bindings, false);
	            	}
	            	return true;
	            }
	            @Override
				protected void doClose() {
	            	synchronized (this) {
	            		if (hashTable != null) {
	                   		startJoin(hashTable, bindings, true);
	            			hashTable = null;
	            		}
	            	}
	            }
	        	/**
	        	 * Performs a hash-join.
	        	 * @param hashTablePartition hash table to join against.
	        	 * @param bindings
	        	 * @param isLast true if this is the last time doJoin() will be called for the current join operation.
	        	 */
	        	private void startJoin(HashJoinTable hashTablePartition, BindingSet bindings, boolean isLast) {
	        		if (hashTablePartition.entryCount() == 0) {
	        			if (isLast) {
	        				parent.close();
	        			}
	        		} else {
	        			startSecondaryPipe(isLast);
	        			// NB: this part may execute asynchronously
	                	probeStep.evaluate(createPipe(this, hashTablePartition), bindings);
	        		}
	        	}
	            @Override
	            public String toString() {
	            	return "HashTableBindingSetPipe";
	            }
	    	}, bindings);
	    }

    	protected abstract BindingSetPipe createPipe(PipeJoin primary, HashJoinTable hashTablePartition);

    	abstract class AbstractHashJoinBindingSetPipe extends BindingSetPipe {
    		private final PipeJoin primary;
    		protected final HashJoinTable hashTablePartition;
    		protected AbstractHashJoinBindingSetPipe(PipeJoin primary, HashJoinTable hashTablePartition) {
				super(primary.getParent());
				this.primary = primary;
				this.hashTablePartition = hashTablePartition;
			}
			protected final boolean pushToParent(BindingSet bs) {
        		return primary.pushToParent(bs);
        	}
            @Override
			protected void doClose() {
            	primary.endSecondaryPipe();
        	}
    	}
    }

	final class HashJoinEvaluationStep extends AbstractHashJoinEvaluationStep {
		HashJoinEvaluationStep(Join join) {
			super(join, hashJoinLimit);
		}

		final class HashJoinBindingSetPipe extends AbstractHashJoinBindingSetPipe {
			protected HashJoinBindingSetPipe(PipeJoin primary, HashJoinTable hashTablePartition) {
				super(primary, hashTablePartition);
			}
			@Override
			protected boolean next(BindingSet probeBs) {
				if (probeBs.isEmpty()) {
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
				return !parent.isClosed();
			}
		    @Override
		    public String toString() {
		    	return "HashJoinBindingSetPipe";
		    }
		}

		@Override
		protected BindingSetPipe createPipe(PipeJoin primary, HashJoinTable hashTablePartition) {
			return new HashJoinBindingSetPipe(primary, hashTablePartition);
		}
	}

	final class LeftHashJoinEvaluationStep extends AbstractHashJoinEvaluationStep {
		LeftHashJoinEvaluationStep(LeftJoin join) {
			super(join, Integer.MAX_VALUE);
		}

		final class LeftHashJoinBindingSetPipe extends AbstractHashJoinBindingSetPipe {
			protected LeftHashJoinBindingSetPipe(PipeJoin primary, HashJoinTable hashTablePartition) {
				super(primary, hashTablePartition);
			}
			@Override
			protected boolean next(BindingSet probeBs) {
				if (probeBs.isEmpty()) {
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
				return !parent.isClosed();
			}
		    @Override
		    public String toString() {
		    	return "LeftHashJoinBindingSetPipe";
		    }
		}

		@Override
		protected BindingSetPipe createPipe(PipeJoin primary, HashJoinTable hashTablePartition) {
			return new LeftHashJoinBindingSetPipe(primary, hashTablePartition);
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
     * Precompiles {@link Union} query model nodes.
     * @param union
     */
    private BindingSetPipeEvaluationStep precompileUnion(Union union) {
        BindingSetPipeEvaluationStep leftStep = precompileTupleExpr(union.getLeftArg());
        BindingSetPipeEvaluationStep rightStep = precompileTupleExpr(union.getRightArg());
        return (parent, bindings) -> {
        	parentStrategy.initTracking(union);
            final AtomicInteger args = new AtomicInteger(2);
            // A pipe can only be closed once, so need separate instances
	        leftStep.evaluate(new UnionBindingSetPipe(parent, union, args), bindings);
	        rightStep.evaluate(new UnionBindingSetPipe(parent, union, args), bindings);
        };
    }

    final class UnionBindingSetPipe extends BindingSetPipe {
    	final Union union;
        final AtomicInteger args;
    	UnionBindingSetPipe(BindingSetPipe parent, Union union, AtomicInteger args) {
    		super(parent);
    		this.union = union;
    		this.args = args;
    	}
    	@Override
    	protected boolean next(BindingSet bs) {
            parentStrategy.incrementResultSizeActual(union);
    		boolean pushMore = parent.push(bs);
    		if (!pushMore) {
    			args.set(0);
    		}
    		return pushMore;
    	}
        @Override
		protected void doClose() {
            if (args.decrementAndGet() == 0) {
                parent.close();
            }
        }
        @Override
        public String toString() {
        	return "UnionBindingSetPipe";
        }
    }

    /**
     * Precompiles {@link Intersection} query model nodes
     * @param intersection
     */
    private BindingSetPipeEvaluationStep precompileIntersection(final Intersection intersection) {
        BindingSetPipeEvaluationStep rightStep = precompileTupleExpr(intersection.getRightArg());
        BindingSetPipeEvaluationStep leftStep = precompileTupleExpr(intersection.getLeftArg());
        return (topPipe, bindings) -> {
	        rightStep.evaluate(new BindingSetPipe(topPipe) {
	            private final BigHashSet<BindingSet> secondSet = BigHashSet.create(collectionMemoryThreshold);
	            @Override
	            public boolean handleException(Throwable e) {
	                secondSet.close();
	                return parent.handleException(e);
	            }
	            @Override
	            protected boolean next(BindingSet bs) {
					if (parent.isClosed()) {
						return false;
					}
	                try {
	                    secondSet.add(bs);
	                    return true;
	                } catch (IOException e) {
	                    return handleException(e);
	                }
	            }
	            @Override
				protected void doClose() {
	                leftStep.evaluate(new BindingSetPipe(parent) {
	                    @Override
	                    protected boolean next(BindingSet bs) {
	                        try {
	                            return secondSet.contains(bs) ? parent.push(bs) : true;
	                        } catch (IOException e) {
	                            return handleException(e);
	                        }
	                    }
	                    @Override
	    				protected void doClose() {
	                        secondSet.close();
	                        parent.close();
	                    }
	                    @Override
	                    public String toString() {
	                    	return "IntersectionBindingSetPipe(left)";
	                    }
	                }, bindings);
	            }
	            @Override
	            public String toString() {
	            	return "IntersectionBindingSetPipe(right)";
	            }
	        }, bindings);
        };
    }

    /**
     * Precompiles {@link Difference} query model nodes
     * @param difference
     */
    private BindingSetPipeEvaluationStep precompileDifference(final Difference difference) {
        BindingSetPipeEvaluationStep rightStep = precompileTupleExpr(difference.getRightArg());
        BindingSetPipeEvaluationStep leftStep = precompileTupleExpr(difference.getLeftArg());
        return (topPipe, bindings) -> {
	        rightStep.evaluate(new BindingSetPipe(topPipe) {
	            private final BigHashSet<BindingSet> excludeSet = BigHashSet.create(collectionMemoryThreshold);
	            @Override
	            public boolean handleException(Throwable e) {
	                excludeSet.close();
	                return parent.handleException(e);
	            }
	            @Override
	            protected boolean next(BindingSet bs) {
					if (parent.isClosed()) {
						return false;
					}
	                try {
	                    excludeSet.add(bs);
	                    return true;
	                } catch (IOException e) {
	                    return handleException(e);
	                }
	            }
	            @Override
				protected void doClose() {
	                leftStep.evaluate(new BindingSetPipe(parent) {
	                    @Override
	                    protected boolean next(BindingSet bs) {
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
	    				protected void doClose() {
	                        excludeSet.close();
	                        parent.close();
	                    }
	                    @Override
	                    public String toString() {
	                    	return "DifferenceBindingSetPipe(left)";
	                    }
	                }, bindings);
	            }
	            @Override
	            public String toString() {
	            	return "DifferenceBindingSetPipe(right)";
	            }
	        }, bindings);
        };
    }

    private BindingSetPipeEvaluationStep precompileStarJoin(StarJoin starJoin) {
    	List<? extends TupleExpr> args = starJoin.getArgs();
    	int i = args.size() - 1;
    	BindingSetPipeEvaluationStep step = precompileTupleExpr(args.get(i));
    	for (i--; i>=0; i--) {
    		step = precompileNestedLoopsJoin(precompileTupleExpr(args.get(i)), step, (i==0) ? new ResultTracker(starJoin) : null);
    	}
        return step;
    }

    /**
     * Precompile {@link SingletonSet} query model nodes
     * @param singletonSet
     */
    private BindingSetPipeEvaluationStep precompileSingletonSet(SingletonSet singletonSet) {
    	return (parent, bindings) -> {
			parentStrategy.initTracking(singletonSet);
            parentStrategy.incrementResultSizeActual(singletonSet);
            parent.pushLast(bindings);
    	};
    }

	/**
	 * Precompile {@link EmptySet} query model nodes
	 * @param emptySet
	 */
	private BindingSetPipeEvaluationStep precompileEmptySet(EmptySet emptySet) {
    	return (parent, bindings) -> {
			parentStrategy.initTracking(emptySet);
			parent.close(); // nothing to push
    	};
	}

	/**
	 * Precompiles {@link ZeroLengthPath} query model nodes
	 * @param zlp
	 */
	private BindingSetPipeEvaluationStep precompileZeroLengthPath(ZeroLengthPath zlp) {
		return (parent, bindings) -> {
			final Var subjVar = zlp.getSubjectVar();
			final Var objVar = zlp.getObjectVar();
			final Var contextVar = zlp.getContextVar();
			Value subj = Algebra.getVarValue(subjVar, bindings);
			Value obj = Algebra.getVarValue(objVar, bindings);
			if (subj != null && obj != null) {
				if (!subj.equals(obj)) {
					parent.close(); // nothing to push
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
					private final BigHashSet<Value> set = BigHashSet.create(collectionMemoryThreshold);
					@Override
					protected boolean next(BindingSet bs) {
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
					private boolean nextValue(Value v, Value ctx) {
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
					parent.push(result);
				}
				parent.close();
			}
		};
	}

	/**
     * Precompiles {@link ArbitraryLengthPath} query model nodes
     * @param alp
     */
    private BindingSetPipeEvaluationStep precompileArbitraryLengthPath(ArbitraryLengthPath alp) {
    	return (parent, bindings) -> {
	        final StatementPattern.Scope scope = alp.getScope();
	        final Var subjectVar = alp.getSubjectVar();
	        final TupleExpr pathExpression = alp.getPathExpression();
	        final Var objVar = alp.getObjectVar();
	        final Var contextVar = alp.getContextVar();
	        final long minLength = alp.getMinLength();
	        //temporary solution using copy of the original iterator
	        //re-writing this to push model is a bit more complex task
	        try {
	        	parentStrategy.executor.pullAndPushAsync(parent, bs -> new PathIteration(new StrictEvaluationStrategy(null, null) {
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
	
	            }, scope, subjectVar, pathExpression, objVar, contextVar, minLength, bs), alp, bindings, parentStrategy);
	        } catch (QueryEvaluationException e) {
	            parent.handleException(e);
	        }
    	};
    }

    /**
     * Precompiles {@link BindingSetAssignment} query model nodes
     * @param bsa
     */
    private BindingSetPipeEvaluationStep precompileBindingSetAssignment(BindingSetAssignment bsa) {
    	return (parent, bindings) -> {
	        if (bindings.isEmpty()) {
	    		try {
		        	for (BindingSet b : bsa.getBindingSets()) {
		        		if (!parent.push(b)) {
		        			return;
		        		}
		        	}
	            } finally {
	            	parent.close();
	            }
	        } else {
	    		try {
		        	for (BindingSet assignedBindings : bsa.getBindingSets()) {
	                    QueryBindingSet result;
	                    if (assignedBindings.isEmpty()) {
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
	        }
    	};
    }

    /**
	 * Precompile {@link TupleFunctionCall} query model nodes
	 * 
	 * @param tfc
	 */
	private BindingSetPipeEvaluationStep precompileTupleFunctionCall(ExtendedTupleFunctionCall tfc)
			throws QueryEvaluationException {
		TupleFunction func = tupleFunctionRegistry.get(tfc.getURI())
				.orElseThrow(() -> new QueryEvaluationException("Unknown tuple function '" + tfc.getURI() + "'"));

		List<ValueExpr> args = tfc.getArgs();
		ValuePipeQueryValueEvaluationStep[] argSteps = new ValuePipeQueryValueEvaluationStep[args.size()];
		for (int i = 0; i < args.size(); i++) {
			argSteps[i] = parentStrategy.precompile(args.get(i), evalContext);
		}

		Function<BindingSetPipe,BindingSetPipe> pipeBuilder = parent -> {
			return new BindingSetPipe(parent) {
				@Override
				protected boolean next(BindingSet bs) {
					try {
						Value[] argValues = new Value[argSteps.length];
						for (int i = 0; i < argSteps.length; i++) {
							argValues[i] = argSteps[i].evaluate(bs);
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
						LOGGER.trace("Failed to evaluate " + tfc.getURI(), ignore);
					}
					return true;
				}
				@Override
				public String toString() {
					return "TupleFunctionCallBindingSetPipe";
				}
			};
		};

		TupleExpr depExpr = tfc.getDependentExpression();
		if (depExpr != null) {
			BindingSetPipeEvaluationStep step = precompileTupleExpr(depExpr);
			return (parent, bindings) -> {
				step.evaluate(pipeBuilder.apply(parent), bindings);
			};
		} else {
			// dependencies haven't been identified, but we'll try to evaluate anyway
			return (parent, bindings) -> {
				BindingSetPipe pipe = pipeBuilder.apply(parent);
				pipe.pushLast(bindings);
			};
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


    final class ResultTracker {
    	final TupleExpr node;
    	ResultTracker(TupleExpr node) {
    		this.node = node;
    	}
    	void initTracking() {
	    	parentStrategy.initTracking(node);
    	}
    	void incrementResultSize() {
    		parentStrategy.incrementResultSizeActual(node);
    	}
    }

    static abstract class PipeJoin extends BindingSetPipe {
    	private final AtomicLong inProgress = new AtomicLong();
    	private final AtomicBoolean finished = new AtomicBoolean();
    	private final ResultTracker resultTracker;

    	PipeJoin(BindingSetPipe parent, ResultTracker resultTracker) {
    		super(parent);
    		this.resultTracker = resultTracker;
    		if (resultTracker != null) {
    			resultTracker.initTracking();
    		}
    	}
    	protected BindingSetPipe getParent() {
    		return parent;
    	}
    	protected final void startSecondaryPipe() {
    		inProgress.incrementAndGet();
    	}
    	/**
    	 * @param isLast if known else false
    	 */
    	protected final void startSecondaryPipe(boolean isLast) {
    		inProgress.incrementAndGet();
    		if (isLast) {
    			finished.set(true);
    		}
    	}
    	protected final boolean pushToParent(BindingSet bs) {
    		if (resultTracker != null) {
    			resultTracker.incrementResultSize();
    		}
    		boolean more = parent.push(bs);
    		if (!more) {
    			finished.set(true);
    		}
    		return more;
    	}
    	protected final void endSecondaryPipe() {
    		inProgress.decrementAndGet();
    		// close if we are the last child and the main pipe has already finished
    		if (finished.get() && inProgress.compareAndSet(0L, -1L)) {
    			parent.close();
    		}
    	}
    	@Override
    	protected void doClose() {
    		finished.set(true);
    		// close if all children have already finished
    		if (inProgress.compareAndSet(0L, -1L)) {
    			parent.close();
    		}
    	}
    }
}
