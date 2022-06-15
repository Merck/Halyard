/*******************************************************************************
 * Copyright (c) 2016 Eclipse RDF4J contributors.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *******************************************************************************/
package com.msd.gin.halyard.strategy;

import java.util.List;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.BooleanLiteral;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Compare;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.MathExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.function.datetime.Now;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.TupleFunctionEvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.util.QueryEvaluationUtil;
import org.eclipse.rdf4j.query.algebra.evaluation.util.XMLDatatypeMathUtil;

/**
 * SPARQL 1.1 extended query evaluation strategy. This strategy adds the use of virtual properties, as well as extended
 * comparison and mathematical operators to the minimally-conforming {@link org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategy}.
 *
 * @author Jeen Broekstra
 */
public class ExtendedEvaluationStrategy extends TupleFunctionEvaluationStrategy {
	private final FunctionRegistry funcRegistry;
	private boolean isStrict = false;

	public ExtendedEvaluationStrategy(TripleSource tripleSource, Dataset dataset,
			FederatedServiceResolver serviceResolver, TupleFunctionRegistry tupleFuncRegistry, FunctionRegistry funcRegistry,
			long iterationCacheSyncThreshold, EvaluationStatistics evaluationStatistics) {
		super(tripleSource, dataset, serviceResolver, tupleFuncRegistry, iterationCacheSyncThreshold, evaluationStatistics);
		this.funcRegistry = funcRegistry;
	}

	public void setStrict(boolean f) {
		isStrict = f;
	}

	public boolean isStrict() {
		return isStrict;
	}

	@Override
	public Value evaluate(Compare node, BindingSet bindings)
			throws ValueExprEvaluationException, QueryEvaluationException {
		Value leftVal = evaluate(node.getLeftArg(), bindings);
		Value rightVal = evaluate(node.getRightArg(), bindings);

		// return result of non-strict comparisson.
		return BooleanLiteral.valueOf(QueryEvaluationUtil.compare(leftVal, rightVal, node.getOperator(), isStrict));
	}

	@Override
	public Value evaluate(MathExpr node, BindingSet bindings)
			throws ValueExprEvaluationException, QueryEvaluationException {
		Value leftVal = evaluate(node.getLeftArg(), bindings);
		Value rightVal = evaluate(node.getRightArg(), bindings);

		if ((leftVal instanceof Literal) && (rightVal instanceof Literal)) {
			return XMLDatatypeMathUtil.compute((Literal) leftVal, (Literal) rightVal, node.getOperator());
		}

		throw new ValueExprEvaluationException("Both arguments must be literals");
	}

	/**
	 * Evaluates a function.
	 */
	@Override
	public Value evaluate(FunctionCall node, BindingSet bindings)
			throws QueryEvaluationException {
		Function function = funcRegistry
				.get(node.getURI())
				.orElseThrow(() -> new QueryEvaluationException("Unknown function '" + node.getURI() + "'"));

		// the NOW function is a special case as it needs to keep a shared
		// return
		// value for the duration of the query.
		if (function instanceof Now) {
			return evaluate((Now) function, bindings);
		}

		List<ValueExpr> args = node.getArgs();

		Value[] argValues = new Value[args.size()];

		for (int i = 0; i < args.size(); i++) {
			argValues[i] = evaluate(args.get(i), bindings);
		}

		return function.evaluate(tripleSource, argValues);

	}
}
