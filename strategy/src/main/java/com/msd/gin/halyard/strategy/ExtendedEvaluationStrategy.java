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
import org.eclipse.rdf4j.query.algebra.evaluation.QueryValueEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.function.datetime.Now;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.TupleFunctionEvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.util.QueryEvaluationUtil;
import org.eclipse.rdf4j.query.algebra.evaluation.util.XMLDatatypeMathUtil;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;

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
	protected QueryValueEvaluationStep prepare(Compare node, QueryEvaluationContext context) {
		return supplyBinaryValueEvaluation(node, (leftVal, rightVal) -> BooleanLiteral
				.valueOf(QueryEvaluationUtil.compare(leftVal, rightVal, node.getOperator(), false)), context);
	}

	@Override
	public Value evaluate(MathExpr node, BindingSet bindings)
			throws QueryEvaluationException {
		Value leftVal = evaluate(node.getLeftArg(), bindings);
		Value rightVal = evaluate(node.getRightArg(), bindings);

		return mathOperationApplier(node, leftVal, rightVal);
	}

	private Value mathOperationApplier(MathExpr node, Value leftVal, Value rightVal) {
		if (leftVal.isLiteral() && rightVal.isLiteral()) {
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

	@Override
	public QueryValueEvaluationStep prepare(FunctionCall node, QueryEvaluationContext context)
			throws QueryEvaluationException {
		Function function = funcRegistry
				.get(node.getURI())
				.orElseThrow(() -> new QueryEvaluationException("Unknown function '" + node.getURI() + "'"));

		// the NOW function is a special case as it needs to keep a shared
		// return
		// value for the duration of the query.
		if (function instanceof Now) {
			return prepare((Now) function, context);
		}

		List<ValueExpr> args = node.getArgs();

		QueryValueEvaluationStep[] argSteps = new QueryValueEvaluationStep[args.size()];

		boolean allConstant = determineIfFunctionCallWillBeAConstant(context, function, args, argSteps);
		if (allConstant) {
			Value[] argValues = evaluateAllArguments(args, argSteps, EmptyBindingSet.getInstance());
			Value res = function.evaluate(tripleSource, argValues);
			return new QueryValueEvaluationStep.ConstantQueryValueEvaluationStep(res);
		} else {
			return bindings -> {
				Value[] argValues = evaluateAllArguments(args, argSteps, bindings);
				return function.evaluate(tripleSource, argValues);
			};
		}
	}

	/**
	 * If all input is constant normally the function call output will be constant as well.
	 *
	 * @param context  used to precompile arguments of the function
	 * @param function that might be constant
	 * @param args     that the function must evaluate
	 * @param argSteps side effect this array is filled
	 * @return if this function resolves to a constant value
	 */
	private boolean determineIfFunctionCallWillBeAConstant(QueryEvaluationContext context, Function function,
			List<ValueExpr> args, QueryValueEvaluationStep[] argSteps) {
		boolean allConstant = true;
		if (function.mustReturnDifferentResult()) {
			allConstant = false;
			for (int i = 0; i < args.size(); i++) {
				argSteps[i] = precompile(args.get(i), context);
			}
		} else {
			for (int i = 0; i < args.size(); i++) {
				argSteps[i] = precompile(args.get(i), context);
				if (!argSteps[i].isConstant()) {
					allConstant = false;
				}
			}
		}
		return allConstant;
	}

	private Value[] evaluateAllArguments(List<ValueExpr> args, QueryValueEvaluationStep[] argSteps,
			BindingSet bindings) {
		Value[] argValues = new Value[argSteps.length];
		for (int i = 0; i < args.size(); i++) {
			argValues[i] = argSteps[i].evaluate(bindings);
		}
		return argValues;
	}
}
