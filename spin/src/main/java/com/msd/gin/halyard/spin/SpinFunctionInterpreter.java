/*******************************************************************************
 * Copyright (c) 2015 Eclipse RDF4J contributors, Aduna, and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *******************************************************************************/
package com.msd.gin.halyard.spin;

import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.FN;
import org.eclipse.rdf4j.model.vocabulary.SPIF;
import org.eclipse.rdf4j.model.vocabulary.SPIN;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.msd.gin.halyard.spin.function.AskFunction;
import com.msd.gin.halyard.spin.function.EvalFunction;
import com.msd.gin.halyard.spin.function.spif.CanInvoke;
import com.msd.gin.halyard.spin.function.spif.ConvertSpinRDFToString;

/**
 * QueryOptimizer that adds support for SPIN functions.
 */
public class SpinFunctionInterpreter implements QueryOptimizer {

	private static final Logger logger = LoggerFactory.getLogger(SpinFunctionInterpreter.class);

	private final TripleSource tripleSource;

	private final SpinParser parser;

	private final FunctionRegistry functionRegistry;

	public static void registerSpinParsingFunctions(SpinParser spinParser, FunctionRegistry functionRegistry, TupleFunctionRegistry tupleFunctionRegistry) {
		if (!(functionRegistry.get(FN.CONCAT.stringValue()).orElse(null) instanceof com.msd.gin.halyard.spin.function.Concat)) {
			functionRegistry.add(new com.msd.gin.halyard.spin.function.Concat());
		}
		if (!functionRegistry.has(SPIN.EVAL_FUNCTION.stringValue())) {
			functionRegistry.add(new EvalFunction(spinParser));
		}
		if (!functionRegistry.has(SPIN.ASK_FUNCTION.stringValue())) {
			functionRegistry.add(new AskFunction(spinParser));
		}
		if (!functionRegistry.has(SPIF.CONVERT_SPIN_RDF_TO_STRING_FUNCTION.stringValue())) {
			functionRegistry.add(new ConvertSpinRDFToString(spinParser));
		}
		if (!functionRegistry.has(SPIF.CAN_INVOKE_FUNCTION.stringValue())) {
			functionRegistry.add(new CanInvoke(spinParser, functionRegistry, tupleFunctionRegistry));
		}
	}

	public SpinFunctionInterpreter(SpinParser parser, TripleSource tripleSource, FunctionRegistry functionRegistry) {
		this.parser = parser;
		this.tripleSource = tripleSource;
		this.functionRegistry = functionRegistry;
	}

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		try {
			tupleExpr.visit(new FunctionScanner());
		} catch (RDF4JException e) {
			logger.warn("Failed to parse function");
		}
	}

	private class FunctionScanner extends AbstractQueryModelVisitor<RDF4JException> {

		ValueFactory vf = tripleSource.getValueFactory();

		@Override
		public void meet(FunctionCall node) {
			String name = node.getURI();
			if (!functionRegistry.has(name)) {
				IRI funcUri = vf.createIRI(name);
				Function f = parser.parseFunction(funcUri, tripleSource);
				functionRegistry.add(f);
			}
			super.meet(node);
		}

		@Override
		public void meet(Service node) {
			// leave for the remote endpoint to interpret
		}
	}
}