/*******************************************************************************
 * Copyright (c) 2015 Eclipse RDF4J contributors, Aduna, and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 *******************************************************************************/
package com.msd.gin.halyard.spin.function;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.SPIN;
import org.eclipse.rdf4j.model.vocabulary.SPINX;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.algebra.evaluation.util.TripleSources;

import com.msd.gin.halyard.spin.Argument;
import com.msd.gin.halyard.spin.SpinParser;

public class SpinxFunctionParser implements FunctionParser {

	private static final String CLASSPATH_SCHEME = "classpath:/";

	private final SpinParser parser;

	private final ScriptEngineManager scriptManager;

	public SpinxFunctionParser(SpinParser parser) {
		this.parser = parser;
		this.scriptManager = new ScriptEngineManager();
	}

	@Override
	public Function parse(IRI funcUri, TripleSource store) throws RDF4JException {
		Value codeValue = TripleSources.singleValue(funcUri, SPINX.JAVA_SCRIPT_CODE_PROPERTY, store);
		String code = (codeValue instanceof Literal) ? ((Literal) codeValue).getLabel() : null;
		Value fileValue = TripleSources.singleValue(funcUri, SPINX.JAVA_SCRIPT_FILE_PROPERTY, store);
		String file = (fileValue instanceof Literal) ? ((Literal) fileValue).getLabel() : null;
		if (code == null && file == null) {
			return null;
		}

		ScriptEngine engine = scriptManager.getEngineByName("javascript");
		if (engine == null) {
			throw new UnsupportedOperationException("No javascript engine available!");
		}

		try {
			if (file != null) {
				String location;
				int pos = file.indexOf(':');
				if (pos == -1) {
					location = funcUri.getNamespace() + file;
				} else {
					location = file;
				}
				URL url;
				if (location.startsWith(CLASSPATH_SCHEME)) {
					url = Thread.currentThread().getContextClassLoader().getResource(location.substring(CLASSPATH_SCHEME.length()));
				} else {
					url = new URL(location);
				}
				try (Reader reader = new InputStreamReader(
						url.openStream())) {
					engine.eval(reader);
				} catch (IOException e) {
					throw new QueryEvaluationException(e);
				}
			}
		} catch (ScriptException | MalformedURLException e) {
			throw new QueryEvaluationException(e);
		}

		SpinxFunction func = new SpinxFunction(funcUri.stringValue());
		func.setScriptEngine(engine);
		Value returnValue = TripleSources.singleValue(funcUri, SPIN.RETURN_TYPE_PROPERTY, store);
		func.setReturnType((returnValue instanceof IRI) ? (IRI) returnValue : null);

		Map<IRI, Argument> templateArgs = parser.parseArguments(funcUri, store);
		List<IRI> orderedArgs = SpinParser.orderArguments(templateArgs.keySet());

		String funcName = funcUri.getLocalName();
		StringBuilder codeBuf;
		if (code != null) {
			// wrap as a function
			codeBuf = new StringBuilder(code.length() + 100);
			codeBuf.append("function ");
			codeBuf.append(funcName);
			codeBuf.append("(");
			String sep = "";
			for (IRI argIri : orderedArgs) {
				codeBuf.append(sep);
				codeBuf.append(argIri.getLocalName());
				sep = ", ";
			}
			codeBuf.append(") {\n");
			codeBuf.append(code);
			codeBuf.append("\n}\n");
		} else {
			codeBuf = new StringBuilder(100);
		}

		// add function call
		codeBuf.append(funcName);
		codeBuf.append("(");
		String sep = "";
		for (IRI argIri : orderedArgs) {
			Argument arg = templateArgs.get(argIri);
			func.addArgument(arg);
			codeBuf.append(sep);
			codeBuf.append(argIri.getLocalName());
			sep = ", ";
		}
		codeBuf.append(");");
		func.setScript(codeBuf.toString());

		return func;
	}
}
