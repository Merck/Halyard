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
package com.msd.gin.halyard.strategy.aggregators;

import com.msd.gin.halyard.strategy.collections.BigHashSet;

import java.io.IOException;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.AbstractAggregateOperator;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.util.ValueComparator;

public abstract class Aggregator implements AutoCloseable {
	protected static final ValueComparator COMPARATOR = new ValueComparator();
	protected static final Literal ZERO = SimpleValueFactory.getInstance().createLiteral("0", XSD.INTEGER);
	private final ValueExpr arg;
	private final boolean isDistinct;
	private final EvaluationStrategy strategy;
	private BigHashSet<Value> distinctValues;

	public Aggregator(AbstractAggregateOperator op, EvaluationStrategy strategy) {
		this.arg = op.getArg();
		this.isDistinct = op.isDistinct();
		this.strategy = strategy;
	}

	protected final ValueExpr getArg() {
		return arg;
	}

	protected final boolean isDistinct() {
		return isDistinct;
	}

	/**
	 * Must be thread-safe.
	 */
	public abstract void process(BindingSet bs);
	public abstract Value getValue();

	@Override
	public void close() {
		if (distinctValues != null) {
			distinctValues.close();
			distinctValues = null;
		}
	}

	protected final Value evaluate(BindingSet bs) {
		try {
			return strategy.evaluate(arg, bs);
		} catch (ValueExprEvaluationException e) {
			return null; // treat missing or invalid expressions as null
		}
	}

	protected boolean distinctValue(Value v) {
		if (isDistinct) {
			if (distinctValues == null) {
				distinctValues = BigHashSet.create();
			}
			try {
				return distinctValues.add(v);
			} catch (IOException e) {
				throw new QueryEvaluationException(e);
			}
		} else {
			return true;
		}
	}
}
