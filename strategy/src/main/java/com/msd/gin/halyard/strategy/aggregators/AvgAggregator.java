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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.Avg;
import org.eclipse.rdf4j.query.algebra.MathExpr.MathOp;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.util.MathUtil;

public class AvgAggregator extends Aggregator {

	private final ValueFactory vf;

	private final AtomicLong count = new AtomicLong();

	private final AtomicReference<Literal> sum = new AtomicReference<>(ZERO);

	private volatile ValueExprEvaluationException typeError = null;

	public AvgAggregator(Avg op, EvaluationStrategy strategy, ValueFactory vf) {
		super(op, strategy);
		this.vf = vf;
	}

	@Override
	public void process(BindingSet bs) {
		if (typeError != null) {
			// Prevent calculating the aggregate further if a type error has
			// occured.
			return;
		}

		Value v = evaluate(bs);
		if (distinctValue(v)) {
			if (v instanceof Literal) {
				Literal nextLiteral = (Literal) v;
				// check if the literal is numeric.
				if (nextLiteral.getDatatype() != null
						&& XMLDatatypeUtil.isNumericDatatype(nextLiteral.getDatatype())) {
					sum.accumulateAndGet(nextLiteral, (total,next) -> MathUtil.compute(total, next, MathOp.PLUS));
				} else {
					typeError = new ValueExprEvaluationException("not a number: " + v);
				}
				count.incrementAndGet();
			} else if (v != null) {
				// we do not actually throw the exception yet, but record it and
				// stop further processing. The exception will be thrown when
				// getValue() is invoked.
				typeError = new ValueExprEvaluationException("not a number: " + v);
			}
		}
	}

	@Override
	public Value getValue() {
		if (typeError != null) {
			// a type error occurred while processing the aggregate, throw it
			// now.
			throw typeError;
		}

		if (count.get() == 0) {
			return ZERO;
		} else {
			Literal sizeLit = vf.createLiteral(count.get());
			return MathUtil.compute(sum.get(), sizeLit, MathOp.DIVIDE);
		}
	}
}
