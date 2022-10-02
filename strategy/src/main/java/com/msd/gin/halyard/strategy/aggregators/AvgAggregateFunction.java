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

import com.msd.gin.halyard.strategy.QueryValueStepEvaluator;

import java.util.function.Predicate;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunction;

public class AvgAggregateFunction extends AggregateFunction<AvgCollector,Value> {

	public AvgAggregateFunction(QueryValueStepEvaluator evaluator) {
		super(evaluator);
	}

	@Override
	public void processAggregate(BindingSet bs, Predicate<Value> distinctPredicate, AvgCollector col) {
		if (col.hasError()) {
			// Prevent calculating the aggregate further if a type error has
			// occured.
			return;
		}

		Value v = evaluate(bs);
		if ( v != null) {
			if (v.isLiteral()) {
				if (distinctPredicate.test(v)) {
					Literal nextLiteral = (Literal) v;
					// check if the literal is numeric.
					CoreDatatype coreDatatype = nextLiteral.getCoreDatatype();
					if (coreDatatype.isXSDDatatype() && ((CoreDatatype.XSD) coreDatatype).isNumericDatatype()) {
						col.addValue(nextLiteral);
					} else {
						col.setError(new ValueExprEvaluationException("not a number: " + v));
					}
					col.incrementCount();
				}
			} else {
				// we do not actually throw the exception yet, but record it and
				// stop further processing. The exception will be thrown when
				// getValue() is invoked.
				col.setError(new ValueExprEvaluationException("not a number: " + v));
			}
		}
	}
}
