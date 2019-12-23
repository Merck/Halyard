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

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.GroupConcat;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;

public class ConcatAggregator extends Aggregator {

	private final ValueFactory vf;
	private final StringBuffer concatenated = new StringBuffer(1024);

	private final String separator;

	public ConcatAggregator(GroupConcat op, EvaluationStrategy strategy, BindingSet parentBindings, ValueFactory vf) {
		super(op, strategy);
		this.vf = vf;
		ValueExpr separatorExpr = op.getSeparator();
		if (separatorExpr != null) {
			Value separatorValue = strategy.evaluate(separatorExpr, parentBindings);
			separator = separatorValue.stringValue();
		} else {
			separator = " ";
		}
	}

	@Override
	public void process(BindingSet bs) {
		Value v = evaluate(bs);
		if (v != null && distinctValue(v)) {
			String newStr = v.stringValue()+separator;
			// atomically add newStr
			concatenated.append(newStr);
		}
	}

	@Override
	public Value getValue() {
		if (concatenated.length() == 0) {
			return vf.createLiteral("");
		}

		// remove separator at the end.
		int len = concatenated.length() - separator.length();
		return vf.createLiteral(concatenated.substring(0, len));
	}
}
