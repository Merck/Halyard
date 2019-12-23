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

import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.Min;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;

public class MinAggregator extends Aggregator {

	private final AtomicReference<Value> min = new AtomicReference<>();

	public MinAggregator(Min op, EvaluationStrategy strategy) {
		super(op, strategy);
	}

	@Override
	public void process(BindingSet bs) {
		Value v = evaluate(bs);
		if (v != null && distinctValue(v)) {
			min.accumulateAndGet(v, (current,next) -> {
				if (current == null || Aggregator.COMPARATOR.compare(next, current) < 0) {
					return next;
				} else {
					return current;
				}
			});
		}
	}

	@Override
	public Value getValue() {
		return min.get();
	}
}
