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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Count;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;

import com.msd.gin.halyard.strategy.collections.BigHashSet;

public class CountAggregator extends Aggregator {

	private final boolean isWildcard;
	private final ValueFactory vf;
	private final AtomicLong count = new AtomicLong();

	private BigHashSet<BindingSet> distinctBindingSets;

	public CountAggregator(Count op, EvaluationStrategy strategy, ValueFactory vf) {
		super(op, strategy);
		this.isWildcard = (op.getArg() == null);
		this.vf = vf;
	}

	@Override
	public void process(BindingSet bs) {
		if (isWildcard) {
			// for a wildcarded count we need to filter on
			// bindingsets rather than individual values.
			if (bs.size() > 0 && distinctBindingSet(bs)) {
				count.incrementAndGet();
			}
		} else {
			Value value = evaluate(bs);
			if (value != null && distinctValue(value)) {
				count.incrementAndGet();
			}
		}
	}

	protected boolean distinctBindingSet(BindingSet s) {
		if (isDistinct()) {
			if (distinctBindingSets == null) {
				distinctBindingSets = BigHashSet.create();
			}
			try {
				return distinctBindingSets.add(s);
			} catch (IOException e) {
				throw new QueryEvaluationException(e);
			}
		} else {
			return true;
		}
	}

	@Override
	public Value getValue() {
		return vf.createLiteral(Long.toString(count.get()), XSD.INTEGER);
	}

	@Override
	public void close() {
		super.close();
		if (distinctBindingSets != null) {
			distinctBindingSets.close();
			distinctBindingSets = null;
		}
	}
}
