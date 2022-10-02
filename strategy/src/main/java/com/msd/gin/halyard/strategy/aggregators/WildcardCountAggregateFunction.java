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

import java.util.function.Predicate;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunction;

public final class WildcardCountAggregateFunction extends AggregateFunction<LongCollector,BindingSet> {

	public WildcardCountAggregateFunction() {
		super(null);
	}

	@Override
	public void processAggregate(BindingSet bs, Predicate<BindingSet> distinctPredicate, LongCollector col) {
		// for a wildcarded count we need to filter on
		// bindingsets rather than individual values.
		if (bs.size() > 0 && distinctPredicate.test(bs)) {
			col.increment();
		}
	}
}
