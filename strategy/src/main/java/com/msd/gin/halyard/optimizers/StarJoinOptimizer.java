/*
 * Copyright 2018 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
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
package com.msd.gin.halyard.optimizers;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.msd.gin.halyard.algebra.Algebra;
import com.msd.gin.halyard.algebra.StarJoin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.BGPCollector;

public class StarJoinOptimizer implements QueryOptimizer {

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		tupleExpr.visit(new StarJoinFinder());
	}

	static class StarJoinFinder extends AbstractQueryModelVisitor<RDF4JException> {

		@Override
		public void meet(Service node) throws RDF4JException {
			// skip service nodes - leave it to the remote endpoint to optimize
		}

		@Override
		public void meet(Join node) throws RDF4JException {
			BGPCollector<RDF4JException> collector = new BGPCollector<>(this);
			node.visit(collector);
			if(!collector.getStatementPatterns().isEmpty()) {
				Parent parent = new Parent();
				node.replaceWith(parent);
				parent.setArg(node);
				processJoins(parent, collector.getStatementPatterns());
			}
		}

		private void processJoins(Parent parent, List<StatementPattern> sps) {
			ListMultimap<Pair<Var,Var>, StatementPattern> spByCtxSubj = ArrayListMultimap.create(sps.size(), 4);
			for(StatementPattern sp : sps) {
				Var ctxVar = sp.getContextVar();
				Var subjVar = sp.getSubjectVar();
				spByCtxSubj.put(Pair.of(ctxVar != null ? ctxVar.clone() : null, subjVar.clone()), sp);
			}
			List<StarJoin> starJoins = new ArrayList<>(sps.size());
			for(Map.Entry<Pair<Var,Var>, Collection<StatementPattern>> entry : spByCtxSubj.asMap().entrySet()) {
				List<StatementPattern> subjSps = (List<StatementPattern>) entry.getValue();
				if(subjSps.size() > 1) {
					for(StatementPattern sp : subjSps) {
						Algebra.remove(sp);
					}
					starJoins.add(new StarJoin(entry.getKey().getRight(), entry.getKey().getLeft(), subjSps));
				}
			}

			if (!starJoins.isEmpty()) {
				Join combined = new Join();
				parent.replaceWith(combined);
				TupleExpr starJoinTree = Algebra.join(starJoins);
				combined.setLeftArg(parent.getArg());
				combined.setRightArg(starJoinTree);
			} else {
				parent.replaceWith(parent.getArg());
			}
		}
	}

	static final class Parent extends UnaryTupleOperator {
		private static final long serialVersionUID = 1620947330539139269L;

		@Override
		public <X extends Exception> void visit(QueryModelVisitor<X> visitor) throws X {
			visitor.meetOther(this);
		}

		@Override
		public boolean equals(Object other) {
			return other instanceof Parent && super.equals(other);
		}

		@Override
		public int hashCode() {
			return super.hashCode() ^ "Parent".hashCode();
		}

		@Override
		public Parent clone() {
			return (Parent) super.clone();
		}
	}
}
