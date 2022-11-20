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

import com.msd.gin.halyard.algebra.AbstractExtendedQueryModelVisitor;
import com.msd.gin.halyard.algebra.StarJoin;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.And;
import org.eclipse.rdf4j.query.algebra.Difference;
import org.eclipse.rdf4j.query.algebra.Distinct;
import org.eclipse.rdf4j.query.algebra.EmptySet;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Intersection;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.Order;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.Reduced;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractSimpleQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.collectors.VarNameCollector;

/**
 *
 * @author Adam Sotona (MSD)
 */
public final class HalyardFilterOptimizer implements QueryOptimizer {
    @Override
    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
    	decomposeFilters(tupleExpr);
    	pushDownFilters(tupleExpr);
    	mergeFilters(tupleExpr);
    }

    public static void decomposeFilters(TupleExpr tupleExpr) {
		tupleExpr.visit(new FilterUnMerger());
    }

    public static void pushDownFilters(TupleExpr tupleExpr) {
		tupleExpr.visit(new FilterOrganizer());
    }

    public static void mergeFilters(TupleExpr tupleExpr) {
		tupleExpr.visit(new FilterMerger());
    }



	private static class FilterUnMerger extends AbstractSimpleQueryModelVisitor<RuntimeException> {

		private FilterUnMerger() {
			super(false);
		}

		@Override
		public void meet(Filter filter) {
			if (filter.getCondition() instanceof And) {
				And and = (And) filter.getCondition();
				filter.setCondition(and.getLeftArg().clone());
				Filter newFilter = new Filter(filter.getArg().clone(), and.getRightArg().clone());
				filter.replaceChildNode(filter.getArg(), newFilter);
			}
			super.meet(filter);
		}
	}

	private static class FilterMerger extends AbstractSimpleQueryModelVisitor<RuntimeException> {

		private FilterMerger() {
			super(false);
		}

		// Halyard - bug fix
		@Override
		public void meet(Filter filter) {
			super.meet(filter);
			if (filter.getArg() instanceof Filter && filter.getParentNode() != null) {

				Filter childFilter = (Filter) filter.getArg();
				QueryModelNode parent = filter.getParentNode();
				And merge = new And(childFilter.getCondition().clone(), filter.getCondition().clone());

				Filter newFilter = new Filter(childFilter.getArg().clone(), merge);
				parent.replaceChildNode(filter, newFilter);
				meet(newFilter);
			}
		}
	}

	private static class FilterOrganizer extends AbstractSimpleQueryModelVisitor<RuntimeException> {

		public FilterOrganizer() {
			super(false);
		}

		@Override
		public void meet(Filter filter) {
			super.meet(filter);
			FilterRelocator.optimize(filter);
		}
	}

	private static class FilterRelocator extends /*Halyard*/AbstractExtendedQueryModelVisitor<RuntimeException> {

		private final Filter filter;
		private final List<String> filterVars;

		private FilterRelocator(Filter filter) {
			this.filter = filter;
			filterVars = new ArrayList<>(VarNameCollector.process(filter.getCondition()));
			filterVars.retainAll(filter.getArg().getBindingNames()); // Halyard
		}

		public static void optimize(Filter filter) {
			filter.visit(new FilterRelocator(filter));
		}

		@Override
		protected void meetNode(QueryModelNode node) {
			// By default, do not traverse
			assert node instanceof TupleExpr;
			relocate(filter, (TupleExpr) node);
		}

		// Halyard
		@Override
		public void meet(StarJoin node) {
			for (TupleExpr expr : node.getArgs()) {
				if (expr.getBindingNames().containsAll(filterVars)) {
					expr.visit(this);
					return;
				}
			}
			relocate(filter, node);
		}

		@Override
		public void meet(Join join) {
			if (join.getLeftArg().getBindingNames().containsAll(filterVars)) {
				// All required vars are bound by the left expr
				join.getLeftArg().visit(this);
			} else if (join.getRightArg().getBindingNames().containsAll(filterVars)) {
				// All required vars are bound by the right expr
				join.getRightArg().visit(this);
			} else {
				relocate(filter, join);
			}
		}

		@Override
		public void meet(StatementPattern sp) {
			if (sp.getBindingNames().containsAll(filterVars)) {
				// All required vars are bound by the left expr
				relocate(filter, sp);
			}
		}

		@Override
		public void meet(LeftJoin leftJoin) {
			if (leftJoin.getLeftArg().getBindingNames().containsAll(filterVars)) {
				leftJoin.getLeftArg().visit(this);
			} else {
				relocate(filter, leftJoin);
			}
		}

		@Override
		public void meet(Union union) {
			Filter clone = new Filter();
			clone.setCondition(filter.getCondition().clone());

			relocate(filter, union.getLeftArg());
			relocate(clone, union.getRightArg());

			FilterRelocator.optimize(filter);
			FilterRelocator.optimize(clone);
		}

		@Override
		public void meet(Difference node) {
			Filter clone = new Filter();
			clone.setCondition(filter.getCondition().clone());

			relocate(filter, node.getLeftArg());
			relocate(clone, node.getRightArg());

			FilterRelocator.optimize(filter);
			FilterRelocator.optimize(clone);
		}

		@Override
		public void meet(Intersection node) {
			Filter clone = new Filter();
			clone.setCondition(filter.getCondition().clone());

			relocate(filter, node.getLeftArg());
			relocate(clone, node.getRightArg());

			FilterRelocator.optimize(filter);
			FilterRelocator.optimize(clone);
		}

		@Override
		public void meet(Extension node) {
			if (node.getArg().getBindingNames().containsAll(filterVars)) {
				node.getArg().visit(this);
			} else {
				relocate(filter, node);
			}
		}

		@Override
		public void meet(EmptySet node) {
			if (filter.getParentNode() != null) {
				// Remove filter from its original location
				filter.replaceWith(filter.getArg().clone());
			}
		}

		@Override
		public void meet(Filter filter) {
			// Filters are commutative
			filter.getArg().visit(this);
		}

		@Override
		public void meet(Distinct node) {
			node.getArg().visit(this);
		}

		@Override
		public void meet(Order node) {
			node.getArg().visit(this);
		}

		@Override
		public void meet(QueryRoot node) {
			node.getArg().visit(this);
		}

		@Override
		public void meet(Reduced node) {
			node.getArg().visit(this);
		}

		private void relocate(Filter filter, TupleExpr newFilterArg) {
			if (filter.getArg() != newFilterArg) {
				if (filter.getParentNode() != null) {
					// Remove filter from its original location
					filter.replaceWith(filter.getArg());
				}

				// Insert filter at the new location
				newFilterArg.replaceWith(filter);
				filter.setArg(newFilterArg);
			}
		}
	}
}
