package com.msd.gin.halyard.sail.geosparql;

import com.msd.gin.halyard.algebra.Algebra;
import com.msd.gin.halyard.algebra.ExtendedTupleFunctionCall;
import com.msd.gin.halyard.optimizers.HalyardFilterOptimizer;
import com.msd.gin.halyard.vocab.HALYARD;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.vocabulary.GEOF;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.Compare;
import org.eclipse.rdf4j.query.algebra.Compare.CompareOp;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.ExtensionElem;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

/**
 * ?s ?p ?to. bind(geof:distance(?from, ?to, ?units) as ?dist) filter(?dist &lt; ?distLimit)
 */
public class WithinDistanceInterpreter implements QueryOptimizer {
	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		HalyardFilterOptimizer.decomposeFilters(tupleExpr);
		HalyardFilterOptimizer.pushDownFilters(tupleExpr);
		rewrite(tupleExpr);
		HalyardFilterOptimizer.mergeFilters(tupleExpr);
	}

	private void rewrite(TupleExpr tupleExpr) {
		FunctionCallFinder finder = new FunctionCallFinder(GEOF.DISTANCE.stringValue());
		tupleExpr.visit(finder);
		for (FunctionCall funcCall : finder.funcCalls) {
			List<ValueExpr> args = funcCall.getArgs();
			if (args.size() != 3) {
				return;
			}

			Filter filter = null;
			ValueExpr distLimit = null;
			ExtensionElem distanceAssignment = null;
			QueryModelNode parent = funcCall.getParentNode();
			if (parent instanceof ExtensionElem) {
				// distance assigned to a var
				distanceAssignment = (ExtensionElem) parent;
				QueryModelNode extension = parent.getParentNode();
				Object[] rv = getFilterAndDistanceLimit(extension, distanceAssignment.getName());
				if (rv == null) {
					return;
				}
				filter = (Filter) rv[0];
				distLimit = (ValueExpr) rv[1];
			} else if (parent instanceof Compare) {
				// inline comparison
				filter = (Filter) parent.getParentNode();
				Compare compare = (Compare) parent;
				CompareOp op = compare.getOperator();
				if (op == CompareOp.LT && compare.getLeftArg() == funcCall) {
					distLimit = compare.getRightArg();
				} else if (op == CompareOp.GT && compare.getRightArg() == funcCall) {
					distLimit = compare.getLeftArg();
				}
			}

			if (distLimit != null) {
				ValueExpr fromExpr = args.get(0);
				ValueExpr toExpr = args.get(1);
				ValueExpr unitsExpr = args.get(2);

				if (toExpr instanceof Var) {
					Var toVar = (Var) toExpr;
					WithinDistanceCall call = new WithinDistanceCall(filter, fromExpr, toVar, unitsExpr, distanceAssignment, distLimit);
					StatementPattern sp = call.findChildStatementPattern();
					if (sp != null) {
						call.rewrite(sp);
					}
				}
			}
		}
	}


	static final class FunctionCallFinder extends AbstractQueryModelVisitor<RuntimeException> {
		final List<FunctionCall> funcCalls = new ArrayList<>();
		final String funcIri;

		FunctionCallFinder(String funcIri) {
			this.funcIri = funcIri;
		}

		@Override
		public void meet(FunctionCall funcCall) {
			if (funcCall.getURI().equals(funcIri)) {
				funcCalls.add(funcCall);
			}
		}
	}

	static final class WithinDistanceCall {
		final Filter filter;
		final ValueExpr fromExpr;
		final Var toVar;
		final ValueExpr unitsExpr;
		final ExtensionElem distanceAssignment;
		final ValueExpr distLimit;

		public WithinDistanceCall(Filter filter, ValueExpr fromExpr, Var toVar, ValueExpr unitsExpr, @Nullable ExtensionElem distanceAssignment, ValueExpr distLimit) {
			this.filter = filter;
			this.fromExpr = fromExpr;
			this.toVar = toVar;
			this.unitsExpr = unitsExpr;
			this.distanceAssignment = distanceAssignment;
			this.distLimit = distLimit;
		}

		private ExtendedTupleFunctionCall create() {
			ExtendedTupleFunctionCall tfc = new ExtendedTupleFunctionCall(HALYARD.WITHIN_DISTANCE.stringValue());
			tfc.addArg(fromExpr.clone());
			tfc.addArg(distLimit.clone());
			tfc.addArg(unitsExpr.clone());
			tfc.addResultVar(toVar.clone());
			if (distanceAssignment != null) {
				tfc.addArg(new ValueConstant(HALYARD.DISTANCE));
				tfc.addResultVar(new Var(distanceAssignment.getName()));
			}
			return tfc;
		}

		StatementPattern findChildStatementPattern() {
			class StatementPatternFinder extends AbstractQueryModelVisitor<RuntimeException> {
				final String toVarName = toVar.getName();
				StatementPattern statementPattern;

				@Override
				protected void meetBinaryTupleOperator(BinaryTupleOperator node) throws RuntimeException {
					if (statementPattern == null) {
						super.meetBinaryTupleOperator(node);
					}
				}

				@Override
				protected void meetUnaryTupleOperator(UnaryTupleOperator node) throws RuntimeException {
					if (statementPattern == null) {
						super.meetUnaryTupleOperator(node);
					}
				}

				@Override
				public void meet(StatementPattern sp) {
					if (statementPattern == null) {
						// ?to must appear as an object literal for it to have been indexed
						String objectVarName = sp.getObjectVar().getName();
						if (objectVarName.equals(toVarName)) {
							statementPattern = sp;
						}
					}
				}
			}
			StatementPatternFinder finder = new StatementPatternFinder();
			filter.getArg().visit(finder);
			return finder.statementPattern;
		}

		void rewrite(StatementPattern sp) {
			Algebra.remove(sp);
			if (distanceAssignment != null) {
				Extension extension = (Extension) distanceAssignment.getParentNode();
				List<ExtensionElem> elements = extension.getElements();
				if (elements.size() > 1) {
					elements.remove(distanceAssignment);
				} else {
					extension.replaceWith(extension.getArg());
				}
			}
			ExtendedTupleFunctionCall tfc = create();
			Join join = new Join(tfc, sp);
			filter.replaceWith(join);
			tfc.setDependentExpression(filter.getArg());
		}
	}

	private static Object[] getFilterAndDistanceLimit(QueryModelNode node, String distanceVarName) {
		QueryModelNode parent = node.getParentNode();
		while (parent != null) {
			if (parent instanceof Filter) {
				Filter f = (Filter) parent;
				ValueExpr condition = f.getCondition();
				if (condition instanceof Compare) {
					Compare compare = (Compare) condition;
					CompareOp op = compare.getOperator();
					ValueExpr distLimit = null;
					if (op == CompareOp.LT && distanceVarName.equals(getVarName(compare.getLeftArg()))) {
						distLimit = compare.getRightArg();
					} else if (op == CompareOp.GT && distanceVarName.equals(getVarName(compare.getRightArg()))) {
						distLimit = compare.getLeftArg();
					}
					return new Object[] { f, distLimit };
				}
			}
			parent = parent.getParentNode();
		}
		return null;
	}

	private static String getVarName(ValueExpr v) {
		if (v instanceof Var) {
			Var var = (Var) v;
			if (!var.isConstant()) {
				return var.getName();
			}
		}
		return null;
	}
}
