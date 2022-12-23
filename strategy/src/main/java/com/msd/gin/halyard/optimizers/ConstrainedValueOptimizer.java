package com.msd.gin.halyard.optimizers;

import com.msd.gin.halyard.algebra.AbstractExtendedQueryModelVisitor;
import com.msd.gin.halyard.algebra.Algebra;
import com.msd.gin.halyard.algebra.BGPCollector;
import com.msd.gin.halyard.algebra.ConstrainedStatementPattern;
import com.msd.gin.halyard.common.ValueType;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.BooleanLiteral;
import org.eclipse.rdf4j.model.vocabulary.AFN;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.Compare;
import org.eclipse.rdf4j.query.algebra.Compare.CompareOp;
import org.eclipse.rdf4j.query.algebra.Datatype;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.IsBNode;
import org.eclipse.rdf4j.query.algebra.IsLiteral;
import org.eclipse.rdf4j.query.algebra.IsNumeric;
import org.eclipse.rdf4j.query.algebra.IsURI;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.Lang;
import org.eclipse.rdf4j.query.algebra.LocalName;
import org.eclipse.rdf4j.query.algebra.Namespace;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TripleRef;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryValueOperator;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;

public class ConstrainedValueOptimizer implements QueryOptimizer {

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		tupleExpr.visit(new ConstraintScanner(bindings));
	}

	static final class ConstraintScanner extends AbstractExtendedQueryModelVisitor<RuntimeException> {
		final BindingSet bindings;

		ConstraintScanner(BindingSet bindings) {
			this.bindings = bindings;
		}

		private void processGraphPattern(ConstraintCollector gpc) {
			for (StatementPattern sp: gpc.getStatementPatterns()) {
				Var s = sp.getSubjectVar();
				if (!s.hasValue()) {
					VarConstraint constraint = gpc.varConstraints.get(s.getName());
					if (constraint != null) {
						ConstrainedStatementPattern csp = ConstrainedStatementPattern.replace(sp);
						csp.setSubjectType(constraint.valueType);
					}
				}
				Var o = sp.getObjectVar();
				if (!o.hasValue()) {
					VarConstraint constraint = gpc.varConstraints.get(o.getName());
					if (constraint != null) {
						ConstrainedStatementPattern csp = ConstrainedStatementPattern.replace(sp);
						if (constraint.valueType == ValueType.LITERAL) {
							csp.setLiteralConstraint(constraint.literalConstraintFunction, constraint.literalConstraintValue);
						} else {
							csp.setObjectType(constraint.valueType);
						}
					}
				}
			}
		}

		@Override
		public void meet(StatementPattern node) {
			// skip children
		}

		@Override
		public void meet(Filter filter) {
			ConstraintCollector collector = new ConstraintCollector(this, bindings);
			filter.visit(collector);
			processGraphPattern(collector);
		}

		@Override
		public void meet(Join join) {
			ConstraintCollector collector = new ConstraintCollector(this, bindings);
			join.visit(collector);
			processGraphPattern(collector);
		}
	}


	static final class ConstraintCollector extends BGPCollector<RuntimeException> {
		final Map<String,VarConstraint> varConstraints = new HashMap<>();
		final BindingSet bindings;

		ConstraintCollector(QueryModelVisitor<RuntimeException> visitor, BindingSet bindings) {
			super(visitor);
			this.bindings = bindings;
		}

		@Override
		public void meet(Filter filter) {
			ValueExpr condition = filter.getCondition();
			if (condition instanceof Compare) {
				Compare cmp = (Compare) filter.getCondition();
				if (cmp.getLeftArg() instanceof UnaryValueOperator) {
					UnaryValueOperator func = (UnaryValueOperator) cmp.getLeftArg();
					if (func.getArg() instanceof Var) {
						Var var = (Var) func.getArg();
						if (func instanceof Datatype || func instanceof Lang) {
							if (cmp.getOperator() == CompareOp.EQ) {
								varConstraints.put(var.getName(), new VarConstraint(ValueType.LITERAL, func, cmp.getRightArg()));
							} else {
								varConstraints.put(var.getName(), new VarConstraint(ValueType.LITERAL));
							}
						} else if (cmp.getOperator() == CompareOp.EQ && BooleanLiteral.TRUE.equals(getValue(cmp.getRightArg()))) {
							if (func instanceof IsLiteral) {
								varConstraints.put(var.getName(), new VarConstraint(ValueType.LITERAL));
							} else if (func instanceof IsURI) {
								varConstraints.put(var.getName(), new VarConstraint(ValueType.IRI));
							} else if (func instanceof IsBNode) {
								varConstraints.put(var.getName(), new VarConstraint(ValueType.BNODE));
							} else if (func instanceof IsNumeric) {
								varConstraints.put(var.getName(), new VarConstraint(ValueType.LITERAL, func, cmp.getRightArg()));
							}
						} else if (func instanceof LocalName || func instanceof Namespace) {
							varConstraints.put(var.getName(), new VarConstraint(ValueType.IRI));
						}
					}
				} else if (cmp.getLeftArg() instanceof FunctionCall) {
					FunctionCall funcCall = (FunctionCall) cmp.getLeftArg();
					if (AFN.LOCALNAME.stringValue().equals(funcCall.getURI()) && funcCall.getArgs().get(0) instanceof Var) {
						varConstraints.put(((Var) funcCall.getArgs().get(0)).getName(), new VarConstraint(ValueType.IRI));
					}
				} else if (cmp.getLeftArg() instanceof Var && isLiteral(cmp.getRightArg())) {
					varConstraints.put(((Var) cmp.getLeftArg()).getName(), new VarConstraint(ValueType.LITERAL));
				}
			} else if (condition instanceof UnaryValueOperator) {
				UnaryValueOperator func = (UnaryValueOperator) condition;
				if (func.getArg() instanceof Var) {
					Var var = (Var) func.getArg();
					if (func instanceof IsLiteral) {
						varConstraints.put(var.getName(), new VarConstraint(ValueType.LITERAL));
					} else if (func instanceof IsURI) {
						varConstraints.put(var.getName(), new VarConstraint(ValueType.IRI));
					} else if (func instanceof IsBNode) {
						varConstraints.put(var.getName(), new VarConstraint(ValueType.BNODE));
					} else if (func instanceof IsNumeric) {
						varConstraints.put(var.getName(), new VarConstraint(ValueType.LITERAL, func, new ValueConstant(BooleanLiteral.TRUE)));
					}
				}
			}

			filter.getArg().visit(this);
		}

		private Value getValue(ValueExpr expr) {
			if (expr instanceof ValueConstant) {
				return ((ValueConstant) expr).getValue();
			} else if (expr instanceof Var) {
				return Algebra.getVarValue((Var) expr, bindings);
			} else {
				return null;
			}
		}

		private boolean isLiteral(ValueExpr expr) {
			Value v = getValue(expr);
			return (v != null) && v.isLiteral();
		}

		@Override
		public void meet(TripleRef tripleRef) {
			varConstraints.merge(tripleRef.getExprVar().getName(), new VarConstraint(ValueType.TRIPLE), this::removeOnConflict);
		}

		private VarConstraint removeOnConflict(VarConstraint oldConstraint, VarConstraint newConstraint) {
			return null;
		}
	}


	static final class VarConstraint {
		ValueType valueType;
		UnaryValueOperator literalConstraintFunction;
		ValueExpr literalConstraintValue;

		VarConstraint(ValueType t) {
			this.valueType = t;
		}

		VarConstraint(ValueType t, UnaryValueOperator func, ValueExpr value) {
			this.valueType = t;
			this.literalConstraintFunction = func;
			this.literalConstraintValue = value;
		}
	}
}
