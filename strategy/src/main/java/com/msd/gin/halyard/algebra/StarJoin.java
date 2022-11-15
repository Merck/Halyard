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
package com.msd.gin.halyard.algebra;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.query.algebra.AbstractQueryModelNode;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.collectors.StatementPatternCollector;

/**
 * Collection of joins (incl. filters) that share a common subject var and context var (if present), e.g. ?s :p1 ?o1; :p2 ?o2; :p3 ?o3.
 * In some cases, it is faster to evaluate these as ?s ?p ?o then filter the results (?s known at evaluation time).
 */
public class StarJoin extends AbstractQueryModelNode implements TupleExpr {
	private static final long serialVersionUID = -4523270958311045771L;

	private Var commonVar;
	private Var contextVar;
	private TupleExpr[] args;

	public StarJoin(Var commonVar, @Nullable Var contextVar, List<StatementPattern> exprs) {
		assert exprs.size() > 1;
		setCommonVar(commonVar);
		setContextVar(contextVar);
		setStatementPatterns(exprs);
	}

	public void setCommonVar(Var var) {
		var.setParentNode(this);
		commonVar = var;
	}

	public Var getCommonVar() {
		return commonVar;
	}

	public void setContextVar(Var var) {
		if (var != null) {
			var.setParentNode(this);
		}
		contextVar = var;
	}

	public Var getContextVar() {
		return contextVar;
	}

	public List<? extends TupleExpr> getArgs() {
		return Arrays.asList(args);
	}

	public void setStatementPatterns(List<StatementPattern> exprs) {
		args = new TupleExpr[exprs.size()];
		for (int i=0; i<exprs.size(); i++) {
			StatementPattern sp = exprs.get(i);
			setStatementPattern(i, sp);
		}
	}

	private void setStatementPattern(int i, TupleExpr sp) {
		sp.setParentNode(this);
		args[i] = sp;
	}

	public List<Var> getVarList() {
		return getVars(new ArrayList<>(4*args.length));
	}

	public <L extends Collection<Var>> L getVars(L varCollection) {
		StatementPatternCollector spc = new StatementPatternCollector();
		for(TupleExpr expr : args) {
			expr.visit(spc);
		}
		for(StatementPattern sp : spc.getStatementPatterns()) {
			sp.getVars(varCollection);
		}
		return varCollection;
	}

	public Join toJoins() {
		return (Join) Algebra.join(getArgs());
	}

	@Override
	public <X extends Exception> void visit(QueryModelVisitor<X> visitor) throws X {
		visitor.meetOther(this);
	}

	@Override
	public <X extends Exception> void visitChildren(final QueryModelVisitor<X> visitor) throws X {
		commonVar.visit(visitor);
		if (contextVar != null) {
			contextVar.visit(visitor);
		}
		for (TupleExpr arg : args) {
			arg.visit(visitor);
		}
	}

	@Override
	public void replaceChildNode(final QueryModelNode current, final QueryModelNode replacement) {
		if (current == commonVar) {
			setCommonVar((Var) replacement);
		} else if (current == contextVar) {
			setContextVar((Var) replacement);
		} else {
			for (int i=0; i<args.length; i++) {
				if (current == args[i]) {
					setStatementPattern(i, (TupleExpr) replacement);
					return;
				}
			}
			super.replaceChildNode(current, replacement);
		}
	}

	@Override
	public Set<String> getBindingNames() {
		Set<String> bindingNames = new LinkedHashSet<>(16);
		for (TupleExpr arg : args) {
			bindingNames.addAll(arg.getBindingNames());
		}
		return bindingNames;
	}

	@Override
	public Set<String> getAssuredBindingNames() {
		Set<String> bindingNames = new LinkedHashSet<>(16);
		for (TupleExpr arg : args) {
			bindingNames.addAll(arg.getAssuredBindingNames());
		}
		return bindingNames;
	}

	@Override
	public StarJoin clone() {
		StarJoin clone = (StarJoin) super.clone();

		clone.setCommonVar(commonVar.clone());
		if (contextVar != null) {
			clone.setContextVar(contextVar.clone());
		}

		for (int i=0; i<args.length; i++) {
			TupleExpr exprClone = args[i].clone();
			clone.setStatementPattern(i, exprClone);
		}
		return clone;
	}


	public static final class TopJoin extends Join {
		private static final long serialVersionUID = 8151800716831792753L;

		public TopJoin(TupleExpr parent, TupleExpr left, TupleExpr right) {
			super(left, right);
			setParentNode(parent);
		}

		@Override
		public void setResultSizeActual(long resultSizeActual) {
			super.setResultSizeActual(resultSizeActual);
			getParentNode().setResultSizeActual(resultSizeActual);
		}
	}
}
