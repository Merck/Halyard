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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.rdf4j.query.algebra.AbstractQueryModelNode;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.collectors.StatementPatternCollector;

public class StarJoin extends AbstractQueryModelNode implements TupleExpr {
	private static final long serialVersionUID = -4523270958311045771L;

	private Var commonVar;
	private Var contextVar;
	private List<TupleExpr> args;

	public StarJoin(Var commonVar, Var contextVar, List<StatementPattern> exprs) {
		assert exprs.size() > 1;
		setCommonVar(commonVar);
		setContextVar(contextVar);
		setStatementPatterns(exprs);
	}

	public void setCommonVar(Var var) {
		commonVar = var;
		commonVar.setParentNode(this);
	}

	public Var getCommonVar() {
		return commonVar;
	}

	public void setContextVar(Var var) {
		contextVar = var;
		if (contextVar != null) {
			contextVar.setParentNode(this);
		}
	}

	public Var getContextVar() {
		return contextVar;
	}

	public List<? extends TupleExpr> getArgs() {
		return Collections.unmodifiableList(args);
	}

	public void setStatementPatterns(List<StatementPattern> exprs) {
		this.args = new ArrayList<>(exprs);
		for (StatementPattern sp : exprs) {
			sp.setParentNode(this);
		}
	}

	public List<Var> getVarList() {
		return getVars(new ArrayList<>(4*args.size()));
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
		return (Join) Algebra.join(args);
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
			final int index = args.indexOf(current);
			if (index >= 0) {
				args.set(index, (TupleExpr) replacement);
				replacement.setParentNode(this);
			} else {
				super.replaceChildNode(current, replacement);
			}
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

		for (int i=0; i<args.size(); i++) {
			TupleExpr exprClone = args.get(i).clone();
			clone.args.set(i, exprClone);
			exprClone.setParentNode(clone);
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
