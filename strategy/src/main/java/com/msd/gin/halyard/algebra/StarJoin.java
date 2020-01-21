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

public class StarJoin extends AbstractQueryModelNode implements TupleExpr {
	private static final long serialVersionUID = -4523270958311045771L;

	public static TupleExpr join(List<? extends TupleExpr> exprs) {
		int i = exprs.size()-1;
		TupleExpr te = exprs.get(i);
		for (i--; i>=0; i--) {
			te = new Join(exprs.get(i), te);
		}
		return te;
	}

	private Var commonVar;
	private Var contextVar;
	private List<StatementPattern> args;

	public StarJoin(Var commonVar, Var contextVar, List<StatementPattern> sps) {
		assert sps.size() > 1;
		this.commonVar = commonVar;
		this.contextVar = contextVar;
		this.args = sps;
	}

	public Var getCommonVar() {
		return commonVar;
	}

	public Var getContextVar() {
		return contextVar;
	}

	public List<StatementPattern> getArgs() {
		return Collections.unmodifiableList(args);
	}

	public void setArgs(List<StatementPattern> sps) {
		this.args = sps;
	}

	public List<Var> getVarList() {
		return getVars(new ArrayList<>(4*args.size()));
	}

	public <L extends Collection<Var>> L getVars(L varCollection) {
		for(StatementPattern sp : args) {
			sp.getVars(varCollection);
		}
		return varCollection;
	}

	public Join toJoins() {
		return (Join) join(args);
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
		for (StatementPattern arg : args) {
			arg.visit(visitor);
		}
	}

	@Override
	public void replaceChildNode(final QueryModelNode current, final QueryModelNode replacement) {
		if (current == commonVar) {
			commonVar = (Var) replacement;
		} else if (current == contextVar) {
			contextVar = (Var) replacement;
		} else {
			final int index = args.indexOf(current);
			if (index >= 0) {
				args.set(index, (StatementPattern) replacement);
			} else {
				super.replaceChildNode(current, replacement);
			}
		}
	}

	@Override
	public Set<String> getBindingNames() {
		Set<String> bindingNames = new LinkedHashSet<>(16);
		for (StatementPattern arg : args) {
			bindingNames.addAll(arg.getBindingNames());
		}
		return bindingNames;
	}

	@Override
	public Set<String> getAssuredBindingNames() {
		Set<String> bindingNames = new LinkedHashSet<>(16);
		for (StatementPattern arg : args) {
			bindingNames.addAll(arg.getAssuredBindingNames());
		}
		return bindingNames;
	}

	@Override
	public StarJoin clone() {
		return (StarJoin) super.clone();
	}
}
