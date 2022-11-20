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

import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.algebra.And;
import org.eclipse.rdf4j.query.algebra.Compare;
import org.eclipse.rdf4j.query.algebra.Compare.CompareOp;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.ProjectionElem;
import org.eclipse.rdf4j.query.algebra.ProjectionElemList;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardFilterOptimizerTest {
	public QueryOptimizer getOptimizer() {
		return new HalyardFilterOptimizer();
	}

	@Test
	public void merge() {
		String expectedQuery = "SELECT * WHERE {?s ?p ?o . FILTER( ?o <4 && ?o > 2) }";
		String query = "SELECT * WHERE {?s ?p ?o . FILTER(?o > 2) . FILTER(?o <4) }";

		testOptimizer(expectedQuery, query);
	}

	@Test
	public void dontMerge() {
		Var s = new Var("s");
		Var p = new Var("p");
		Var o = new Var("o");
		Var o2 = new Var("o2");
		ValueConstant two = new ValueConstant(SimpleValueFactory.getInstance().createLiteral(2));
		ValueConstant four = new ValueConstant(SimpleValueFactory.getInstance().createLiteral(4));
		Compare oSmallerThanTwo = new Compare(o.clone(), two, CompareOp.GT);
		Filter spo = new Filter(new StatementPattern(s.clone(), p.clone(), o.clone()), oSmallerThanTwo);
		Compare o2SmallerThanFour = new Compare(o2.clone(), four, CompareOp.LT);
		Filter spo2 = new Filter(new StatementPattern(s.clone(), p.clone(), o2.clone()), o2SmallerThanFour);
		TupleExpr expected = new QueryRoot(
				new Projection(new Join(spo, spo2), new ProjectionElemList(new ProjectionElem("s"),
						new ProjectionElem("p"), new ProjectionElem("o"), new ProjectionElem("o2"))));
		String query = "SELECT * WHERE {?s ?p ?o . ?s ?p ?o2  . FILTER(?o > '2'^^xsd:int)  . FILTER(?o2 < '4'^^xsd:int) }";

		testOptimizer(expected, query);
	}

	@Test
	public void deMerge() {
		Var s = new Var("s");
		Var p = new Var("p");
		Var o = new Var("o");
		Var o2 = new Var("o2");
		ValueConstant one = new ValueConstant(SimpleValueFactory.getInstance().createLiteral(1));
		ValueConstant two = new ValueConstant(SimpleValueFactory.getInstance().createLiteral(2));
		ValueConstant four = new ValueConstant(SimpleValueFactory.getInstance().createLiteral(4));
		ValueConstant five = new ValueConstant(SimpleValueFactory.getInstance().createLiteral(5));

		And firstAnd = new And(new Compare(o.clone(), five, CompareOp.LT), new Compare(o.clone(), two, CompareOp.GT));
		Filter spo = new Filter(new StatementPattern(s.clone(), p.clone(), o.clone()), firstAnd);

		And secondAnd = new And(
				new And(new Compare(o2.clone(), two, CompareOp.NE), new Compare(o2.clone(), one, CompareOp.GT)),
				new Compare(o2.clone(), four, CompareOp.LT));
		Filter spo2 = new Filter(new StatementPattern(s.clone(), p.clone(), o2.clone()), secondAnd);

		TupleExpr expected = new QueryRoot(
				new Projection(new Join(spo, spo2), new ProjectionElemList(new ProjectionElem("s"),
						new ProjectionElem("p"), new ProjectionElem("o"), new ProjectionElem("o2"))));

		String query = "SELECT * WHERE {?s ?p ?o . ?s ?p ?o2  . FILTER(?o2 != '2'^^xsd:int && ?o2 > '1'^^xsd:int && ?o < '5'^^xsd:int && ?o > '2'^^xsd:int && ?o2 < '4'^^xsd:int) }";

		testOptimizer(expected, query);
	}

	void testOptimizer(String expectedQuery, String actualQuery) {
		ParsedQuery pq = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, actualQuery, null);
		QueryOptimizer opt = getOptimizer();
		opt.optimize(pq.getTupleExpr(), null, null);

		ParsedQuery expectedParsedQuery = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, expectedQuery, null);
		assertEquals(expectedParsedQuery.getTupleExpr(), pq.getTupleExpr());
	}

	void testOptimizer(TupleExpr expectedQuery, String actualQuery) {
		ParsedQuery pq = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, actualQuery, null);
		QueryOptimizer opt = getOptimizer();
		opt.optimize(pq.getTupleExpr(), null, null);

		assertEquals(expectedQuery, pq.getTupleExpr());
	}

	@Test
    public void testPropagateFilterMoreAgresivelly() {
		TupleExpr expr = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, "select * where {?a ?b ?c, ?d. filter (?d = ?nonexistent)}", "http://baseuri/").getTupleExpr();
		getOptimizer().optimize(expr, null, null);
        expr.visit(new AbstractExtendedQueryModelVisitor<RuntimeException>(){
            @Override
            public void meet(Join node) throws RuntimeException {
                assertEquals(expr.toString(), "Filter", node.getRightArg().getSignature());
                super.meet(node);
            }
            @Override
            public void meet(Filter node) throws RuntimeException {
                assertEquals(expr.toString(), "StatementPattern", node.getArg().getSignature());
            }
        });
    }

    @Test
    public void testKeepBindWithFilter() {
    	TupleExpr expr = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, "SELECT ?x\nWHERE {BIND (\"x\" AS ?x) FILTER (?x = \"x\")}", "http://baseuri/").getTupleExpr();
        TupleExpr clone = expr.clone();
        getOptimizer().optimize(clone, null, null);
        assertEquals(expr, clone);
    }

    @Test
    public void testPushFilterIntoStarJoins() {
    	TupleExpr expr = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, "select * {?s <:p1> ?o1; <:p2> ?o2; <:p3> ?o3 filter(?o1 = \"x\")}", null).getTupleExpr();
        new StarJoinOptimizer().optimize(expr, null, null);
        getOptimizer().optimize(expr, null, null);
        expr.visit(new AbstractExtendedQueryModelVisitor<RuntimeException>(){
            @Override
            public void meet(Filter node) throws RuntimeException {
                assertEquals(expr.toString(), "StarJoin", node.getParentNode().getSignature());
            }
        });
    }
}
