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

import com.msd.gin.halyard.vocab.HALYARD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardQueryJoinOptimizerTest {
	private static final String BASE_URI = "http://baseuri/";

	private HalyardEvaluationStatistics createStatistics() {
		return new HalyardEvaluationStatistics(SimpleStatementPatternCardinalityCalculator.FACTORY, null);
	}

	@Test
    public void testQueryJoinOptimizerWithSimpleJoin() {
        final TupleExpr expr = new SPARQLParser().parseQuery("select * where {?a ?b ?c, \"1\".}", BASE_URI).getTupleExpr();
        new HalyardQueryJoinOptimizer(createStatistics()).optimize(expr, null, null);
        expr.visit(new AbstractQueryModelVisitor<RuntimeException>(){
            @Override
            public void meet(Join node) {
                assertTrue(expr.toString(), ((StatementPattern)node.getLeftArg()).getObjectVar().hasValue());
                assertEquals(expr.toString(), "c", ((StatementPattern)node.getRightArg()).getObjectVar().getName());
            }
        });
    }

    @Test
    public void testQueryJoinOptimizerWithSplitFunction() {
        final TupleExpr expr = new SPARQLParser().parseQuery("select * where {?a a \"1\";?b ?d. filter (<" + HALYARD.PARALLEL_SPLIT_FUNCTION + ">(10, ?d))}", BASE_URI).getTupleExpr();
        new HalyardQueryJoinOptimizer(createStatistics()).optimize(expr, null, null);
        expr.visit(new AbstractQueryModelVisitor<RuntimeException>(){
            @Override
            public void meet(Join node) {
                assertTrue(expr.toString(), ((StatementPattern)node.getLeftArg()).getObjectVar().hasValue());
                assertEquals(expr.toString(), "d", ((StatementPattern)node.getRightArg()).getObjectVar().getName());
            }
        });
    }

    @Test
    public void testQueryJoinOptimizerWithBind() {
        final TupleExpr expr = new SPARQLParser().parseQuery("SELECT * WHERE { BIND (<http://whatever/obj> AS ?b)  ?a <http://whatever/pred> ?b , \"whatever\".}", BASE_URI).getTupleExpr();
        new HalyardQueryJoinOptimizer(createStatistics()).optimize(expr, null, null);
        expr.visit(new AbstractQueryModelVisitor<RuntimeException>(){
            @Override
            public void meet(Join node) {
                if (node.getLeftArg() instanceof StatementPattern) {
                    assertEquals(expr.toString(), "b", ((StatementPattern)node.getLeftArg()).getObjectVar().getName());
                }
            }
        });
    }

    @Test
    public void testQueryJoinOptimizerWithStats1() {
    	ValueFactory vf = SimpleValueFactory.getInstance();
        final TupleExpr expr = new SPARQLParser().parseQuery("SELECT * WHERE { ?a <http://whatever/1>/<http://whatever/2>/<http://whatever/3> ?b }", BASE_URI).getTupleExpr();
        IRI pred1 = vf.createIRI("http://whatever/1");
        IRI pred2 = vf.createIRI("http://whatever/2");
        IRI pred3 = vf.createIRI("http://whatever/3");
        Map<IRI, Double> predicateStats = new HashMap<>();
        predicateStats.put(pred1, 100.0);
        predicateStats.put(pred2, 5.0);
        predicateStats.put(pred3, 25.0);
        new HalyardQueryJoinOptimizer(new HalyardEvaluationStatistics(() -> new MockStatementPatternCardinalityCalculator(predicateStats), null)).optimize(expr, null, null);
        List<IRI> joinOrder = new ArrayList<>();
        expr.visit(new AbstractQueryModelVisitor<RuntimeException>(){
            @Override
            public void meet(Join node) {
                if (node.getLeftArg() instanceof StatementPattern) {
                    joinOrder.add((IRI) ((StatementPattern)node.getLeftArg()).getPredicateVar().getValue());
                }
                if (node.getRightArg() instanceof StatementPattern) {
                    joinOrder.add((IRI) ((StatementPattern)node.getRightArg()).getPredicateVar().getValue());
                }
                super.meet(node);
            }
        });
        assertEquals(expr.toString(), Arrays.asList(pred2, pred3, pred1), joinOrder);
    }

    @Test
    public void testQueryJoinOptimizerWithStats2() {
    	ValueFactory vf = SimpleValueFactory.getInstance();
        final TupleExpr expr = new SPARQLParser().parseQuery("SELECT * WHERE { ?a <http://whatever/1>/<http://whatever/2> ?b. ?a <http://whatever/a>/<http://whatever/b> ?c }", BASE_URI).getTupleExpr();
        IRI pred1 = vf.createIRI("http://whatever/1");
        IRI pred2 = vf.createIRI("http://whatever/2");
        IRI preda = vf.createIRI("http://whatever/a");
        IRI predb = vf.createIRI("http://whatever/b");
        Map<IRI, Double> predicateStats = new HashMap<>();
        predicateStats.put(pred1, 100.0);
        predicateStats.put(pred2, 5.0);
        predicateStats.put(preda, 2.0);
        predicateStats.put(predb, 45.0);
        new HalyardQueryJoinOptimizer(new HalyardEvaluationStatistics(() -> new MockStatementPatternCardinalityCalculator(predicateStats), null)).optimize(expr, null, null);
        List<IRI> joinOrder = new ArrayList<>();
        expr.visit(new AbstractQueryModelVisitor<RuntimeException>(){
            @Override
            public void meet(Join node) {
                if (node.getLeftArg() instanceof StatementPattern) {
                    joinOrder.add((IRI) ((StatementPattern)node.getLeftArg()).getPredicateVar().getValue());
                }
                if (node.getRightArg() instanceof StatementPattern) {
                    joinOrder.add((IRI) ((StatementPattern)node.getRightArg()).getPredicateVar().getValue());
                }
                super.meet(node);
            }
        });
        assertEquals(expr.toString(), Arrays.asList(preda, pred2, pred1, predb), joinOrder);
    }

    @Test
    public void testQueryJoinOptimizerWithStarJoin() {
    	ValueFactory vf = SimpleValueFactory.getInstance();
        final TupleExpr expr = new SPARQLParser().parseQuery("SELECT * WHERE { ?a <http://whatever/1> ?b; <http://whatever/2> ?c }", BASE_URI).getTupleExpr();
        IRI pred1 = vf.createIRI("http://whatever/1");
        IRI pred2 = vf.createIRI("http://whatever/2");
        Map<IRI, Double> predicateStats = new HashMap<>();
        predicateStats.put(pred1, 100.0);
        predicateStats.put(pred2, 5.0);
        new StarJoinOptimizer().optimize(expr, null, null);
        new HalyardQueryJoinOptimizer(new HalyardEvaluationStatistics(() -> new MockStatementPatternCardinalityCalculator(predicateStats), null)).optimize(expr, null, null);
        List<IRI> joinOrder = new ArrayList<>();
        expr.visit(new AbstractQueryModelVisitor<RuntimeException>(){
            @Override
            public void meet(StatementPattern node) {
                joinOrder.add((IRI) node.getPredicateVar().getValue());
            }
        });
        assertEquals(expr.toString(), Arrays.asList(pred2, pred1), joinOrder);
    }


	public static class MockStatementPatternCardinalityCalculator extends SimpleStatementPatternCardinalityCalculator {
		final Map<IRI, Double> predicateStats;
	
		public MockStatementPatternCardinalityCalculator(Map<IRI, Double> predicateStats) {
			this.predicateStats = predicateStats;
		}
	
		@Override
		public double getCardinality(StatementPattern sp, Collection<String> boundVars) {
			IRI predicate = (IRI) sp.getPredicateVar().getValue();
			return predicateStats.getOrDefault(predicate, super.getCardinality(sp, boundVars));
		}
	}
}
