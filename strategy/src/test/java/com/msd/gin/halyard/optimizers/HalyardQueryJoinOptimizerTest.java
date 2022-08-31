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
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardQueryJoinOptimizerTest {

	private HalyardEvaluationStatistics createStatistics() {
		return new HalyardEvaluationStatistics(() -> new SimpleStatementPatternCardinalityCalculator(), null);
	}

	@Test
    public void testQueryJoinOptimizer() {
        final TupleExpr expr = new SPARQLParser().parseQuery("select * where {?a ?b ?c, \"1\".}", "http://baseuri/").getTupleExpr();
        new HalyardQueryJoinOptimizer(createStatistics()).optimize(expr, null, null);
        expr.visit(new AbstractQueryModelVisitor<RuntimeException>(){
            @Override
            public void meet(Join node) throws RuntimeException {
                assertTrue(expr.toString(), ((StatementPattern)node.getLeftArg()).getObjectVar().hasValue());
                assertEquals(expr.toString(), "c", ((StatementPattern)node.getRightArg()).getObjectVar().getName());
            }
        });
    }

    @Test
    public void testQueryJoinOptimizerWithSplitFunction() {
        final TupleExpr expr = new SPARQLParser().parseQuery("select * where {?a a \"1\";?b ?d. filter (<" + HALYARD.PARALLEL_SPLIT_FUNCTION + ">(10, ?d))}", "http://baseuri/").getTupleExpr();
        new HalyardQueryJoinOptimizer(createStatistics()).optimize(expr, null, null);
        expr.visit(new AbstractQueryModelVisitor<RuntimeException>(){
            @Override
            public void meet(Join node) throws RuntimeException {
                assertEquals(expr.toString(), "d", ((StatementPattern)node.getLeftArg()).getObjectVar().getName());
                assertTrue(expr.toString(), ((StatementPattern)node.getRightArg()).getObjectVar().hasValue());
            }
        });
    }

    @Test
    public void testQueryJoinOptimizarWithBind() {
        final TupleExpr expr = new SPARQLParser().parseQuery("SELECT * WHERE { BIND (<http://whatever/> AS ?b)  ?a <http://whatever/> ?b , \"whatever\".}", "http://baseuri/").getTupleExpr();
        new HalyardQueryJoinOptimizer(createStatistics()).optimize(expr, null, null);
        expr.visit(new AbstractQueryModelVisitor<RuntimeException>(){
            @Override
            public void meet(Join node) throws RuntimeException {
                if (node.getLeftArg() instanceof StatementPattern) {
                    assertEquals(expr.toString(), "b", ((StatementPattern)node.getLeftArg()).getObjectVar().getName());
                }
            }
        });
    }
}
