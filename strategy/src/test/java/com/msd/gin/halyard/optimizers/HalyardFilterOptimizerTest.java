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

import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardFilterOptimizerTest {

    @Test
    public void testPropagateFilterMoreAgresivelly() {
        final TupleExpr expr = new SPARQLParser().parseQuery("select * where {?a ?b ?c, ?d. filter (?d = ?nonexistent)}", "http://baseuri/").getTupleExpr();
        new HalyardFilterOptimizer().optimize(expr, null, null);
        expr.visit(new AbstractQueryModelVisitor<RuntimeException>(){
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
}
