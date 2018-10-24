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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 * @author Adam Sotona (MSD)
 */
@RunWith(Parameterized.class)
public class HalyardEvaluationStatisticsTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {"select * where {?s a ?o}",            1.0E2, null},
            {"select * where {?s ?p ?o}",           1.0E3, null},
            {"select * where {?s a \"1\"}",         1.0E1, null},
            {"select * where {?a ?b ?c; ?d ?e}",    1.0E8, null},
            {"select * where {?a a ?c; ?d ?e}",     1.0E6, null},
            {"select * where {?a ?b ?c; a ?e}",     1.0E7, null},
            {"select * where {?s a ?o}",            1.0E1, new String[]{"o"}},
            {"select * where {?s ?p ?o}",           1.0E1, new String[]{"s", "o"}},
            {"select * where {?s a \"1\"}",         1.0,   new String[]{"s"}},
            {"select * where {?a ?b ?c; ?d ?e}",    1.0E4, new String[]{"b", "c"}},
            {"select * where {?a a ?c; ?d ?e}",     1.0E3, new String[]{"d", "c"}},
            {"select * where {?a ?b ?c; a ?e}",     1.0E2, new String[]{"b", "e", "c"}},
            {"select * where {{?a a \"1\". optional {?a a ?b}} union {?a a \"2\"}}", 1010.0, null},
        });
    }

    private final String query;
    private final double cardinality;
    private final Set<String> boundVars = new HashSet<>();

    public HalyardEvaluationStatisticsTest(String query, double cardinality, String[] boundVars) {
        this.query = query;
        this.cardinality = cardinality;
        if (boundVars != null) {
            this.boundVars.addAll(Arrays.asList(boundVars));
        }
    }

    @Test
    public void testCardinality() {
        Assert.assertEquals(query, cardinality, new HalyardEvaluationStatistics(null, null).getCardinality(new SPARQLParser().parseQuery(query, "http://baseuri/").getTupleExpr(), boundVars), cardinality/1000000.0);
    }
}
