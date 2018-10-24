/*
 * Copyright © 2014 Merck Sharp & Dohme Corp., a subsidiary of Merck & Co., Inc.
 * All rights reserved.
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
