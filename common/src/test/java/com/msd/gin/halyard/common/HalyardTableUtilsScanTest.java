/*
 * Copyright 2016 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
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
package com.msd.gin.halyard.common;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import static org.junit.Assert.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
@RunWith(Parameterized.class)
public class HalyardTableUtilsScanTest {

    private static final String SUBJ1 = "http://whatever/subj1";
    private static final String SUBJ2 = "http://whatever/subj2";
    private static final String PRED1 = "http://whatever/pred1";
    private static final String PRED2 = "http://whatever/pred2";
    private static final String EXPL1 = "whatever explicit value1";
    private static final String EXPL2 = "whatever explicit value2";
    private static final String CTX1 = "http://whatever/ctx1";
    private static final String CTX2 = "http://whatever/ctx2";

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {

                 {null,   null,  null, null, 2},
                 {SUBJ1,  null,  null, null, 1},
                 {SUBJ2,  null,  null, null, 1},
                 {null,  PRED1,  null, null, 1},
                 {null,  PRED2,  null, null, 1},
                 {null,   null, EXPL1, null, 1},
                 {null,   null, EXPL2, null, 1},
                 {SUBJ1, PRED1,  null, null, 1},
                 {SUBJ1, PRED2,  null, null, 0},
                 {SUBJ2, PRED1,  null, null, 0},
                 {SUBJ2, PRED2,  null, null, 1},
                 {SUBJ1,  null, EXPL1, null, 1},
                 {SUBJ2,  null, EXPL1, null, 0},
                 {SUBJ1,  null, EXPL2, null, 0},
                 {SUBJ2,  null, EXPL2, null, 1},
                 {null,  PRED1, EXPL1, null, 1},
                 {null,  PRED2, EXPL1, null, 0},
                 {null,  PRED1, EXPL2, null, 0},
                 {null,  PRED2, EXPL2, null, 1},
                 {SUBJ1, PRED1, EXPL1, null, 1},
                 {SUBJ2, PRED2, EXPL2, null, 1},
                 {SUBJ1, PRED2, EXPL1, null, 0},
                 {SUBJ2, PRED1, EXPL2, null, 0},
                 {SUBJ1, PRED2, EXPL2, null, 0},
                 {SUBJ2, PRED1, EXPL1, null, 0},
                 {SUBJ1, PRED1, EXPL2, null, 0},
                 {SUBJ2, PRED2, EXPL1, null, 0},
                 {null,   null,  null, CTX1, 1},
                 {SUBJ1,  null,  null, CTX1, 1},
                 {SUBJ2,  null,  null, CTX1, 0},
                 {null,  PRED1,  null, CTX1, 1},
                 {null,  PRED2,  null, CTX1, 0},
                 {null,   null, EXPL1, CTX1, 1},
                 {null,   null, EXPL2, CTX1, 0},
                 {SUBJ1, PRED1,  null, CTX1, 1},
                 {SUBJ1, PRED2,  null, CTX1, 0},
                 {SUBJ2, PRED1,  null, CTX1, 0},
                 {SUBJ2, PRED2,  null, CTX1, 0},
                 {SUBJ1,  null, EXPL1, CTX1, 1},
                 {SUBJ2,  null, EXPL1, CTX1, 0},
                 {SUBJ1,  null, EXPL2, CTX1, 0},
                 {SUBJ2,  null, EXPL2, CTX1, 0},
                 {null,  PRED1, EXPL1, CTX1, 1},
                 {null,  PRED2, EXPL1, CTX1, 0},
                 {null,  PRED1, EXPL2, CTX1, 0},
                 {null,  PRED2, EXPL2, CTX1, 0},
                 {SUBJ1, PRED1, EXPL1, CTX1, 1},
                 {SUBJ2, PRED2, EXPL2, CTX1, 0},
                 {SUBJ1, PRED2, EXPL1, CTX1, 0},
                 {SUBJ2, PRED1, EXPL2, CTX1, 0},
                 {SUBJ1, PRED2, EXPL2, CTX1, 0},
                 {SUBJ2, PRED1, EXPL1, CTX1, 0},
                 {SUBJ1, PRED1, EXPL2, CTX1, 0},
                 {SUBJ2, PRED2, EXPL1, CTX1, 0},
                 {null,   null,  null, CTX2, 1},
                 {SUBJ1,  null,  null, CTX2, 0},
                 {SUBJ2,  null,  null, CTX2, 1},
                 {null,  PRED1,  null, CTX2, 0},
                 {null,  PRED2,  null, CTX2, 1},
                 {null,   null, EXPL1, CTX2, 0},
                 {null,   null, EXPL2, CTX2, 1},
                 {SUBJ1, PRED1,  null, CTX2, 0},
                 {SUBJ1, PRED2,  null, CTX2, 0},
                 {SUBJ2, PRED1,  null, CTX2, 0},
                 {SUBJ2, PRED2,  null, CTX2, 1},
                 {SUBJ1,  null, EXPL1, CTX2, 0},
                 {SUBJ2,  null, EXPL1, CTX2, 0},
                 {SUBJ1,  null, EXPL2, CTX2, 0},
                 {SUBJ2,  null, EXPL2, CTX2, 1},
                 {null,  PRED1, EXPL1, CTX2, 0},
                 {null,  PRED2, EXPL1, CTX2, 0},
                 {null,  PRED1, EXPL2, CTX2, 0},
                 {null,  PRED2, EXPL2, CTX2, 1},
                 {SUBJ1, PRED1, EXPL1, CTX2, 0},
                 {SUBJ2, PRED2, EXPL2, CTX2, 1},
                 {SUBJ1, PRED2, EXPL1, CTX2, 0},
                 {SUBJ2, PRED1, EXPL2, CTX2, 0},
                 {SUBJ1, PRED2, EXPL2, CTX2, 0},
                 {SUBJ2, PRED1, EXPL1, CTX2, 0},
                 {SUBJ1, PRED1, EXPL2, CTX2, 0},
                 {SUBJ2, PRED2, EXPL1, CTX2, 0},
           });
    }

    private static HTable table;
    private static Set<Statement> allStatements;

    @BeforeClass
    public static void setup() throws Exception {
        table = HalyardTableUtils.getTable(HBaseServerTestInstance.getInstanceConfig(), "testScan", true, 0);

        allStatements = new HashSet<>();
        SimpleValueFactory vf = SimpleValueFactory.getInstance();
        allStatements.add(vf.createStatement(vf.createIRI(SUBJ1), vf.createIRI(PRED1), vf.createLiteral(EXPL1), vf.createIRI(CTX1)));
        allStatements.add(vf.createStatement(vf.createIRI(SUBJ2), vf.createIRI(PRED2), vf.createLiteral(EXPL2), vf.createIRI(CTX2)));
        long timestamp = System.currentTimeMillis();
        for (Statement st : allStatements) {
            for (KeyValue kv : HalyardTableUtils.toKeyValues(st.getSubject(), st.getPredicate(), st.getObject(), st.getContext(), false, timestamp)) {
                    table.put(new Put(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), kv.getTimestamp()).add(kv));
            }
        }
        table.flushCommits();
    }

    @AfterClass
    public static void teardown() throws Exception {
        table.close();
    }

    private final String s, p, o, c;
    private final int expRes;

    public HalyardTableUtilsScanTest(String s, String p, String o, String c, int expRes) {
        this.s = s;
        this.p = p;
        this.o = o;
        this.c = c;
        this.expRes = expRes;
    }

    @Test
    public void testScan() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();

        try (ResultScanner rs = table.getScanner(HalyardTableUtils.scan(s == null ? null : vf.createIRI(s), p == null ? null : vf.createIRI(p), o == null ? null : vf.createLiteral(o), c == null ? null : vf.createIRI(c)))) {
            Set<Statement> res = new HashSet<>();
            Result r;
            while ((r = rs.next()) != null) {
                res.addAll(HalyardTableUtils.parseStatements(r));
            }
            assertTrue(allStatements.containsAll(res));
            assertEquals(expRes, res.size());
        }
    }

}
