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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
@RunWith(Parameterized.class)
public class HalyardTableUtilsScanTest {

	private static final String SUBJ1 = "http://whatever/subj1";
    private static final String SUBJ2 = "http://whatever/subj2";
    private static final String PRED1 = RDF.TYPE.stringValue();
    private static final String PRED2 = "http://whatever/pred";
    private static final String EXPL1 = "whatever explicit value1";
    private static final String EXPL2 = "whatever explicit value2";
    private static final String CTX1 = "http://whatever/ctx1";
    private static final String CTX2 = "http://whatever/ctx2";

    @Parameters(name = "{0}, {1}, {2}, {3}")
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

    private static RDFFactory rdfFactory;
	private static Table table;
    private static Set<Statement> allStatements;

    @BeforeClass
    public static void setup() throws Exception {
		Configuration conf = HBaseServerTestInstance.getInstanceConfig();
        table = HalyardTableUtils.getTable(conf, "testScan", true, 0);
		rdfFactory = RDFFactory.create(table);

        SimpleValueFactory vf = SimpleValueFactory.getInstance();
        allStatements = new HashSet<>();
        allStatements.add(vf.createStatement(vf.createIRI(SUBJ1), vf.createIRI(PRED1), vf.createLiteral(EXPL1), vf.createIRI(CTX1)));
        allStatements.add(vf.createStatement(vf.createIRI(SUBJ2), vf.createIRI(PRED2), vf.createLiteral(EXPL2), vf.createIRI(CTX2)));
        long timestamp = System.currentTimeMillis();
		List<Put> puts = new ArrayList<>();
        for (Statement st : allStatements) {
            for (Cell kv : HalyardTableUtils.toKeyValues(st.getSubject(), st.getPredicate(), st.getObject(), st.getContext(), false, timestamp, rdfFactory)) {
				puts.add(new Put(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), kv.getTimestamp()).add(kv));
            }
        }
		table.put(puts);
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
        ValueIO.Reader reader = rdfFactory.createReader(vf);

        RDFSubject subj = rdfFactory.createSubject(s == null ? null : vf.createIRI(s));
        RDFPredicate pred = rdfFactory.createPredicate(p == null ? null : vf.createIRI(p));
        RDFObject obj = rdfFactory.createObject(o == null ? null : vf.createLiteral(o));
        RDFContext ctx = rdfFactory.createContext(c == null ? null : vf.createIRI(c));
        try (ResultScanner rs = table.getScanner(HalyardTableUtils.scan(subj, pred, obj, ctx, rdfFactory))) {
            Set<Statement> res = new HashSet<>();
            Result r;
            while ((r = rs.next()) != null) {
                res.addAll(HalyardTableUtils.parseStatements(subj, pred, obj, ctx, r, reader, rdfFactory));
            }
            assertTrue(allStatements.containsAll(res));
            assertEquals(s+", "+p+", "+o+", "+c, expRes, res.size());
        }

        // check all complete combinations
    	if (subj != null && pred != null & obj != null) {
            StatementIndex<SPOC.S,SPOC.P,SPOC.O,SPOC.C> spo = rdfFactory.getSPOIndex();
            StatementIndex<SPOC.P,SPOC.O,SPOC.S,SPOC.C> pos = rdfFactory.getPOSIndex();
            StatementIndex<SPOC.O,SPOC.S,SPOC.P,SPOC.C> osp = rdfFactory.getOSPIndex();
            StatementIndex<SPOC.C,SPOC.S,SPOC.P,SPOC.O> cspo = rdfFactory.getCSPOIndex();
            StatementIndex<SPOC.C,SPOC.P,SPOC.O,SPOC.S> cpos = rdfFactory.getCPOSIndex();
            StatementIndex<SPOC.C,SPOC.O,SPOC.S,SPOC.P> cosp = rdfFactory.getCOSPIndex();
            List<Scan> scans = new ArrayList<>();
            if (c != null) {
                scans.add(spo.scan(subj, pred, obj, ctx));
                scans.add(pos.scan(pred, obj, subj, ctx));
                scans.add(osp.scan(obj, subj, pred, ctx));
                scans.add(cspo.scan(ctx, subj, pred, obj));
                scans.add(cpos.scan(ctx, pred, obj, subj));
                scans.add(cosp.scan(ctx, obj, subj, pred));
            } else {
                scans.add(spo.scan(subj, pred, obj));
                scans.add(pos.scan(pred, obj, subj));
                scans.add(osp.scan(obj, subj, pred));
            }
            for (Scan scan : scans) {
                try (ResultScanner rs = table.getScanner(scan)) {
                    Set<Statement> res = new HashSet<>();
                    Result r;
                    while ((r = rs.next()) != null) {
                        res.addAll(HalyardTableUtils.parseStatements(null, null, null, null, r, reader, rdfFactory));
                    }
                    assertTrue(allStatements.containsAll(res));
                    assertEquals(s+", "+p+", "+o+", "+c, expRes, res.size());
                }
            }
    	}
    }

    @Test
    public void testScanWithSubjectAndObjectConstraint() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        ValueIO.Reader reader = rdfFactory.createReader(vf);

        RDFSubject subj = rdfFactory.createSubject(s == null ? null : vf.createIRI(s));
        RDFPredicate pred = rdfFactory.createPredicate(p == null ? null : vf.createIRI(p));
        RDFObject obj = rdfFactory.createObject(o == null ? null : vf.createLiteral(o));
        RDFContext ctx = rdfFactory.createContext(c == null ? null : vf.createIRI(c));
        try (ResultScanner rs = table.getScanner(HalyardTableUtils.scanWithConstraints(subj, new ValueConstraint(ValueType.IRI), pred, obj, new ObjectConstraint(XSD.STRING), ctx, rdfFactory))) {
            Set<Statement> res = new HashSet<>();
            Result r;
            while ((r = rs.next()) != null) {
                res.addAll(HalyardTableUtils.parseStatements(subj, pred, null, ctx, r, reader, rdfFactory));
            }
            assertTrue(allStatements.containsAll(res));
            assertEquals(s+", "+p+", "+o+", "+c, expRes, res.size());
        }
    }

    @Test
    public void testScanWithSubjectConstraint() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        ValueIO.Reader reader = rdfFactory.createReader(vf);

        RDFSubject subj = rdfFactory.createSubject(s == null ? null : vf.createIRI(s));
        RDFPredicate pred = rdfFactory.createPredicate(p == null ? null : vf.createIRI(p));
        RDFObject obj = rdfFactory.createObject(o == null ? null : vf.createLiteral(o));
        RDFContext ctx = rdfFactory.createContext(c == null ? null : vf.createIRI(c));
        try (ResultScanner rs = table.getScanner(HalyardTableUtils.scanWithConstraints(subj, new ValueConstraint(ValueType.IRI), pred, obj, null, ctx, rdfFactory))) {
            Set<Statement> res = new HashSet<>();
            Result r;
            while ((r = rs.next()) != null) {
                res.addAll(HalyardTableUtils.parseStatements(subj, pred, null, ctx, r, reader, rdfFactory));
            }
            assertTrue(allStatements.containsAll(res));
            if (pred == null || obj != null) {
            	assertEquals(s+", "+p+", "+o+", "+c, expRes, res.size());
            } else {
            	// scan is going to be a superset
            	assertThat(s+", "+p+", "+o+", "+c, expRes, lessThanOrEqualTo(res.size()));
            }
        }
    }

    @Test
    public void testScanWithObjectConstraint() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        ValueIO.Reader reader = rdfFactory.createReader(vf);

        RDFSubject subj = rdfFactory.createSubject(s == null ? null : vf.createIRI(s));
        RDFPredicate pred = rdfFactory.createPredicate(p == null ? null : vf.createIRI(p));
        RDFObject obj = rdfFactory.createObject(o == null ? null : vf.createLiteral(o));
        RDFContext ctx = rdfFactory.createContext(c == null ? null : vf.createIRI(c));
        try (ResultScanner rs = table.getScanner(HalyardTableUtils.scanWithConstraints(subj, null, pred, obj, new ObjectConstraint(XSD.STRING), ctx, rdfFactory))) {
            Set<Statement> res = new HashSet<>();
            Result r;
            while ((r = rs.next()) != null) {
                res.addAll(HalyardTableUtils.parseStatements(subj, pred, null, ctx, r, reader, rdfFactory));
            }
            assertTrue(allStatements.containsAll(res));
            if (subj == null || pred != null) {
            	assertEquals(s+", "+p+", "+o+", "+c, expRes, res.size());
            } else {
            	// scan is going to be a superset
            	assertThat(s+", "+p+", "+o+", "+c, expRes, lessThanOrEqualTo(res.size()));
            }
        }
    }
}
