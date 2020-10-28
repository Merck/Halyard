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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardTableUtilsTest {

	private static Connection conn;
	private static Table table;

    @BeforeClass
    public static void setup() throws Exception {
		conn = HalyardTableUtils.getConnection(HBaseServerTestInstance.getInstanceConfig());
		table = HalyardTableUtils.getTable(conn, "testUtils", true, -1);
    }

    @AfterClass
    public static void teardown() throws Exception {
        table.close();
		conn.close();
    }

    @Test
    public void testGetTheSameTableAgain() throws Exception {
        table.close();
        table = HalyardTableUtils.getTable(HBaseServerTestInstance.getInstanceConfig(), "testUtils", true, 1);
    }

    @Test
    public void testIdIsUnique() {
        ValueFactory vf = SimpleValueFactory.getInstance();
        assertNotEquals(
        	Hashes.id(vf.createLiteral("1", vf.createIRI("local:type1"))),
        	Hashes.id(vf.createLiteral("1", vf.createIRI("local:type2"))));
    }

    @Test
    public void testBigLiteral() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Resource subj = vf.createIRI("http://testBigLiteral/subject/");
        IRI pred = vf.createIRI("http://testBigLiteral/pred/");
        Value obj = vf.createLiteral(RandomStringUtils.random(100000));
		List<Put> puts = new ArrayList<>();
        for (Cell kv : HalyardTableUtils.toKeyValues(subj, pred, obj, null, false, System.currentTimeMillis())) {
			puts.add(new Put(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), kv.getTimestamp()).add(kv));
        }
		table.put(puts);

        RDFSubject s = RDFSubject.create(subj);
        RDFPredicate p = RDFPredicate.create(pred);
        RDFObject o = RDFObject.create(obj);
        try (ResultScanner rs = table.getScanner(HalyardTableUtils.scan(s, p, o, null))) {
            assertEquals(obj, HalyardTableUtils.parseStatements(s, p, o, null, rs.next(), vf, null).iterator().next().getObject());
        }
        try (ResultScanner rs = table.getScanner(HalyardTableUtils.scan(s, p, null, null))) {
            assertEquals(obj, HalyardTableUtils.parseStatements(s, p, null, null, rs.next(), vf, null).iterator().next().getObject());
        }
    }

    @Test
    public void testConflictingHash() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Resource subj = vf.createIRI("http://testConflictingHash/subject/");
        IRI pred1 = vf.createIRI("http://testConflictingHash/pred1/");
        IRI pred2 = vf.createIRI("http://testConflictingHash/pred2/");
        Value obj1 = vf.createLiteral("literal1");
        Value obj2 = vf.createLiteral("literal2");
        long timestamp = System.currentTimeMillis();
        List<? extends Cell> kv1 = HalyardTableUtils.toKeyValues(subj, pred1, obj1, null, false, timestamp);
        List<? extends Cell> kv2 = HalyardTableUtils.toKeyValues(subj, pred2, obj2, null, false, timestamp);
		List<Put> puts = new ArrayList<>();
        for (int i=0; i<3; i++) {
        	Cell cell1 = kv1.get(i);
        	Cell cell2 = kv2.get(i);
			puts.add(new Put(cell1.getRowArray(), cell1.getRowOffset(), cell1.getRowLength(), cell1.getTimestamp())
					.add(cell1));
            Cell conflicting = new KeyValue(cell1.getRowArray(), cell1.getRowOffset(), cell1.getRowLength(),
                    cell1.getFamilyArray(), cell1.getFamilyOffset(), cell1.getFamilyLength(),
                    cell2.getQualifierArray(), cell2.getQualifierOffset(), cell2.getQualifierLength(),
                    cell1.getTimestamp(), KeyValue.Type.Put, cell2.getValueArray(), cell2.getValueOffset(), cell2.getValueLength());
			puts.add(new Put(conflicting.getRowArray(), conflicting.getRowOffset(), conflicting.getRowLength(),
					conflicting.getTimestamp()).add(conflicting));
        }
		table.put(puts);

        RDFSubject s = RDFSubject.create(subj);
        RDFPredicate p1 = RDFPredicate.create(pred1);
        RDFObject o1 = RDFObject.create(obj1);
        try (ResultScanner rs = table.getScanner(HalyardTableUtils.scan(s, p1, o1, null))) {
            List<Statement> res = HalyardTableUtils.parseStatements(s, p1, o1, null, rs.next(), vf, null);
            assertEquals(1, res.size());
            assertTrue(res.contains(SimpleValueFactory.getInstance().createStatement(subj, pred1, obj1)));
        }
    }

    @Test
    public void testTruncateTable() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Resource subj = vf.createIRI("http://whatever/subj/");
        IRI pred = vf.createIRI("http://whatever/pred/");
        Value expl = vf.createLiteral("explicit");
		List<Put> puts = new ArrayList<>();
        for (Cell kv : HalyardTableUtils.toKeyValues(subj, pred, expl, null, false, System.currentTimeMillis())) {
			puts.add(new Put(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), kv.getTimestamp()).add(kv));
        }
		table.put(puts);
        RDFSubject s = RDFSubject.create(subj);
        RDFPredicate p = RDFPredicate.create(pred);
        RDFObject o = RDFObject.create(expl);
        try (ResultScanner rs = table.getScanner(HalyardTableUtils.scan(s, p, o, null))) {
            assertNotNull(rs.next());
        }
		HalyardTableUtils.truncateTable(conn, table);
        try (ResultScanner rs = table.getScanner(HalyardTableUtils.scan(s, p, o, null))) {
            assertNull(rs.next());
        }
    }

    @Test
    public void testNewInstance() {
        new HalyardTableUtils();
    }

    @Test(expected = RuntimeException.class)
    public void testGetInvalidMessageDigest() {
        Hashes.getMessageDigest("invalid");
    }

    @Test
    public void testNoResult() throws Exception {
        assertEquals(0, HalyardTableUtils.parseStatements(null, null, null, null, Result.EMPTY_RESULT, SimpleValueFactory.getInstance(), null).size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeSplitBits() {
		HalyardTableUtils.calculateSplits(-1, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooBigSplitBits() {
		HalyardTableUtils.calculateSplits(17, true);
    }

    @Test
    public void testToKeyValuesDelete() throws Exception {
        IRI res = SimpleValueFactory.getInstance().createIRI("http://testiri");
        List<? extends Cell> kvs = HalyardTableUtils.toKeyValues(res, res, res, res, true, 0);
        assertEquals(6, kvs.size());
        for (Cell kv : kvs) {
            assertEquals(Cell.Type.DeleteColumn, kv.getType());
        }
    }

    @Test
    public void testEncode() {
        assertEquals("AQIDBAU", Hashes.encode(new byte[]{1, 2, 3, 4, 5}));
    }
}
