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

import java.util.List;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.BeforeClass;
import static org.junit.Assert.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardTableUtilsTest {

    private static HTable table;

    @BeforeClass
    public static void setup() throws Exception {
        table = HalyardTableUtils.getTable(HBaseServerTestInstance.getInstanceConfig(), "testUtils", true, -1);
    }

    @AfterClass
    public static void teardown() throws Exception {
        table.close();
    }

    @Test
    public void testGetTheSameTableAgain() throws Exception {
        table.close();
        table = HalyardTableUtils.getTable(HBaseServerTestInstance.getInstanceConfig(), "testUtils", true, 1);
    }

    @Test
    public void testBigLiteral() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Resource subj = vf.createIRI("http://testBigLiteral/subject/");
        IRI pred = vf.createIRI("http://testBigLiteral/pred/");
        Value obj = vf.createLiteral(RandomStringUtils.random(100000));
        for (KeyValue kv : HalyardTableUtils.toKeyValues(subj, pred, obj, null, false, System.currentTimeMillis())) {
                table.put(new Put(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), kv.getTimestamp()).add(kv));
        }
        table.flushCommits();

        try (ResultScanner rs = table.getScanner(HalyardTableUtils.scan(subj, pred, obj, null))) {
            assertEquals(obj, HalyardTableUtils.parseStatements(rs.next()).iterator().next().getObject());
        }
        try (ResultScanner rs = table.getScanner(HalyardTableUtils.scan(subj, pred, null, null))) {
            assertEquals(obj, HalyardTableUtils.parseStatements(rs.next()).iterator().next().getObject());
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
        KeyValue kv1[] = HalyardTableUtils.toKeyValues(subj, pred1, obj1, null, false, timestamp);
        KeyValue kv2[] = HalyardTableUtils.toKeyValues(subj, pred2, obj2, null, false, timestamp);
        for (int i=0; i<3; i++) {
            table.put(new Put(kv1[i].getRowArray(), kv1[i].getRowOffset(), kv1[i].getRowLength(), kv1[i].getTimestamp()).add(kv1[i]));
            KeyValue conflicting = new KeyValue(kv1[i].getRowArray(), kv1[i].getRowOffset(), kv1[i].getRowLength(),
                    kv1[i].getFamilyArray(), kv1[i].getFamilyOffset(), kv1[i].getFamilyLength(),
                    kv2[i].getQualifierArray(), kv1[i].getQualifierOffset(), kv1[i].getQualifierLength(),
                    kv1[i].getTimestamp(), KeyValue.Type.Put, kv2[i].getValueArray(), kv1[i].getValueOffset(), kv1[i].getValueLength());
            table.put(new Put(conflicting.getRowArray(), conflicting.getRowOffset(), conflicting.getRowLength(), conflicting.getTimestamp()).add(conflicting));
        }
        table.flushCommits();

        try (ResultScanner rs = table.getScanner(HalyardTableUtils.scan(subj, pred1, obj1, null))) {
            List<Statement> res = HalyardTableUtils.parseStatements(rs.next());
            assertEquals(2, res.size());
            assertTrue(res.contains(SimpleValueFactory.getInstance().createStatement(subj, pred1, obj1)));
            assertTrue(res.contains(SimpleValueFactory.getInstance().createStatement(subj, pred2, obj2)));
        }
    }

    @Test
    public void testTruncateTable() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Resource subj = vf.createIRI("http://whatever/subj/");
        IRI pred = vf.createIRI("http://whatever/pred/");
        Value expl = vf.createLiteral("explicit");
        for (KeyValue kv : HalyardTableUtils.toKeyValues(subj, pred, expl, null, false, System.currentTimeMillis())) {
                table.put(new Put(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), kv.getTimestamp()).add(kv));
        }
        table.flushCommits();
        try (ResultScanner rs = table.getScanner(HalyardTableUtils.scan(subj, pred, expl, null))) {
            assertNotNull(rs.next());
        }
        table = HalyardTableUtils.truncateTable(table);
        try (ResultScanner rs = table.getScanner(HalyardTableUtils.scan(subj, pred, expl, null))) {
            assertNull(rs.next());
        }
    }

    @Test
    public void testNewInstance() {
        new HalyardTableUtils();
    }

    @Test(expected = RuntimeException.class)
    public void testGetInvalidMessageDigest() {
        HalyardTableUtils.getMessageDigest("invalid");
    }

    @Test
    public void testNoResult() {
        assertEquals(0, HalyardTableUtils.parseStatements(Result.EMPTY_RESULT).size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeSplitBits() {
        HalyardTableUtils.calculateSplits(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooBigSplitBits() {
        HalyardTableUtils.calculateSplits(17);
    }

    @Test
    public void testToKeyValuesDelete() throws Exception {
        IRI res = SimpleValueFactory.getInstance().createIRI("http://testiri");
        KeyValue kvs[] = HalyardTableUtils.toKeyValues(res, res, res, res, true, 0);
        assertEquals(6, kvs.length);
        for (KeyValue kv : kvs) {
            assertEquals(KeyValue.Type.DeleteColumn, KeyValue.Type.codeToType(kv.getTypeByte()));
        }
    }

    @Test
    public void testEncode() {
        assertEquals("AQIDBAU", HalyardTableUtils.encode(new byte[]{1, 2, 3, 4, 5}));
    }
}
