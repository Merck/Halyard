package com.msd.gin.halyard.common;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class ScanLiteralsTest {
    private static final String CTX = "http://whatever/ctx";

	private static Table table;
    private static Set<Literal> allLiterals;

    @BeforeClass
    public static void setup() throws Exception {
        table = HalyardTableUtils.getTable(HBaseServerTestInstance.getInstanceConfig(), "testScanLiterals", true, 0);

        SimpleValueFactory vf = SimpleValueFactory.getInstance();
        allLiterals = new HashSet<>();
        for (int i=0; i<5; i++) {
			allLiterals.add(vf.createLiteral(Math.random()));
			allLiterals.add(vf.createLiteral((long) Math.random()));
			allLiterals.add(vf.createLiteral(String.valueOf(Math.random())));
        }
        allLiterals.add(vf.createLiteral(new Date()));
        long timestamp = System.currentTimeMillis();
		List<Put> puts = new ArrayList<>();
        for (Literal l : allLiterals) {
            Resource subj = vf.createBNode();
            for (Cell kv : HalyardTableUtils.toKeyValues(subj, RDF.VALUE, l, vf.createIRI(CTX), false, timestamp)) {
				puts.add(new Put(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), kv.getTimestamp()).add(kv));
            }
            // add some non-literal objects
            for (Cell kv : HalyardTableUtils.toKeyValues(subj, OWL.SAMEAS, vf.createBNode(), vf.createIRI(CTX), false, timestamp)) {
				puts.add(new Put(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), kv.getTimestamp()).add(kv));
            }
        }
		table.put(puts);
    }

    @AfterClass
    public static void teardown() throws Exception {
        table.close();
    }

    @Test
    public void testScan() throws Exception {
        Set<Literal> actual = new HashSet<>();
        Scan scan = StatementIndex.scanLiterals();
        try (ResultScanner rs = table.getScanner(scan)) {
            Result r;
            while ((r = rs.next()) != null) {
                for (Statement stmt : HalyardTableUtils.parseStatements(null, null, null, null, r, ValueIO.SIMPLE_READER)) {
                    actual.add((Literal) stmt.getObject());
                }
            }
        }
        assertEquals(allLiterals, actual);
    }

    @Test
    public void testScanContext() throws Exception {
        SimpleValueFactory vf = SimpleValueFactory.getInstance();
        Set<Literal> actual = new HashSet<>();
        Scan scan = StatementIndex.scanLiterals(RDFContext.create(vf.createIRI(CTX)));
        try (ResultScanner rs = table.getScanner(scan)) {
            Result r;
            while ((r = rs.next()) != null) {
                for (Statement stmt : HalyardTableUtils.parseStatements(null, null, null, null, r, ValueIO.SIMPLE_READER)) {
                    actual.add((Literal) stmt.getObject());
                }
            }
        }
        assertEquals(allLiterals, actual);
    }
}
