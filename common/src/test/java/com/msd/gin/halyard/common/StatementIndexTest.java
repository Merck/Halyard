package com.msd.gin.halyard.common;

import java.util.ArrayList;
import java.util.Date;
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
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class StatementIndexTest {
    private static final String SUBJ = "http://whatever/subj";
    private static final String CTX = "http://whatever/ctx";

	private static Table table;
	private static RDFFactory rdfFactory;
    private static Set<Statement> allStatements;
    private static Set<Literal> allLiterals;
    private static Set<Literal> stringLiterals;

    @BeforeClass
    public static void setup() throws Exception {
		Configuration conf = HBaseServerTestInstance.getInstanceConfig();
        table = HalyardTableUtils.getTable(conf, "testStatementIndex", true, 0);
		rdfFactory = RDFFactory.create(table);

        SimpleValueFactory vf = SimpleValueFactory.getInstance();
        allStatements = new HashSet<>();
        allLiterals = new HashSet<>();
        stringLiterals = new HashSet<>();
        for (int i=0; i<5; i++) {
			allLiterals.add(vf.createLiteral(Math.random()));
			allLiterals.add(vf.createLiteral((long) Math.random()));
			Literal strl = vf.createLiteral(String.valueOf(Math.random()));
			stringLiterals.add(strl);
			allLiterals.add(strl);
        }
        allLiterals.add(vf.createLiteral(new Date()));
        long timestamp = System.currentTimeMillis();
		List<Put> puts = new ArrayList<>();
        Resource subj = vf.createIRI(SUBJ);
        for (Literal l : allLiterals) {
            Statement stmt = vf.createStatement(subj, RDF.VALUE, l, vf.createIRI(CTX));
            allStatements.add(stmt);
            for (Cell kv : HalyardTableUtils.toKeyValues(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), stmt.getContext(), false, timestamp, rdfFactory)) {
				puts.add(new Put(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), kv.getTimestamp()).add(kv));
            }

            // add some non-literal objects
            stmt = vf.createStatement(subj, OWL.SAMEAS, vf.createBNode(), vf.createIRI(CTX));
            allStatements.add(stmt);
            for (Cell kv : HalyardTableUtils.toKeyValues(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), stmt.getContext(), false, timestamp, rdfFactory)) {
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
    public void testScanAll() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        ValueIO.Reader reader = rdfFactory.createReader(vf);

        Set<Statement> actual = new HashSet<>();
        Scan scan = StatementIndex.scanAll(rdfFactory);
        try (ResultScanner rs = table.getScanner(scan)) {
            Result r;
            while ((r = rs.next()) != null) {
                for (Statement stmt : HalyardTableUtils.parseStatements(null, null, null, null, r, reader, rdfFactory)) {
                    actual.add(stmt);
                }
            }
        }
        assertEquals(allStatements, actual);
    }

    @Test
    public void testScanLiterals() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        ValueIO.Reader reader = rdfFactory.createReader(vf);

        Set<Literal> actual = new HashSet<>();
        Scan scan = StatementIndex.scanLiterals(rdfFactory);
        try (ResultScanner rs = table.getScanner(scan)) {
            Result r;
            while ((r = rs.next()) != null) {
                for (Statement stmt : HalyardTableUtils.parseStatements(null, null, null, null, r, reader, rdfFactory)) {
                    actual.add((Literal) stmt.getObject());
                }
            }
        }
        assertEquals(allLiterals, actual);
    }

    @Test
    public void testScanLiteralsContext() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        ValueIO.Reader reader = rdfFactory.createReader(vf);

        Set<Literal> actual = new HashSet<>();
        Scan scan = StatementIndex.scanLiterals(vf.createIRI(CTX), rdfFactory);
        try (ResultScanner rs = table.getScanner(scan)) {
            Result r;
            while ((r = rs.next()) != null) {
                for (Statement stmt : HalyardTableUtils.parseStatements(null, null, null, null, r, reader, rdfFactory)) {
                    actual.add((Literal) stmt.getObject());
                }
            }
        }
        assertEquals(allLiterals, actual);
    }

    @Test
    public void testScanStringLiterals_SPO() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        ValueIO.Reader reader = rdfFactory.createReader(vf);
        Resource subj = vf.createIRI(SUBJ);
        RDFSubject rdfSubj = rdfFactory.createSubject(subj);
        RDFPredicate rdfPred = rdfFactory.createPredicate(RDF.VALUE);

        Set<Literal> actual = new HashSet<>();
        Scan scan = rdfFactory.getSPOIndex().scanWithConstraints(rdfSubj, rdfPred, new LiteralConstraints(XSD.STRING));
        try (ResultScanner rs = table.getScanner(scan)) {
            Result r;
            while ((r = rs.next()) != null) {
                for (Statement stmt : HalyardTableUtils.parseStatements(rdfSubj, rdfPred, null, null, r, reader, rdfFactory)) {
                    actual.add((Literal) stmt.getObject());
                }
            }
        }
        assertEquals(stringLiterals, actual);
    }

    @Test
    public void testScanNonStringLiterals_SPO() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        ValueIO.Reader reader = rdfFactory.createReader(vf);
        Resource subj = vf.createIRI(SUBJ);
        RDFSubject rdfSubj = rdfFactory.createSubject(subj);
        RDFPredicate rdfPred = rdfFactory.createPredicate(RDF.VALUE);

        Set<Literal> actual = new HashSet<>();
        Scan scan = rdfFactory.getSPOIndex().scanWithConstraints(rdfSubj, rdfPred, new LiteralConstraints(XSD.STRING));
        try (ResultScanner rs = table.getScanner(scan)) {
            Result r;
            while ((r = rs.next()) != null) {
                for (Statement stmt : HalyardTableUtils.parseStatements(rdfSubj, rdfPred, null, null, r, reader, rdfFactory)) {
                    actual.add((Literal) stmt.getObject());
                }
            }
        }
        Set<Literal> nonStringLiterals = new HashSet<Literal>(allLiterals);
        nonStringLiterals.removeAll(stringLiterals);
        assertEquals(stringLiterals, actual);
    }

    @Test
    public void testScanStringLiterals_POS() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        ValueIO.Reader reader = rdfFactory.createReader(vf);
        RDFPredicate rdfPred = rdfFactory.createPredicate(RDF.VALUE);

        Set<Literal> actual = new HashSet<>();
        Scan scan = rdfFactory.getPOSIndex().scanWithConstraints(rdfPred, new LiteralConstraints(XSD.STRING));
        try (ResultScanner rs = table.getScanner(scan)) {
            Result r;
            while ((r = rs.next()) != null) {
                for (Statement stmt : HalyardTableUtils.parseStatements(null, rdfPred, null, null, r, reader, rdfFactory)) {
                    actual.add((Literal) stmt.getObject());
                }
            }
        }
        assertEquals(stringLiterals, actual);
    }

    @Test
    public void testScanNonStringLiterals_POS() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        ValueIO.Reader reader = rdfFactory.createReader(vf);
        RDFPredicate rdfPred = rdfFactory.createPredicate(RDF.VALUE);

        Set<Literal> actual = new HashSet<>();
        Scan scan = rdfFactory.getPOSIndex().scanWithConstraints(rdfPred, new LiteralConstraints(XSD.STRING));
        try (ResultScanner rs = table.getScanner(scan)) {
            Result r;
            while ((r = rs.next()) != null) {
                for (Statement stmt : HalyardTableUtils.parseStatements(null, rdfPred, null, null, r, reader, rdfFactory)) {
                    actual.add((Literal) stmt.getObject());
                }
            }
        }
        Set<Literal> nonStringLiterals = new HashSet<Literal>(allLiterals);
        nonStringLiterals.removeAll(stringLiterals);
        assertEquals(stringLiterals, actual);
    }

    @Test
    public void testScanStringLiterals_OSP() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        ValueIO.Reader reader = rdfFactory.createReader(vf);

        Set<Literal> actual = new HashSet<>();
        Scan scan = rdfFactory.getOSPIndex().scanWithConstraints(new LiteralConstraints(XSD.STRING));
        try (ResultScanner rs = table.getScanner(scan)) {
            Result r;
            while ((r = rs.next()) != null) {
                for (Statement stmt : HalyardTableUtils.parseStatements(null, null, null, null, r, reader, rdfFactory)) {
                    actual.add((Literal) stmt.getObject());
                }
            }
        }
        assertEquals(stringLiterals, actual);
    }

    @Test
    public void testScanNonStringLiterals_OSP() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        ValueIO.Reader reader = rdfFactory.createReader(vf);

        Set<Literal> actual = new HashSet<>();
        Scan scan = rdfFactory.getOSPIndex().scanWithConstraints(new LiteralConstraints(XSD.STRING));
        try (ResultScanner rs = table.getScanner(scan)) {
            Result r;
            while ((r = rs.next()) != null) {
                for (Statement stmt : HalyardTableUtils.parseStatements(null, null, null, null, r, reader, rdfFactory)) {
                    actual.add((Literal) stmt.getObject());
                }
            }
        }
        Set<Literal> nonStringLiterals = new HashSet<Literal>(allLiterals);
        nonStringLiterals.removeAll(stringLiterals);
        assertEquals(stringLiterals, actual);
    }
}
