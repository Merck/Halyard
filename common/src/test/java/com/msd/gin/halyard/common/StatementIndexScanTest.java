package com.msd.gin.halyard.common;

import com.msd.gin.halyard.vocab.HALYARD;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class StatementIndexScanTest {
    private static final String SUBJ = "http://whatever/subj";
    private static final String CTX = "http://whatever/ctx";

    private static final ValueFactory vf = SimpleValueFactory.getInstance();
    private static Connection hConn;
	private static KeyspaceConnection keyspaceConn;
	private static RDFFactory rdfFactory;
	private static ValueIO.Reader reader;
    private static Set<Statement> allStatements;
    private static Set<Literal> allLiterals;
    private static Set<Literal> stringLiterals;
    private static Set<Literal> nonstringLiterals;
    private static Set<Triple> allTriples;
    private static final Literal foobarLiteral = vf.createLiteral("foobar", vf.createIRI("http://whatever/datatype"));

    @BeforeClass
    public static void setup() throws Exception {
		Configuration conf = HBaseServerTestInstance.getInstanceConfig();
		hConn = HalyardTableUtils.getConnection(conf);
        Table table = HalyardTableUtils.getTable(hConn, "testStatementIndex", true, 0);
        keyspaceConn = new TableKeyspace.TableKeyspaceConnection(table);
		rdfFactory = RDFFactory.create(keyspaceConn);
		reader = rdfFactory.createTableReader(vf, keyspaceConn);

        stringLiterals = new HashSet<>();
        nonstringLiterals = new HashSet<>();
        for (int i=0; i<5; i++) {
			stringLiterals.add(vf.createLiteral(String.valueOf(Math.random())));
			stringLiterals.add(vf.createLiteral(String.valueOf(Math.random()), "en"));
			nonstringLiterals.add(vf.createLiteral(Math.random()));
			nonstringLiterals.add(vf.createLiteral((long) Math.random()));
        }
        nonstringLiterals.add(vf.createLiteral(new Date()));
        nonstringLiterals.add(foobarLiteral);
        allLiterals = new HashSet<>();
        allLiterals.addAll(stringLiterals);
        allLiterals.addAll(nonstringLiterals);
        Resource subj = vf.createIRI(SUBJ);
        allStatements = new HashSet<>();
        allTriples = new HashSet<>();
        for (Literal l : allLiterals) {
            allStatements.add(vf.createStatement(subj, RDF.VALUE, l, vf.createIRI(CTX)));
            // add some non-literal objects
            allStatements.add(vf.createStatement(subj, OWL.SAMEAS, vf.createBNode(), vf.createIRI(CTX)));
            // add some triples
            Triple t = vf.createTriple(subj, RDF.VALUE, l);
            allTriples.add(t);
            allStatements.add(vf.createStatement(subj, RDFS.SEEALSO, t));
        }
        long timestamp = System.currentTimeMillis();
		List<Put> puts = new ArrayList<>();
		for (Statement stmt : allStatements) {
            for (Cell kv : HalyardTableUtils.addKeyValues(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), stmt.getContext(), timestamp, rdfFactory)) {
				puts.add(new Put(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), kv.getTimestamp()).add(kv));
            }
		}
		table.put(puts);
		// expect triple statements to be returned
        for (Triple t : allTriples) {
            allStatements.add(vf.createStatement(t.getSubject(), t.getPredicate(), t.getObject(), HALYARD.TRIPLE_GRAPH_CONTEXT));
        }
    }

    @AfterClass
    public static void teardown() throws Exception {
        keyspaceConn.close();
    }

    @Test
    public void testScanAll() throws Exception {
        Set<Statement> actual = new HashSet<>();
        Scan scan = StatementIndex.scanAll(rdfFactory);
        try (ResultScanner rs = keyspaceConn.getScanner(scan)) {
            Result r;
            while ((r = rs.next()) != null) {
                for (Statement stmt : HalyardTableUtils.parseStatements(null, null, null, null, r, reader, rdfFactory)) {
                    actual.add(stmt);
                }
            }
        }
        assertSets(allStatements, actual);
    }

    @Test
    public void testScanLiterals() throws Exception {
        Set<Literal> actual = new HashSet<>();
        Scan scan = StatementIndex.scanLiterals(rdfFactory);
        try (ResultScanner rs = keyspaceConn.getScanner(scan)) {
            Result r;
            while ((r = rs.next()) != null) {
                for (Statement stmt : HalyardTableUtils.parseStatements(null, null, null, null, r, reader, rdfFactory)) {
                    assertTrue("Not a literal: "+stmt.getObject(), stmt.getObject().isLiteral());
                    actual.add((Literal) stmt.getObject());
                }
            }
        }
        assertSets(allLiterals, actual);
    }

    @Test
    public void testScanLiteralsContext() throws Exception {
        Set<Literal> actual = new HashSet<>();
        Scan scan = StatementIndex.scanLiterals(vf.createIRI(CTX), rdfFactory);
        try (ResultScanner rs = keyspaceConn.getScanner(scan)) {
            Result r;
            while ((r = rs.next()) != null) {
                for (Statement stmt : HalyardTableUtils.parseStatements(null, null, null, null, r, reader, rdfFactory)) {
                    assertTrue("Not a literal: "+stmt.getObject(), stmt.getObject().isLiteral());
                    actual.add((Literal) stmt.getObject());
                }
            }
        }
        assertSets(allLiterals, actual);
    }

    @Test
    public void testScanStringLiterals_SPO() throws Exception {
        Resource subj = vf.createIRI(SUBJ);
        RDFSubject rdfSubj = rdfFactory.createSubject(subj);
        RDFPredicate rdfPred = rdfFactory.createPredicate(RDF.VALUE);

        Set<Literal> actual = new HashSet<>();
        Scan scan = rdfFactory.getSPOIndex().scanWithConstraint(rdfSubj, rdfPred, new ObjectConstraint(XSD.STRING));
        try (ResultScanner rs = keyspaceConn.getScanner(scan)) {
            Result r;
            while ((r = rs.next()) != null) {
                for (Statement stmt : HalyardTableUtils.parseStatements(rdfSubj, rdfPred, null, null, r, reader, rdfFactory)) {
                    assertTrue("Not a literal: "+stmt.getObject(), stmt.getObject().isLiteral());
                    actual.add((Literal) stmt.getObject());
                }
            }
        }
        assertSets(stringLiterals, actual);
    }

    @Test
    public void testScanNonStringLiterals_CSPO() throws Exception {
        Resource subj = vf.createIRI(SUBJ);
        Resource ctx = vf.createIRI(CTX);
        RDFSubject rdfSubj = rdfFactory.createSubject(subj);
        RDFPredicate rdfPred = rdfFactory.createPredicate(RDF.VALUE);
        RDFContext rdfCtx = rdfFactory.createContext(ctx);

        Set<Literal> actual = new HashSet<>();
        Scan scan = rdfFactory.getCSPOIndex().scanWithConstraint(rdfCtx, rdfSubj, rdfPred, new ObjectConstraint(HALYARD.NON_STRING));
        try (ResultScanner rs = keyspaceConn.getScanner(scan)) {
            Result r;
            while ((r = rs.next()) != null) {
                for (Statement stmt : HalyardTableUtils.parseStatements(rdfSubj, rdfPred, null, rdfCtx, r, reader, rdfFactory)) {
                    assertTrue("Not a literal: "+stmt.getObject(), stmt.getObject().isLiteral());
                    actual.add((Literal) stmt.getObject());
                }
            }
        }
        assertSets(nonstringLiterals, actual);
    }

    @Test
    public void testScanTriples_SPO() throws Exception {
        Resource subj = vf.createIRI(SUBJ);
        RDFSubject rdfSubj = rdfFactory.createSubject(subj);
        RDFPredicate rdfPred = rdfFactory.createPredicate(RDFS.SEEALSO);

        Set<Triple> actual = new HashSet<>();
        Scan scan = rdfFactory.getSPOIndex().scanWithConstraint(rdfSubj, rdfPred, new ValueConstraint(ValueType.TRIPLE));
        try (ResultScanner rs = keyspaceConn.getScanner(scan)) {
            Result r;
            while ((r = rs.next()) != null) {
                for (Statement stmt : HalyardTableUtils.parseStatements(rdfSubj, rdfPred, null, null, r, reader, rdfFactory)) {
                    assertTrue("Not a triple: "+stmt.getObject(), stmt.getObject().isTriple());
                    actual.add((Triple) stmt.getObject());
                }
            }
        }
        assertSets(allTriples, actual);
    }

    @Test
    public void testScanStringLiterals_POS() throws Exception {
        RDFPredicate rdfPred = rdfFactory.createPredicate(RDF.VALUE);

        Set<Literal> actual = new HashSet<>();
        Scan scan = rdfFactory.getPOSIndex().scanWithConstraint(rdfPred, new ObjectConstraint(XSD.STRING));
        try (ResultScanner rs = keyspaceConn.getScanner(scan)) {
            Result r;
            while ((r = rs.next()) != null) {
                for (Statement stmt : HalyardTableUtils.parseStatements(null, rdfPred, null, null, r, reader, rdfFactory)) {
                    assertTrue("Not a literal: "+stmt.getObject(), stmt.getObject().isLiteral());
                    actual.add((Literal) stmt.getObject());
                }
            }
        }
        assertSets(stringLiterals, actual);
    }

    @Test
    public void testScanNonStringLiterals_CPOS() throws Exception {
        Resource ctx = vf.createIRI(CTX);
        RDFPredicate rdfPred = rdfFactory.createPredicate(RDF.VALUE);
        RDFContext rdfCtx = rdfFactory.createContext(ctx);

        Set<Literal> actual = new HashSet<>();
        Scan scan = rdfFactory.getCPOSIndex().scanWithConstraint(rdfCtx, rdfPred, new ObjectConstraint(HALYARD.NON_STRING));
        try (ResultScanner rs = keyspaceConn.getScanner(scan)) {
            Result r;
            while ((r = rs.next()) != null) {
                for (Statement stmt : HalyardTableUtils.parseStatements(null, rdfPred, null, rdfCtx, r, reader, rdfFactory)) {
                    assertTrue("Not a literal: "+stmt.getObject(), stmt.getObject().isLiteral());
                    actual.add((Literal) stmt.getObject());
                }
            }
        }
        assertSets(nonstringLiterals, actual);
    }

    @Test
    public void testScanTriples_POS() throws Exception {
        RDFPredicate rdfPred = rdfFactory.createPredicate(RDFS.SEEALSO);

        Set<Triple> actual = new HashSet<>();
        Scan scan = rdfFactory.getPOSIndex().scanWithConstraint(rdfPred, new ValueConstraint(ValueType.TRIPLE));
        try (ResultScanner rs = keyspaceConn.getScanner(scan)) {
            Result r;
            while ((r = rs.next()) != null) {
                for (Statement stmt : HalyardTableUtils.parseStatements(null, rdfPred, null, null, r, reader, rdfFactory)) {
                    assertTrue("Not a triple: "+stmt.getObject(), stmt.getObject().isTriple());
                    actual.add((Triple) stmt.getObject());
                }
            }
        }
        assertSets(allTriples, actual);
    }

    @Test
    public void testScanStringLiterals_OSP() throws Exception {
        Set<Literal> actual = new HashSet<>();
        Scan scan = rdfFactory.getOSPIndex().scanWithConstraint(new ObjectConstraint(XSD.STRING));
        try (ResultScanner rs = keyspaceConn.getScanner(scan)) {
            Result r;
            while ((r = rs.next()) != null) {
                for (Statement stmt : HalyardTableUtils.parseStatements(null, null, null, null, r, reader, rdfFactory)) {
                    assertTrue("Not a literal: "+stmt.getObject(), stmt.getObject().isLiteral());
                    actual.add((Literal) stmt.getObject());
                }
            }
        }
        assertSets(stringLiterals, actual);
    }

    @Test
    public void testScanNonStringLiterals_COSP() throws Exception {
        Resource ctx = vf.createIRI(CTX);
        RDFContext rdfCtx = rdfFactory.createContext(ctx);

        Set<Literal> actual = new HashSet<>();
        Scan scan = rdfFactory.getCOSPIndex().scanWithConstraint(rdfCtx, new ObjectConstraint(HALYARD.NON_STRING));
        try (ResultScanner rs = keyspaceConn.getScanner(scan)) {
            Result r;
            while ((r = rs.next()) != null) {
                for (Statement stmt : HalyardTableUtils.parseStatements(null, null, null, rdfCtx, r, reader, rdfFactory)) {
                   assertTrue("Not a literal: "+stmt.getObject(), stmt.getObject().isLiteral());
                   actual.add((Literal) stmt.getObject());
                }
            }
        }
        assertSets(nonstringLiterals, actual);
    }

    @Test
    public void testScanTriples_OSP() throws Exception {
        Set<Triple> actual = new HashSet<>();
        Scan scan = rdfFactory.getOSPIndex().scanWithConstraint(new ValueConstraint(ValueType.TRIPLE));
        try (ResultScanner rs = keyspaceConn.getScanner(scan)) {
            Result r;
            while ((r = rs.next()) != null) {
                for (Statement stmt : HalyardTableUtils.parseStatements(null, null, null, null, r, reader, rdfFactory)) {
                    assertTrue("Not a triple: "+stmt.getObject(), stmt.getObject().isTriple());
                    actual.add((Triple) stmt.getObject());
                }
            }
        }
        assertSets(allTriples, actual);
    }

    @Test
    public void testGetSubject() throws Exception {
        Resource subj = vf.createIRI(SUBJ);
    	Resource actual = HalyardTableUtils.getSubject(keyspaceConn, rdfFactory.id(subj), vf, rdfFactory);
    	assertEquals(subj, actual);
    }

    @Test
    public void testGetPredicate() throws Exception {
    	IRI pred = RDF.VALUE;
    	IRI actual = HalyardTableUtils.getPredicate(keyspaceConn, rdfFactory.id(pred), vf, rdfFactory);
    	assertEquals(pred, actual);
    }

    @Test
    public void testGetObject() throws Exception {
    	Value obj = foobarLiteral;
    	Value actual = HalyardTableUtils.getObject(keyspaceConn, rdfFactory.id(obj), vf, rdfFactory);
    	assertEquals(obj, actual);
    }

    private static <E> void assertSets(Set<E> expected, Set<E> actual) {
    	Set<E> diff = new HashSet<>(actual);
    	diff.removeAll(expected);
    	assertEquals("Unexpected: "+diff, expected, actual);
    }
}
