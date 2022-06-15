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
package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.sail.HBaseSail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.util.ToolRunner;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardBulkLoadTest extends AbstractHalyardToolTest {

	@Override
	protected AbstractHalyardTool createTool() {
		return new HalyardBulkLoad();
	}

    @Test
    public void testBulkLoad() throws Exception {
        File root = createTempDir("test_triples");
        File file1 = new File(root, "test_triples.jsonld");
        try (PrintStream ps = new PrintStream(file1)) {
            ps.println("{\"@graph\": [");
            for (int i = 0; i < 10; i++) {
                if (i > 0) ps.print(',');
                ps.println("{\"@id\": \"http://whatever/subj" + i +"\"");
                for (int j=0; j < 10; j++) {
                    ps.println(",\"http://whatever/pred" + j +"\": [");
                    for (int k=0; k < 8; k++) {
                        if (k > 0) ps.print(',');
                        ps.print("\"literal" + k + "\"");
                    }
                    ps.println(']');
                }
                ps.println("}");
            }
            ps.println("]}");
        }
        File file2 = new File(root, "test_triples.nt.gz");
        try (PrintStream ps = new PrintStream(new GZIPOutputStream(new FileOutputStream(file2)))) {
            for (int i = 0; i < 100; i++) {
                ps.println("<http://whatever/NTsubj> <http://whatever/NTpred" + i + "> \"whatever NT value" + i + "\" .");
            }
        }
        File file3 = new File(root, "test_quads.nq.gz");
        try (PrintStream ps = new PrintStream(new GZIPOutputStream(new FileOutputStream(file3)))) {
            for (int i = 0; i < 100; i++) {
                ps.println("<http://whatever/NQsubj> <http://whatever/NQpred" + i + "> \"whatever NQ value" + i + "\" <http://whatever/graph>.");
            }
        }
        File file4 = new File(root, "test_triples_invalid.nt");
        try (PrintStream ps = new PrintStream(file4)) {
            ps.println("this is an invalid NT file content");
        }
        File htableDir = getTempHTableDir("test_htable");

        //load with override of the graph context, however with no default graph context
        assertEquals(0, run(new String[]{"-b", "-1", "-i", "-d", "-s", root.toURI().toURL().toString(), "-w", htableDir.toURI().toURL().toString(), "-t", "bulkLoadTable", "-o", "-m", "1000"}));

        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "bulkLoadTable", false, 0, true, 0, null, null);
        SailRepository rep = new SailRepository(sail);
        rep.init();
        assertCount(rep, "select (count(*) as ?c) where {?s ?p ?o}", 1000);
        assertCount(rep, "select (count(*) as ?c) where {graph ?g{?s ?p ?o}}", 0);
        rep.shutDown();

        htableDir.delete();
        htableDir = getTempHTableDir("test_htable");

        //default load
        assertEquals(0, run(new String[]{"-b", "-1", "-i", "-d", "-s", root.toURI().toURL().toString(), "-w", htableDir.toURI().toURL().toString(), "-t", "bulkLoadTable", "-g", "{0}"}));

        sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "bulkLoadTable", false, 0, true, 0, null, null);
        rep = new SailRepository(sail);
        rep.init();
        assertCount(rep, "select (count(*) as ?c) where {graph <http://whatever/graph>{?s ?p ?o}}", 100);
        rep.shutDown();

        htableDir.delete();
        htableDir = getTempHTableDir("test_htable");

        //load with default graph context containing full URI pattern
        assertEquals(0, ToolRunner.run(HBaseServerTestInstance.getInstanceConfig(), new HalyardBulkLoad(), new String[]{"-b", "-1", "-i", "-d", "-s", root.toURI().toURL().toString(), "-w", htableDir.toURI().toURL().toString(), "-t", "bulkLoadTable", "-g", "{0}"}));

        sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "bulkLoadTable", false, 0, true, 0, null, null);
        rep = new SailRepository(sail);
        rep.init();
        assertCount(rep, "select (count(*) as ?c) where {graph <"+ file1.toURI().toString() + ">{?s ?p ?o}}", 800);
        assertCount(rep, "select (count(*) as ?c) where {graph <"+ file2.toURI().toString() + ">{?s ?p ?o}}", 100);
        assertCount(rep, "select (count(*) as ?c) where {graph <"+ file3.toURI().toString() + ">{?s ?p ?o}}", 0);
        rep.shutDown();

        htableDir.delete();
        htableDir = getTempHTableDir("test_htable");

        //load with graph context override containing URI path pattern
        assertEquals(0, run(new String[]{"-b", "-1", "-i", "-d", "-s", root.toURI().toURL().toString(), "-w", htableDir.toURI().toURL().toString(), "-t", "bulkLoadTable", "-o", "-g", "http://what{1}"}));

        sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "bulkLoadTable", false, 0, true, 0, null, null);
        rep = new SailRepository(sail);
        rep.init();
        assertCount(rep, "select (count(*) as ?c) where {graph <http://what"+ file1.toURI().getPath() + ">{?s ?p ?o}}", 800);
        assertCount(rep, "select (count(*) as ?c) where {graph <http://what"+ file2.toURI().getPath() + ">{?s ?p ?o}}", 100);
        assertCount(rep, "select (count(*) as ?c) where {graph <http://what"+ file3.toURI().getPath() + ">{?s ?p ?o}}", 100);
        rep.shutDown();

        file1.delete();
        file2.delete();
        file3.delete();
        file4.delete();
        root.delete();
        htableDir.delete();
    }

    @Test
    public void testDirtyBulkLoad() throws Exception {
        File file = File.createTempFile("test_triples", ".ttl.gz");
        try (PrintStream ps = new PrintStream(new GZIPOutputStream(new FileOutputStream(file)))) {
            ps.println("<http://whatever> <http://whatever> <http://invalid:invalid.com>, \"valid1\" .");
            ps.println("<http://whatever> <http://whatever> <http://valid2> .");
        }
        file.deleteOnExit();
        File htableDir = getTempHTableDir("test_htable");

        //load with override of the graph context, however with no default graph context
        assertEquals(0, run(new String[]{"-b", "-1", "-i", "-s", file.toURI().toURL().toString(), "-w", htableDir.toURI().toURL().toString(), "-t", "bulkLoadTable2"}));

        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "bulkLoadTable2", false, 0, true, 30, null, null);
        sail.init();
		try (SailConnection conn = sail.getConnection()) {
			assertEquals(3, conn.size());
		}
        sail.shutDown();
    }

    @Test
    public void testRDFStarLoad() throws Exception {
        File file = File.createTempFile("rdfstar", ".ttl");
        try (PrintStream ps = new PrintStream(new FileOutputStream(file))) {
            ps.println("<< <http://whatever/s> <http://whatever/p> <http://whatever/o> >> <http://whatever/from> <http://whatever/source> .");
        }
        file.deleteOnExit();
        File htableDir = getTempHTableDir("test_htable");

        //load with override of the graph context, however with no default graph context
        assertEquals(0, run(new String[]{"-s", file.toURI().toURL().toString(), "-w", htableDir.toURI().toURL().toString(), "-t", "bulkLoadTable3"}));

        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "bulkLoadTable3", false, 0, true, 30, null, null);
        sail.init();
		try (SailConnection conn = sail.getConnection()) {
			CloseableIteration<? extends Statement,? extends SailException> iter = conn.getStatements(null, null, null, false);
			assertTrue(iter.hasNext());
			Statement stmt = iter.next();
			assertFalse(iter.hasNext());
			assertTrue(stmt.getSubject().isTriple());
		}
        sail.shutDown();
    }

    private void assertCount(SailRepository rep, String query, int count) {
        TupleQuery q = rep.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, query);
        TupleQueryResult res = q.evaluate();
        assertTrue(query, res.hasNext());
        Value c = res.next().getValue("c");
        assertNotNull(query, c);
        assertTrue(query, c instanceof Literal);
        assertEquals(query, count, ((Literal)c).intValue());
    }
}
