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
import com.msd.gin.halyard.tools.HalyardExport.ExportException;
import com.msd.gin.halyard.sail.HBaseSail;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.MissingOptionException;
import org.junit.Test;
import org.junit.BeforeClass;
import org.apache.commons.cli.ParseException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Rule;
import org.junit.rules.TestName;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardExportTest {

    private static final String TABLE = "exporttesttable";
    private static final String TUPLE_QUERY = "select * where {?s ?p ?o}";
    private static final String GRAPH_QUERY = "construct {?s ?p ?o} where {?s ?p ?o}";
    private static String ROOT;

    @Rule
    public TestName name = new TestName();

    @BeforeClass
    public static void setup() throws Exception {
        File rf = File.createTempFile("HalyardExportTest", "");
        rf.delete();
        rf.mkdirs();
        ROOT = rf.toURI().toURL().toString();
        if (!ROOT.endsWith("/")) {
            ROOT = ROOT + "/";
        }
        ValueFactory vf = SimpleValueFactory.getInstance();
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), TABLE, true, 0, true, 0, null, null);
        sail.initialize();
        for (int i=0; i<10; i++) {
            for (int j=0; j<10; j++) {
                for (int k=0; k<10; k++) {
                    sail.addStatement(vf.createIRI("http://whatever/subj" + i), vf.createIRI("http://whatever/pred" + j), vf.createLiteral("whatever\n\"\\" + k));
                }
            }
        }
        sail.commit();
        sail.shutDown();
    }

    @AfterClass
    public static void teardown() throws Exception {
        FileUtils.deleteDirectory(new File(URI.create(ROOT)));
    }

    @Test
    public void testHelp() throws Exception {
        runExport("-h");
    }

    @Test(expected = MissingOptionException.class)
    public void testRunNoArgs() throws Exception {
        runExport();
    }

    @Test
    public void testVersion() throws Exception {
        runExport("-v");
    }

    @Test(expected = ParseException.class)
    public void testMissingArgs() throws Exception {
        runExport("-s", "whatever", "-q", "query");
    }

    @Test(expected = ParseException.class)
    public void testUnknownArg() throws Exception {
        runExport("-y");
    }

    @Test(expected = ParseException.class)
    public void testDupArgs() throws Exception {
        runExport("-s", "whatever", "-q", "query", "-t", "target", "-s", "whatever2");
    }

    @Test
    public void testExport_CSV() throws Exception {
        runExport("-s", TABLE, "-q", TUPLE_QUERY, "-t", ROOT + name.getMethodName() +".csv");
        assertEquals(1001, getLinesCount(ROOT + name.getMethodName() +".csv", null));
    }

    @Test
    public void testExport_CSV_GZ() throws Exception {
        runExport("-s", TABLE, "-q", TUPLE_QUERY, "-t", ROOT + name.getMethodName() +".csv.gz");
        assertEquals(1001, getLinesCount(ROOT + name.getMethodName() +".csv.gz", CompressorStreamFactory.GZIP));
    }

    @Test
    public void testExport_CSV_BZ2() throws Exception {
        runExport("-s", TABLE, "-q", TUPLE_QUERY, "-t", ROOT + name.getMethodName() +".csv.bz2");
        assertEquals(1001, getLinesCount(ROOT + name.getMethodName() +".csv.bz2", CompressorStreamFactory.BZIP2));
    }

    @Test
    public void testExport_JSONLD() throws Exception {
        runExport("-s", TABLE, "-q", GRAPH_QUERY, "-t", ROOT + name.getMethodName() +".jsonld");
        assertEquals(1000, getTriplesCount(ROOT + name.getMethodName() +".jsonld", null, RDFFormat.JSONLD));
    }

    @Test
    public void testExport_NT_GZ() throws Exception {
        runExport("-s", TABLE, "-q", GRAPH_QUERY, "-t", ROOT + name.getMethodName() +".nt.gz");
        assertEquals(1000, getTriplesCount(ROOT + name.getMethodName() +".nt.gz", CompressorStreamFactory.GZIP, RDFFormat.NTRIPLES));
    }

    @Test
    public void testExport_TTL_BZ2() throws Exception {
        runExport("-s", TABLE, "-q", GRAPH_QUERY, "-t", ROOT + name.getMethodName() +".ttl.bz2");
        assertEquals(1000, getTriplesCount(ROOT + name.getMethodName() +".ttl.bz2", CompressorStreamFactory.BZIP2, RDFFormat.TURTLE));
    }

    @Test
    public void testExport_Graph_to_NULL() throws Exception {
        runExport("-s", TABLE, "-q", GRAPH_QUERY, "-t", "null:/");
    }

    @Test
    public void testExport_Tuples_to_NULL() throws Exception {
        runExport("-s", TABLE, "-q", TUPLE_QUERY, "-t", "null:/");
    }

    public static int getLinesCount(String uri, String compression) throws Exception {
        InputStream in = FileSystem.get(URI.create(uri), HBaseServerTestInstance.getInstanceConfig()).open(new Path(uri));
        try {
            if (compression != null) {
                in = new CompressorStreamFactory().createCompressorInputStream(compression, in);
            }
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            int i = 0;
            while (br.readLine() != null)  i++;
            return i;
        } finally {
            in.close();
        }
    }

    private static int getTriplesCount(String uri, String compression, RDFFormat format) throws Exception {
        InputStream in = FileSystem.get(URI.create(uri), HBaseServerTestInstance.getInstanceConfig()).open(new Path(uri));
        try {
            if (compression != null) {
                in = new CompressorStreamFactory().createCompressorInputStream(compression, in);
            }
            RDFParser parser = Rio.createParser(format);
            final AtomicInteger i = new AtomicInteger();
            parser.setRDFHandler(new AbstractRDFHandler(){
                @Override
                public void handleStatement(Statement st) throws RDFHandlerException {
                    i.incrementAndGet();
                }
            });
            parser.parse(in, uri);
            return i.get();
        } finally {
            in.close();
        }
    }

    @Test(expected = ExportException.class)
    public void testUnknownForm() throws Exception {
        runExport("-s", TABLE, "-q", TUPLE_QUERY, "-t", ROOT + "/testUnknownForm.xyz.gz");
    }

    @Test(expected = ExportException.class)
    public void testGraphToCSV() throws Exception {
        runExport("-s", TABLE, "-q", GRAPH_QUERY, "-t", ROOT + "/testGraphToCSV.csv");
    }

    @Test(expected = ExportException.class)
    public void testInvalidTableName() throws Exception {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        DriverManager.getConnection("jdbc:derby:memory:halyard-graph-export-test;create=true").close();
        try {
            runExport("-s", TABLE, "-q", TUPLE_QUERY, "-t", "jdbc:derby:memory:halyard-graph-export-test/what@ever", "-c", "org.apache.derby.jdbc.EmbeddedDriver");
        } finally {
            try {
                DriverManager.getConnection("jdbc:derby:memory:halyard-graph-export-test;shutdown=true").close();
            } catch (SQLException ignore) {}
        }
    }

    @Test(expected = ExportException.class)
    public void testGraphToJDBC() throws Exception {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        DriverManager.getConnection("jdbc:derby:memory:halyard-graph-export-test;create=true").close();
        try {
            runExport("-s", TABLE, "-q", GRAPH_QUERY, "-t", "jdbc:derby:memory:halyard-graph-export-test/whatever", "-c", "org.apache.derby.jdbc.EmbeddedDriver");
        } finally {
            try {
                DriverManager.getConnection("jdbc:derby:memory:halyard-graph-export-test;shutdown=true").close();
            } catch (SQLException ignore) {}
        }
    }

    @Test(expected = ExportException.class)
    public void testTupleToRDF() throws Exception {
        runExport("-s", TABLE, "-q", TUPLE_QUERY, "-t", ROOT + "/testTupleToRDF.nt");
    }

    @Test(expected = ExportException.class)
    public void testInvalidQuery() throws Exception {
        runExport("-s", TABLE, "-q", "ask {<http://whatever/subj> ?p ?o}", "-t", ROOT + name.getMethodName() +".csv");
    }

    @Test
    public void testExportJDBC() throws Exception {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        try (Connection c = DriverManager.getConnection("jdbc:derby:memory:halyard-export-test;create=true")) {
            c.createStatement().executeUpdate("create table " + name.getMethodName() + " (s varchar(100), p varchar(100), o varchar(100))");
        }
        try {
            runExport("-s", TABLE, "-q", TUPLE_QUERY, "-t", "jdbc:derby:memory:halyard-export-test/" + name.getMethodName(), "-c", "org.apache.derby.jdbc.EmbeddedDriver", "-r");
            try (Connection c = DriverManager.getConnection("jdbc:derby:memory:halyard-export-test")) {
                try (ResultSet rs = c.createStatement().executeQuery("select count(*) from " + name.getMethodName())) {
                    assertTrue(rs.next());
                    assertEquals(1000, rs.getInt(1));
                }
            }
        } finally {
            try {
                DriverManager.getConnection("jdbc:derby:memory:halyard-export-test;shutdown=true").close();
            } catch (SQLException ignore) {}
        }
    }

    private static int runExport(String ... args) throws Exception {
        return ToolRunner.run(HBaseServerTestInstance.getInstanceConfig(), new HalyardExport(), args);
    }
}
