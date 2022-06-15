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
import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.vocab.HALYARD;
import com.msd.gin.halyard.vocab.VOID_EXT;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.rio.ParserConfig;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.helpers.BasicWriterSettings;
import org.eclipse.rdf4j.rio.helpers.ParseErrorLogger;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.junit.Test;

import static org.junit.Assert.*;
/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardStatsTest extends AbstractHalyardToolTest {
	private final ValueFactory vf = SimpleValueFactory.getInstance();

	@Override
	protected AbstractHalyardTool createTool() {
		return new HalyardStats();
	}

	private static Sail createData(String tableName) throws Exception {
        final HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), tableName, true, -1, true, 0, null, null);
        sail.init();
		try (SailConnection conn = sail.getConnection()) {
			try (InputStream ref = HalyardStatsTest.class.getResourceAsStream("testData.trig")) {
				RDFParser p = Rio.createParser(RDFFormat.TRIG);
				p.setPreserveBNodeIDs(true);
				p.setRDFHandler(new AbstractRDFHandler() {
					@Override
					public void handleStatement(Statement st) throws RDFHandlerException {
						conn.addStatement(st.getSubject(), st.getPredicate(), st.getObject(), st.getContext());
					}
				}).parse(ref, "");
			}
		}
		return sail;
	}

	@Test
    public void testStatsTarget() throws Exception {
		Sail sail = createData("statsTable");
		sail.shutDown();

        File root = createTempDir("test_stats");

        assertEquals(0, run(new String[]{"-s", "statsTable", "-t", root.toURI().toURL().toString() + "stats{0}.trig", "-r", "100", "-o", "http://whatever/myStats"}));

        File stats = new File(root, "stats0.trig");
        assertTrue(stats.isFile());
        try (InputStream statsStream = new FileInputStream(stats)) {
            try (InputStream refStream = HalyardStatsTest.class.getResourceAsStream("testStatsTarget.trig")) {
                Model statsM = Rio.parse(statsStream, "", RDFFormat.TRIG, new ParserConfig().set(BasicParserSettings.PRESERVE_BNODE_IDS, true), vf, new ParseErrorLogger());
                Model refM = Rio.parse(refStream, "", RDFFormat.TRIG, new ParserConfig().set(BasicParserSettings.PRESERVE_BNODE_IDS, true), vf, new ParseErrorLogger(), vf.createIRI("http://whatever/myStats"));
                assertEqualModels(refM, statsM);
            }
        }
    }

    static void assertEqualModels(Set<Statement> ref, Set<Statement> m) {
        StringBuilder sb = new StringBuilder();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RDFWriter w = Rio.createWriter(RDFFormat.TRIG, out);
        w.getWriterConfig().set(BasicWriterSettings.PRETTY_PRINT, true);
        w.startRDF();
        w.handleNamespace("", "http://whatever/");
        w.handleNamespace(HALYARD.PREFIX, HALYARD.NAMESPACE);
        w.handleNamespace(VOID.PREFIX, VOID.NAMESPACE);
        w.handleNamespace(VOID_EXT.PREFIX, VOID_EXT.NAMESPACE);
        w.handleNamespace(XSD.PREFIX, XSD.NAMESPACE);
        w.handleNamespace(SD.PREFIX, SD.NAMESPACE);
        w.handleNamespace(RDF.PREFIX, RDF.NAMESPACE);
        boolean any = false;
        for (Statement st : ref) {
            if (!m.contains(st)) {
                any = true;
                w.handleStatement(st);
            }
        }
        w.endRDF();
        if (any) {
            sb.append("Expected but missing statements:\n").append(out.toString()).append('\n');
        }
        out = new ByteArrayOutputStream();
        w = Rio.createWriter(RDFFormat.TRIG, out);
        w.getWriterConfig().set(BasicWriterSettings.PRETTY_PRINT, true);
        w.startRDF();
        w.handleNamespace("", "http://whatever/");
        w.handleNamespace(HALYARD.PREFIX, HALYARD.NAMESPACE);
        w.handleNamespace(VOID.PREFIX, VOID.NAMESPACE);
        w.handleNamespace(VOID_EXT.PREFIX, VOID_EXT.NAMESPACE);
        w.handleNamespace(XSD.PREFIX, XSD.NAMESPACE);
        w.handleNamespace(SD.PREFIX, SD.NAMESPACE);
        w.handleNamespace(RDF.PREFIX, RDF.NAMESPACE);
        any = false;
        for (Statement st : m) {
            if (!ref.contains(st)) {
                any = true;
                w.handleStatement(st);
            }
        }
        w.endRDF();
        if (any) {
            sb.append("Unexpected statements:\n").append(out.toString()).append('\n');
        }
        if (sb.length() > 0) {
            fail(sb.toString());
        }
    }

    @Test
    public void testStatsUpdate() throws Exception {
        Sail sail = createData("statsTable2");

		// update stats
		assertEquals(0, run(new String[] { "-s", "statsTable2", "-r", "100" }));

		// verify with golden file
		try (SailConnection conn = sail.getConnection()) {
			Set<Statement> statsM = new HashSet<>();
			try (CloseableIteration<? extends Statement, SailException> it = conn.getStatements(null, null, null, true, HALYARD.STATS_GRAPH_CONTEXT)) {
				while (it.hasNext()) {
					statsM.add(it.next());
				}
			}
			try (InputStream refStream = HalyardStatsTest.class.getResourceAsStream("testStatsTarget.trig")) {
				Model refM = Rio.parse(refStream, "", RDFFormat.TRIG, new ParserConfig().set(BasicParserSettings.PRESERVE_BNODE_IDS, true), vf, new ParseErrorLogger());
				assertEqualModels(refM, statsM);
			}

			// load additional data
			try (InputStream ref = HalyardStatsTest.class.getResourceAsStream("testMoreData.trig")) {
				RDFParser p = Rio.createParser(RDFFormat.TRIG);
				p.setPreserveBNodeIDs(true);
				p.setRDFHandler(new AbstractRDFHandler() {
					@Override
					public void handleStatement(Statement st) throws RDFHandlerException {
						conn.addStatement(st.getSubject(), st.getPredicate(), st.getObject(), st.getContext());
					}
				}).parse(ref, "");
			}
		}

		// update stats only for graph1
		assertEquals(0, run(new String[] { "-s", "statsTable2", "-r", "100", "-g", "http://whatever/graph1" }));

		// verify with golden file
		try (SailConnection conn = sail.getConnection()) {
			Set<Statement> statsM = new HashSet<>();
			try (CloseableIteration<? extends Statement, SailException> it = conn.getStatements(null, null, null, true, HALYARD.STATS_GRAPH_CONTEXT)) {
				while (it.hasNext()) {
					statsM.add(it.next());
				}
			}
			try (InputStream refStream = HalyardStatsTest.class.getResourceAsStream("testStatsMoreTarget.trig")) {
				Model refM = Rio.parse(refStream, "", RDFFormat.TRIG, new ParserConfig().set(BasicParserSettings.PRESERVE_BNODE_IDS, true), vf, new ParseErrorLogger());
				assertEqualModels(refM, statsM);
			}

		}
    }

    @Test
    public void testStatsTargetPartial() throws Exception {
        Sail sail = createData("statsTable3");
		sail.shutDown();

        File root = createTempDir("test_stats");

        assertEquals(0, run(new String[]{"-s", "statsTable3", "-t", root.toURI().toURL().toString() + "stats{0}.trig", "-r", "100", "-o", "http://whatever/myStats", "-g", "http://whatever/graph0"}));

        File stats = new File(root, "stats0.trig");
        assertTrue(stats.isFile());
        try (InputStream statsStream = new FileInputStream(stats)) {
            try (InputStream refStream = HalyardStatsTest.class.getResourceAsStream("testStatsTargetPartial.trig")) {
                Model statsM = Rio.parse(statsStream, "", RDFFormat.TRIG, new ParserConfig().set(BasicParserSettings.PRESERVE_BNODE_IDS, true), vf, new ParseErrorLogger());
                Model refM = Rio.parse(refStream, "", RDFFormat.TRIG, new ParserConfig().set(BasicParserSettings.PRESERVE_BNODE_IDS, true), vf, new ParseErrorLogger(), vf.createIRI("http://whatever/myStats"));
                assertEqualModels(refM, statsM);
            }
        }
    }

	@Test
    public void testStatsTarget_snapshot() throws Exception {
		String table = "statsTable4";
		Sail sail = createData(table);
		sail.shutDown();

		String snapshot = table + "Snapshot";
		Configuration conf = HBaseServerTestInstance.getInstanceConfig();
    	try (Connection conn = HalyardTableUtils.getConnection(conf)) {
        	try (Admin admin = conn.getAdmin()) {
        		admin.snapshot(snapshot, TableName.valueOf(table));
        	}
    	}

        File root = createTempDir("test_stats_snapshot");
        File restoredSnapshot = getTempSnapshotDir("restored_snapshot");
        assertEquals(0, run(new String[]{"-s", snapshot, "-t", root.toURI().toURL().toString() + "stats{0}_snapshot.trig", "-r", "100", "-o", "http://whatever/myStats", "-u", restoredSnapshot.toURI().toURL().toString()}));

        File stats = new File(root, "stats0_snapshot.trig");
        assertTrue(stats.isFile());
        try (InputStream statsStream = new FileInputStream(stats)) {
            try (InputStream refStream = HalyardStatsTest.class.getResourceAsStream("testStatsTarget.trig")) {
                Model statsM = Rio.parse(statsStream, "", RDFFormat.TRIG, new ParserConfig().set(BasicParserSettings.PRESERVE_BNODE_IDS, true), vf, new ParseErrorLogger());
                Model refM = Rio.parse(refStream, "", RDFFormat.TRIG, new ParserConfig().set(BasicParserSettings.PRESERVE_BNODE_IDS, true), vf, new ParseErrorLogger(), vf.createIRI("http://whatever/myStats"));
                assertEqualModels(refM, statsM);
            }
        }
    }
}
