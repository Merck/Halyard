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
import com.msd.gin.halyard.sail.HALYARD;
import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.sail.VOID_EXT;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.hadoop.util.ToolRunner;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
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
import org.eclipse.rdf4j.sail.SailException;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardStatsTest {

    @Test
    public void testStatsTarget() throws Exception {
        final HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "statsTable", true, -1, true, 0, null, null);
        sail.initialize();
        try (InputStream ref = HalyardStatsTest.class.getResourceAsStream("testData.trig")) {
            RDFParser p = Rio.createParser(RDFFormat.TRIG);
            p.setPreserveBNodeIDs(true);
            p.setRDFHandler(new AbstractRDFHandler() {
                @Override
                public void handleStatement(Statement st) throws RDFHandlerException {
                    sail.addStatement(st.getSubject(), st.getPredicate(), st.getObject(), st.getContext());
                }
            }).parse(ref, "");
        }
        sail.commit();
        sail.close();

        File root = File.createTempFile("test_stats", "");
        root.delete();
        root.mkdirs();

        assertEquals(0, ToolRunner.run(HBaseServerTestInstance.getInstanceConfig(), new HalyardStats(),
                new String[]{"-s", "statsTable", "-t", root.toURI().toURL().toString() + "stats{0}.trig", "-r", "100", "-g", "http://whatever/myStats"}));

        File stats = new File(root, "stats0.trig");
        assertTrue(stats.isFile());
        try (InputStream statsStream = new FileInputStream(stats)) {
            try (InputStream refStream = HalyardStatsTest.class.getResourceAsStream("testStatsTarget.trig")) {
                Model statsM = Rio.parse(statsStream, "", RDFFormat.TRIG, new ParserConfig().set(BasicParserSettings.PRESERVE_BNODE_IDS, true), SimpleValueFactory.getInstance(), new ParseErrorLogger());
                Model refM = Rio.parse(refStream, "", RDFFormat.TRIG, new ParserConfig().set(BasicParserSettings.PRESERVE_BNODE_IDS, true), SimpleValueFactory.getInstance(), new ParseErrorLogger(), SimpleValueFactory.getInstance().createIRI("http://whatever/myStats"));
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
        w.handleNamespace(XMLSchema.PREFIX, XMLSchema.NAMESPACE);
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
            sb.append("Missing statements:\n").append(out.toString()).append('\n');
        }
        out = new ByteArrayOutputStream();
        w = Rio.createWriter(RDFFormat.TRIG, out);
        w.getWriterConfig().set(BasicWriterSettings.PRETTY_PRINT, true);
        w.startRDF();
        w.handleNamespace("", "http://whatever/");
        w.handleNamespace(HALYARD.PREFIX, HALYARD.NAMESPACE);
        w.handleNamespace(VOID.PREFIX, VOID.NAMESPACE);
        w.handleNamespace(VOID_EXT.PREFIX, VOID_EXT.NAMESPACE);
        w.handleNamespace(XMLSchema.PREFIX, XMLSchema.NAMESPACE);
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
        final HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "statsTable2", true, -1, true, 0, null, null);
        sail.initialize();

        //load test data
        try (InputStream ref = HalyardStatsTest.class.getResourceAsStream("testData.trig")) {
            RDFParser p = Rio.createParser(RDFFormat.TRIG);
            p.setPreserveBNodeIDs(true);
            p.setRDFHandler(new AbstractRDFHandler() {
                @Override
                public void handleStatement(Statement st) throws RDFHandlerException {
                    sail.addStatement(st.getSubject(), st.getPredicate(), st.getObject(), st.getContext());
                }
            }).parse(ref, "");
        }
        sail.commit();

        //update stats
        assertEquals(0, ToolRunner.run(HBaseServerTestInstance.getInstanceConfig(), new HalyardStats(),
                new String[]{"-s", "statsTable2", "-r", "100"}));

        //verify with golden file
        Set<Statement> statsM = new HashSet<>();
        try (CloseableIteration<? extends Statement,SailException> it = sail.getStatements(null, null, null, true, HALYARD.STATS_GRAPH_CONTEXT)) {
            while (it.hasNext()) {
                statsM.add(it.next());
            }
        }
        try (InputStream refStream = HalyardStatsTest.class.getResourceAsStream("testStatsTarget.trig")) {
            Model refM = Rio.parse(refStream, "", RDFFormat.TRIG, new ParserConfig().set(BasicParserSettings.PRESERVE_BNODE_IDS, true), SimpleValueFactory.getInstance(), new ParseErrorLogger());
            assertEqualModels(refM, statsM);
        }

        //load additional data
        try (InputStream ref = HalyardStatsTest.class.getResourceAsStream("testMoreData.trig")) {
            RDFParser p = Rio.createParser(RDFFormat.TRIG);
            p.setPreserveBNodeIDs(true);
            p.setRDFHandler(new AbstractRDFHandler() {
                @Override
                public void handleStatement(Statement st) throws RDFHandlerException {
                    sail.addStatement(st.getSubject(), st.getPredicate(), st.getObject(), st.getContext());
                }
            }).parse(ref, "");
        }
        sail.commit();

        //update stats only for graph1
        assertEquals(0, ToolRunner.run(HBaseServerTestInstance.getInstanceConfig(), new HalyardStats(),
                new String[]{"-s", "statsTable2", "-r", "100", "-c", "http://whatever/graph1"}));

        //verify with golden file
        statsM = new HashSet<>();
        try (CloseableIteration<? extends Statement,SailException> it = sail.getStatements(null, null, null, true, HALYARD.STATS_GRAPH_CONTEXT)) {
            while (it.hasNext()) {
                statsM.add(it.next());
            }
        }
        try (InputStream refStream = HalyardStatsTest.class.getResourceAsStream("testStatsMoreTarget.trig")) {
            Model refM = Rio.parse(refStream, "", RDFFormat.TRIG, new ParserConfig().set(BasicParserSettings.PRESERVE_BNODE_IDS, true), SimpleValueFactory.getInstance(), new ParseErrorLogger());
            assertEqualModels(refM, statsM);
        }

        sail.close();
    }

    @Test
    public void testStatsTargetPartial() throws Exception {
        final HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "statsTable3", true, -1, true, 0, null, null);
        sail.initialize();
        try (InputStream ref = HalyardStatsTest.class.getResourceAsStream("testData.trig")) {
            RDFParser p = Rio.createParser(RDFFormat.TRIG);
            p.setPreserveBNodeIDs(true);
            p.setRDFHandler(new AbstractRDFHandler() {
                @Override
                public void handleStatement(Statement st) throws RDFHandlerException {
                    sail.addStatement(st.getSubject(), st.getPredicate(), st.getObject(), st.getContext());
                }
            }).parse(ref, "");
        }
        sail.commit();
        sail.close();

        File root = File.createTempFile("test_stats", "");
        root.delete();
        root.mkdirs();

        assertEquals(0, ToolRunner.run(HBaseServerTestInstance.getInstanceConfig(), new HalyardStats(),
                new String[]{"-s", "statsTable3", "-t", root.toURI().toURL().toString() + "stats{0}.trig", "-r", "100", "-g", "http://whatever/myStats", "-c", "http://whatever/graph0"}));

        File stats = new File(root, "stats0.trig");
        assertTrue(stats.isFile());
        try (InputStream statsStream = new FileInputStream(stats)) {
            try (InputStream refStream = HalyardStatsTest.class.getResourceAsStream("testStatsTargetPartial.trig")) {
                Model statsM = Rio.parse(statsStream, "", RDFFormat.TRIG, new ParserConfig().set(BasicParserSettings.PRESERVE_BNODE_IDS, true), SimpleValueFactory.getInstance(), new ParseErrorLogger());
                Model refM = Rio.parse(refStream, "", RDFFormat.TRIG, new ParserConfig().set(BasicParserSettings.PRESERVE_BNODE_IDS, true), SimpleValueFactory.getInstance(), new ParseErrorLogger(), SimpleValueFactory.getInstance().createIRI("http://whatever/myStats"));
                assertEqualModels(refM, statsM);
            }
        }
    }

    public void testRunNoArgs() throws Exception {
        assertEquals(-1, new HalyardStats().run(new String[0]));
    }

    @Test
    public void testRunVersion() throws Exception {
        assertEquals(0, new HalyardStats().run(new String[]{"-v"}));
    }

    @Test(expected = UnrecognizedOptionException.class)
    public void testRunInvalid() throws Exception {
        new HalyardStats().run(new String[]{"-invalid"});
    }
}
