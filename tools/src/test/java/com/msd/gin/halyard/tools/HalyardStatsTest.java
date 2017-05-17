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
import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.MessageFormat;
import org.apache.hadoop.util.ToolRunner;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
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
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "statsTable", true, -1, true, 0, null);
        sail.initialize();
        ValueFactory vf = SimpleValueFactory.getInstance();
        for (int i = 0; i < 1000; i++) {
            sail.addStatement(vf.createIRI("http://whatever/subj" + (i % 100)), vf.createIRI("http://whatever/pred" + (i % 13)),  vf.createLiteral("whatever value " + i), i < 100 ? null : vf.createIRI("http://whatever/graph" + (i % 2)));
            sail.addStatement(vf.createIRI("http://whatever/subj" + (i % 100)), RDF.TYPE,  vf.createIRI("http://whatever/type" + i), i < 100 ? null : vf.createIRI("http://whatever/graph" + (i % 2)));
        }
        sail.commit();
        sail.close();

        File root = File.createTempFile("test_stats", "");
        root.delete();
        root.mkdirs();

        assertEquals(0, ToolRunner.run(HBaseServerTestInstance.getInstanceConfig(), new HalyardStats(),
                new String[]{"-s", "statsTable", "-t", root.toURI().toURL().toString() + "stats.trig"}));

        File f = new File(root, "stats.trig");
        assertTrue(f.isFile());
        System.out.println(f.getAbsolutePath());
        String content =new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8);
        try (FileInputStream in = new FileInputStream(f)) {
            IRI graph0 = vf.createIRI("http://whatever/graph0");
            IRI graph1 = vf.createIRI("http://whatever/graph1");
            String voidd = "http://rdfs.org/ns/void#";
            IRI triples = vf.createIRI(voidd, "triples");
            IRI distinctSubjects = vf.createIRI(voidd, "distinctSubjects");
            IRI properties = vf.createIRI(voidd, "properties");
            IRI distinctObjects = vf.createIRI(voidd, "distinctObjects");
            IRI classes = vf.createIRI(voidd, "classes");
            Model m = Rio.parse(in, "", RDFFormat.TRIG);

            assertContains(m, content, HALYARD.STATS_ROOT_NODE, RDF.TYPE, VOID.DATASET);
            assertContains(m, content, HALYARD.STATS_ROOT_NODE, RDF.TYPE, SD.DATASET);
            assertContains(m, content, HALYARD.STATS_ROOT_NODE, RDF.TYPE, SD.GRAPH_CLASS);
            assertContains(m, content, HALYARD.STATS_ROOT_NODE, distinctSubjects, vf.createLiteral(100l));
            assertContains(m, content, HALYARD.STATS_ROOT_NODE, properties, vf.createLiteral(14l));
            assertContains(m, content, HALYARD.STATS_ROOT_NODE, distinctObjects, vf.createLiteral(2000l));
            assertContains(m, content, HALYARD.STATS_ROOT_NODE, triples, vf.createLiteral(2000l));
            assertContains(m, content, HALYARD.STATS_ROOT_NODE, classes, vf.createLiteral(1000l));
            assertContains(m, content, HALYARD.STATS_ROOT_NODE, SD.DEFAULT_GRAPH, HALYARD.STATS_ROOT_NODE);
            assertContains(m, content, HALYARD.STATS_ROOT_NODE, SD.NAMED_GRAPH_PROPERTY, graph0);
            assertContains(m, content, HALYARD.STATS_ROOT_NODE, SD.NAMED_GRAPH_PROPERTY, graph1);

            assertContains(m, content, graph0, RDF.TYPE, VOID.DATASET);
            assertContains(m, content, graph0, RDF.TYPE, SD.GRAPH_CLASS);
            assertContains(m, content, graph0, RDF.TYPE, SD.NAMED_GRAPH_CLASS);
            assertContains(m, content, graph0, SD.GRAPH_PROPERTY, graph0);
            assertContains(m, content, graph0, SD.NAME, graph0);
            assertContains(m, content, graph0, distinctSubjects, vf.createLiteral(50l));
            assertContains(m, content, graph0, properties, vf.createLiteral(14l));
            assertContains(m, content, graph0, distinctObjects, vf.createLiteral(900l));
            assertContains(m, content, graph0, triples, vf.createLiteral(900l));
            assertContains(m, content, graph0, classes, vf.createLiteral(450l));

            assertContains(m, content, graph1, RDF.TYPE, VOID.DATASET);
            assertContains(m, content, graph1, RDF.TYPE, SD.GRAPH_CLASS);
            assertContains(m, content, graph1, RDF.TYPE, SD.NAMED_GRAPH_CLASS);
            assertContains(m, content, graph1, SD.GRAPH_PROPERTY, graph1);
            assertContains(m, content, graph1, SD.NAME, graph1);
            assertContains(m, content, graph1, distinctSubjects, vf.createLiteral(50l));
            assertContains(m, content, graph1, properties, vf.createLiteral(14l));
            assertContains(m, content, graph1, distinctObjects, vf.createLiteral(900l));
            assertContains(m, content, graph1, triples, vf.createLiteral(900l));
            assertContains(m, content, graph1, classes, vf.createLiteral(450l));
        }
    }

    private static void assertContains(Model model, String content, Resource subj, IRI pred, Value obj) {
        if (!model.contains(subj, pred, obj, HALYARD.STATS_GRAPH_CONTEXT)) {
            fail(MessageFormat.format("Expected {0} {1} {2} in:\n{3}\n", subj, pred, obj, content));
        }
    }

    @Test
    public void testStatsUpdate() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "statsTable2", true, -1, true, 0, null);
        sail.initialize();
        ValueFactory vf = SimpleValueFactory.getInstance();
        for (int i = 0; i < 1000; i++) {
            sail.addStatement(vf.createIRI("http://whatever/subj" + (i % 100)), vf.createIRI("http://whatever/pred" + (i % 13)),  vf.createLiteral("whatever value " + i), i < 100 ? null : vf.createIRI("http://whatever/graph" + (i % 2)));
            sail.addStatement(vf.createIRI("http://whatever/subj" + (i % 100)), RDF.TYPE,  vf.createIRI("http://whatever/type" + i), i < 100 ? null : vf.createIRI("http://whatever/graph" + (i % 2)));
        }
        sail.commit();
        sail.close();

        assertEquals(0, ToolRunner.run(HBaseServerTestInstance.getInstanceConfig(), new HalyardStats(),
                new String[]{"-s", "statsTable2"}));

        sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "statsTable2", false, -1, true, 0, null);
        sail.initialize();

        IRI graph0 = vf.createIRI("http://whatever/graph0");
        IRI graph1 = vf.createIRI("http://whatever/graph1");

        assertContains(sail, HALYARD.STATS_ROOT_NODE, RDF.TYPE, VOID.DATASET);
        assertContains(sail, HALYARD.STATS_ROOT_NODE, RDF.TYPE, SD.DATASET);
        assertContains(sail, HALYARD.STATS_ROOT_NODE, RDF.TYPE, SD.GRAPH_CLASS);
        assertContains(sail, HALYARD.STATS_ROOT_NODE, VOID.DISTINCT_SUBJECTS, vf.createLiteral(100l));
        assertContains(sail, HALYARD.STATS_ROOT_NODE, VOID.PROPERTIES, vf.createLiteral(14l));
        assertContains(sail, HALYARD.STATS_ROOT_NODE, VOID.DISTINCT_OBJECTS, vf.createLiteral(2000l));
        assertContains(sail, HALYARD.STATS_ROOT_NODE, VOID.TRIPLES, vf.createLiteral(2000l));
        assertContains(sail, HALYARD.STATS_ROOT_NODE, VOID.CLASSES, vf.createLiteral(1000l));
        assertContains(sail, HALYARD.STATS_ROOT_NODE, SD.DEFAULT_GRAPH, HALYARD.STATS_ROOT_NODE);
        assertContains(sail, HALYARD.STATS_ROOT_NODE, SD.NAMED_GRAPH_PROPERTY, graph0);
        assertContains(sail, HALYARD.STATS_ROOT_NODE, SD.NAMED_GRAPH_PROPERTY, graph1);

        assertContains(sail, graph0, RDF.TYPE, VOID.DATASET);
        assertContains(sail, graph0, RDF.TYPE, SD.GRAPH_CLASS);
        assertContains(sail, graph0, RDF.TYPE, SD.NAMED_GRAPH_CLASS);
        assertContains(sail, graph0, SD.GRAPH_PROPERTY, graph0);
        assertContains(sail, graph0, SD.NAME, graph0);
        assertContains(sail, graph0, VOID.DISTINCT_SUBJECTS, vf.createLiteral(50l));
        assertContains(sail, graph0, VOID.PROPERTIES, vf.createLiteral(14l));
        assertContains(sail, graph0, VOID.DISTINCT_OBJECTS, vf.createLiteral(900l));
        assertContains(sail, graph0, VOID.TRIPLES, vf.createLiteral(900l));
        assertContains(sail, graph0, VOID.CLASSES, vf.createLiteral(450l));

        assertContains(sail, graph1, RDF.TYPE, VOID.DATASET);
        assertContains(sail, graph1, RDF.TYPE, SD.GRAPH_CLASS);
        assertContains(sail, graph1, RDF.TYPE, SD.NAMED_GRAPH_CLASS);
        assertContains(sail, graph1, SD.GRAPH_PROPERTY, graph1);
        assertContains(sail, graph1, SD.NAME, graph1);
        assertContains(sail, graph1, VOID.DISTINCT_SUBJECTS, vf.createLiteral(50l));
        assertContains(sail, graph1, VOID.PROPERTIES, vf.createLiteral(14l));
        assertContains(sail, graph1, VOID.DISTINCT_OBJECTS, vf.createLiteral(900l));
        assertContains(sail, graph1, VOID.TRIPLES, vf.createLiteral(900l));
        assertContains(sail, graph1, VOID.CLASSES, vf.createLiteral(450l));

        sail.close();
    }

    private static void assertContains(HBaseSail sail, Resource subj, IRI pred, Value obj) {
        try (CloseableIteration<? extends Statement,SailException> it = sail.getStatements(subj, pred, obj, true, HALYARD.STATS_GRAPH_CONTEXT)) {
            if (!it.hasNext()) {
                fail(MessageFormat.format("Expected {0} {1} {2}", subj, pred, obj));
            }
        }
    }

    public void testRunNoArgs() throws Exception {
        assertEquals(-1, new HalyardStats().run(new String[0]));
    }
}
