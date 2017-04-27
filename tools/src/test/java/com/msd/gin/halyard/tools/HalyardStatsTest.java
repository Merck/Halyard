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
import java.io.FileInputStream;
import java.net.URLEncoder;
import org.apache.hadoop.util.ToolRunner;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardStatsTest {

    @Test
    public void testStats() throws Exception {
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
                new String[]{"-s", "statsTable", "-t", root.toURI().toURL().toString() + "stats.ttl"}));

        File f = new File(root, "stats.ttl");
        assertTrue(f.isFile());
        try (FileInputStream in = new FileInputStream(f)) {
            String defaultPrefix =  HBaseServerTestInstance.getInstanceConfig().getTrimmed("hbase.rootdir");
            if (!defaultPrefix.endsWith("/")) defaultPrefix = defaultPrefix + "/";
            IRI statsTable = vf.createIRI(defaultPrefix, "statsTable");
            IRI graph0 = vf.createIRI(statsTable.stringValue() + '/' + URLEncoder.encode("http://whatever/graph0","UTF-8"));
            IRI graph1 = vf.createIRI(statsTable.stringValue() + '/' + URLEncoder.encode("http://whatever/graph1","UTF-8"));
            String voidd = "http://rdfs.org/ns/void#";
            IRI triples = vf.createIRI(voidd, "triples");
            IRI distinctSubjects = vf.createIRI(voidd, "distinctSubjects");
            IRI properties = vf.createIRI(voidd, "properties");
            IRI distinctObjects = vf.createIRI(voidd, "distinctObjects");
            IRI classes = vf.createIRI(voidd, "classes");
            Model m = Rio.parse(in, defaultPrefix, RDFFormat.TURTLE);
            assertTrue(m.contains(statsTable, RDF.TYPE, HalyardStats.VOID_DATASET_TYPE));
            assertTrue(m.contains(statsTable, RDF.TYPE, HalyardStats.SD_DATASET_TYPE));
            assertTrue(m.contains(statsTable, distinctSubjects, vf.createLiteral(100l)));
            assertTrue(m.contains(statsTable, properties, vf.createLiteral(14l)));
            assertTrue(m.contains(statsTable, distinctObjects, vf.createLiteral(2000l)));
            assertTrue(m.contains(statsTable, triples, vf.createLiteral(2000l)));
            assertTrue(m.contains(statsTable, classes, vf.createLiteral(1000l)));
            assertTrue(m.contains(statsTable, HalyardStats.SD_NAMED_GRAPH_PRED, graph0));
            assertTrue(m.contains(statsTable, HalyardStats.SD_NAMED_GRAPH_PRED, graph1));
            assertTrue(m.contains(graph0, RDF.TYPE, HalyardStats.VOID_DATASET_TYPE));
            assertTrue(m.contains(graph0, RDF.TYPE, HalyardStats.SD_GRAPH_TYPE));
            assertTrue(m.contains(graph0, RDF.TYPE, HalyardStats.SD_NAMED_GRAPH_TYPE));
            assertTrue(m.contains(graph0, HalyardStats.SD_GRAPH_PRED, graph0));
            assertTrue(m.contains(graph0, HalyardStats.SD_NAME_PRED, vf.createIRI("http://whatever/graph0")));
            assertTrue(m.contains(graph0, distinctSubjects, vf.createLiteral(50l)));
            assertTrue(m.contains(graph0, properties, vf.createLiteral(14l)));
            assertTrue(m.contains(graph0, distinctObjects, vf.createLiteral(900l)));
            assertTrue(m.contains(graph0, triples, vf.createLiteral(900l)));
            assertTrue(m.contains(graph0, classes, vf.createLiteral(450l)));
            assertTrue(m.contains(graph1, RDF.TYPE, HalyardStats.VOID_DATASET_TYPE));
            assertTrue(m.contains(graph1, RDF.TYPE, HalyardStats.SD_GRAPH_TYPE));
            assertTrue(m.contains(graph1, RDF.TYPE, HalyardStats.SD_NAMED_GRAPH_TYPE));
            assertTrue(m.contains(graph1, HalyardStats.SD_GRAPH_PRED, graph1));
            assertTrue(m.contains(graph1, HalyardStats.SD_NAME_PRED, vf.createIRI("http://whatever/graph1")));
            assertTrue(m.contains(graph1, distinctSubjects, vf.createLiteral(50l)));
            assertTrue(m.contains(graph1, properties, vf.createLiteral(14l)));
            assertTrue(m.contains(graph1, distinctObjects, vf.createLiteral(900l)));
            assertTrue(m.contains(graph1, triples, vf.createLiteral(900l)));
            assertTrue(m.contains(graph1, classes, vf.createLiteral(450l)));
        }
    }

    public void testRunNoArgs() throws Exception {
        assertEquals(-1, new HalyardStats().run(new String[0]));
    }
}
