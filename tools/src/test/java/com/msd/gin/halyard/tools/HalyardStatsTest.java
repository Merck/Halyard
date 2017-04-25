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
import org.apache.hadoop.util.ToolRunner;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
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
            IRI graph0 = vf.createIRI("http://whatever/graph0");
            IRI graph1 = vf.createIRI("http://whatever/graph1");
            String voidd = "http://rdfs.org/ns/void#";
            IRI triples = vf.createIRI(voidd, "triples");
            IRI distinctSubjects = vf.createIRI(voidd, "distinctSubjects");
            IRI properties = vf.createIRI(voidd, "properties");
            IRI distinctObjects = vf.createIRI(voidd, "distinctObjects");
            IRI classes = vf.createIRI(voidd, "classes");
            Model m = Rio.parse(in, defaultPrefix, RDFFormat.TURTLE);
            assertTrue(m.contains(statsTable, distinctSubjects, vf.createLiteral(100)));
            assertTrue(m.contains(statsTable, properties, vf.createLiteral(14)));
            assertTrue(m.contains(statsTable, distinctObjects, vf.createLiteral(2000)));
            assertTrue(m.contains(statsTable, triples, vf.createLiteral(2000)));
            assertTrue(m.contains(statsTable, classes, vf.createLiteral(1000)));
            assertTrue(m.contains(graph0, distinctSubjects, vf.createLiteral(50)));
            assertTrue(m.contains(graph0, properties, vf.createLiteral(14)));
            assertTrue(m.contains(graph0, distinctObjects, vf.createLiteral(900)));
            assertTrue(m.contains(graph0, triples, vf.createLiteral(900)));
            assertTrue(m.contains(graph0, classes, vf.createLiteral(450)));
            assertTrue(m.contains(graph1, distinctSubjects, vf.createLiteral(50)));
            assertTrue(m.contains(graph1, properties, vf.createLiteral(14)));
            assertTrue(m.contains(graph1, distinctObjects, vf.createLiteral(900)));
            assertTrue(m.contains(graph1, triples, vf.createLiteral(900)));
            assertTrue(m.contains(graph1, classes, vf.createLiteral(450)));

//:statsTable void:classes "1000"^^<http://www.w3.org/2001/XMLSchema#int> ;
//	void:distinctObjects "2000"^^<http://www.w3.org/2001/XMLSchema#int> ;
//	void:distinctSubjects "100"^^<http://www.w3.org/2001/XMLSchema#int> ;
//	void:properties "14"^^<http://www.w3.org/2001/XMLSchema#int> ;
//	void:triples "2000"^^<http://www.w3.org/2001/XMLSchema#int> .
//
//<http://whatever/graph0> void:classes "450"^^<http://www.w3.org/2001/XMLSchema#int> ;
//	void:distinctObjects "900"^^<http://www.w3.org/2001/XMLSchema#int> ;
//	void:distinctSubjects "50"^^<http://www.w3.org/2001/XMLSchema#int> ;
//	void:properties "14"^^<http://www.w3.org/2001/XMLSchema#int> ;
//	void:triples "900"^^<http://www.w3.org/2001/XMLSchema#int> .
//
//<http://whatever/graph1> void:classes "450"^^<http://www.w3.org/2001/XMLSchema#int> ;
//	void:distinctObjects "900"^^<http://www.w3.org/2001/XMLSchema#int> ;
//	void:distinctSubjects "50"^^<http://www.w3.org/2001/XMLSchema#int> ;
//	void:properties "14"^^<http://www.w3.org/2001/XMLSchema#int> ;
//	void:triples "900"^^<http://www.w3.org/2001/XMLSchema#int> .

        }
    }

    public void testRunNoArgs() throws Exception {
        assertEquals(-1, new HalyardStats().run(new String[0]));
    }
}
