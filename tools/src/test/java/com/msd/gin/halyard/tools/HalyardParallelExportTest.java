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
import org.apache.hadoop.util.ToolRunner;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardParallelExportTest {

    @Test
    public void testExport() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "exportTable", true, 0, true, 0, null, null);
        sail.initialize();
        ValueFactory vf = SimpleValueFactory.getInstance();
        for (int i = 0; i < 1000; i++) {
            sail.addStatement(vf.createIRI("http://whatever/NTsubj"), vf.createIRI("http://whatever/NTpred" + i),  vf.createLiteral("whatever NT value " + i));
        }
        sail.commit();
        sail.close();

        File root = File.createTempFile("test_export", "");
        root.delete();
        root.mkdirs();

        assertEquals(0, ToolRunner.run(HBaseServerTestInstance.getInstanceConfig(), new HalyardParallelExport(),
                new String[]{"-Dmapreduce.job.maps=2", "-s", "exportTable", "-q", "PREFIX halyard: <http://merck.github.io/Halyard/ns#>\nselect * where {?s ?p ?o .\nFILTER (halyard:parallelSplitBy (?p))}", "-t", root.toURI().toURL().toString() + "data{0}.csv"}));

        File f0 = new File(root, "data0.csv");
        File f1 = new File(root, "data1.csv");
        assertTrue(f0.isFile());
        assertTrue(f1.isFile());
        assertEquals(1002, HalyardExportTest.getLinesCount(f0.toURI().toURL().toString(), null) + HalyardExportTest.getLinesCount(f1.toURI().toURL().toString(), null));
    }

    @Test
    public void testRunNoArgs() throws Exception {
        assertEquals(-1, new HalyardParallelExport().run(new String[0]));
    }
}
