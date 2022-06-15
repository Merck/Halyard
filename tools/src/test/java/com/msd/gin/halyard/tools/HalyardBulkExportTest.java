/*
 * Copyright 2018 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
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
import java.io.PrintStream;

import org.apache.hadoop.util.ToolRunner;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.SailConnection;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardBulkExportTest extends AbstractHalyardToolTest {
	@Override
	protected AbstractHalyardTool createTool() {
		return new HalyardBulkExport();
	}

    @Test
    public void testBulkExport() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "bulkExportTable", true, 0, true, 0, null, null);
        sail.init();
        ValueFactory vf = SimpleValueFactory.getInstance();
		try (SailConnection conn = sail.getConnection()) {
			for (int i = 0; i < 1000; i++) {
				conn.addStatement(vf.createIRI("http://whatever/NTsubj"), vf.createIRI("http://whatever/NTpred" + i), vf.createLiteral("whatever NT value " + i));
			}
		}

        File root = createTempDir("test_bulkExport");
        File q = new File(root, "test_bulkExport.sparql");
        try (PrintStream qs = new PrintStream(q)) {
            qs.println("select * where {?s ?p ?o}");
        }

        File extraLib = File.createTempFile("testBulkExportLib", ".txt");
        extraLib.deleteOnExit();

        assertEquals(0, ToolRunner.run(HBaseServerTestInstance.getInstanceConfig(), new HalyardBulkExport(),
                new String[]{"-s", "bulkExportTable", "-q", q.toURI().toURL().toString(), "-t", root.toURI().toURL().toString() + "{0}.csv", "-l" ,extraLib.getAbsolutePath()}));

        File f = new File(root, "test_bulkExport.csv");
        assertTrue(f.isFile());
        assertEquals(1001, HalyardExportTest.getLinesCount(f.toURI().toURL().toString(), null));

        q.delete();
        f.delete();
        root.delete();
        extraLib.delete();
    }

    @Test
    public void testParallelBulkExport() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "bulkExportTable2", true, 0, true, 0, null, null);
        sail.init();
        ValueFactory vf = SimpleValueFactory.getInstance();
		try (SailConnection conn = sail.getConnection()) {
			for (int i = 0; i < 1000; i++) {
				conn.addStatement(vf.createIRI("http://whatever/NTsubj"), vf.createIRI("http://whatever/NTpred" + i), vf.createLiteral("whatever NT value " + i));
			}
		}

        File root = createTempDir("test_parallelBulkExport");

        File q = new File(root, "test_parallelBulkExport.sparql");
        try (PrintStream qs = new PrintStream(q)) {
            qs.println("select * where {?s ?p ?o. FILTER (<http://merck.github.io/Halyard/ns#forkAndFilterBy> (2, ?p))}");
        }

        assertEquals(0, run(
                new String[]{"-s", "bulkExportTable2", "-q", q.toURI().toURL().toString(), "-t", root.toURI().toURL().toString() + "{0}-{1}.csv"}));

        File f1 = new File(root, "test_parallelBulkExport-0.csv");
        assertTrue(f1.isFile());
        File f2 = new File(root, "test_parallelBulkExport-1.csv");
        assertTrue(f2.isFile());
        assertEquals(1002, HalyardExportTest.getLinesCount(f1.toURI().toURL().toString(), null) + HalyardExportTest.getLinesCount(f2.toURI().toURL().toString(), null));

        q.delete();
        f1.delete();
        f2.delete();
        root.delete();
    }
}
