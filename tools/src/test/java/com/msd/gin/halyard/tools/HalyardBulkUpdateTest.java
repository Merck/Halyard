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
import java.io.PrintStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.SailException;
import org.junit.Assert;
import static org.junit.Assert.*;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardBulkUpdateTest {

    private static final String TABLE = "bulkupdatetesttable";

    @Test
    public void testBulkUpdate() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Configuration conf = HBaseServerTestInstance.getInstanceConfig();
        HBaseSail sail = new HBaseSail(conf, TABLE, true, -1, true, 0, null, null);
        sail.initialize();
        for (int i=0; i<5; i++) {
            for (int j=0; j<5; j++) {
                sail.addStatement(vf.createIRI("http://whatever/subj" + i), vf.createIRI("http://whatever/pred"), vf.createIRI("http://whatever/obj" + j));
            }
        }
        sail.commit();
        sail.shutDown();

        File queries = File.createTempFile("test_update_queries", ".sparql");
        try (PrintStream qs = new PrintStream(queries)) {
            qs.println("construct {?o <http://whatever/reverse> ?s} where {?s <http://whatever/pred> ?o}");
        }
        File htableDir = File.createTempFile("test_htable", "");
        htableDir.delete();

        assertEquals(0, ToolRunner.run(conf, new HalyardBulkUpdate(), new String[]{ queries.toURI().toURL().toString(), htableDir.toURI().toURL().toString(), TABLE}));

        sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), TABLE, false, 0, true, 0, null, null);
        sail.initialize();
        try {
            int count;
            try (CloseableIteration<? extends Statement, SailException> iter = sail.getStatements(null, SimpleValueFactory.getInstance().createIRI("http://whatever/reverse"), null, true)) {
                count = 0;
                while (iter.hasNext()) {
                    iter.next();
                    count++;
                }
            }
            Assert.assertEquals(25, count);
        } finally {
            sail.shutDown();
        }
    }

    @Test
    public void testRunNoArgs() throws Exception {
        assertEquals(-1, new HalyardBulkUpdate().run(new String[0]));
    }
}
