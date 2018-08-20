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
import java.net.URI;
import org.apache.commons.cli.MissingOptionException;
import org.junit.Test;
import org.junit.BeforeClass;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.util.ToolRunner;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.sail.SailException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TestName;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardUpdateTest {

    private static final String TABLE = "updatetesttable";
    private static String ROOT;

    @Rule
    public TestName name = new TestName();

    @BeforeClass
    public static void setup() throws Exception {
        File rf = File.createTempFile("HalyardUpdateTest", "");
        rf.delete();
        rf.mkdirs();
        ROOT = rf.toURI().toURL().toString();
        if (!ROOT.endsWith("/")) {
            ROOT = ROOT + "/";
        }
        ValueFactory vf = SimpleValueFactory.getInstance();
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), TABLE, true, 0, true, 0, null, null);
        sail.initialize();
        for (int i=0; i<5; i++) {
            for (int j=0; j<5; j++) {
                sail.addStatement(vf.createIRI("http://whatever/subj" + i), vf.createIRI("http://whatever/pred"), vf.createIRI("http://whatever/obj" + j));
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
        runUpdate("-h");
    }

    @Test(expected = MissingOptionException.class)
    public void testRunNoArgs() throws Exception {
        runUpdate();
    }

    @Test
    public void testVersion() throws Exception {
        runUpdate("-v");
    }

    @Test(expected = ParseException.class)
    public void testMissingArgs() throws Exception {
        runUpdate("-s", "whatever");
    }

    @Test(expected = ParseException.class)
    public void testUnknownArg() throws Exception {
        runUpdate("-y");
    }

    @Test(expected = ParseException.class)
    public void testDupArgs() throws Exception {
        runUpdate("-s", "whatever", "-q", "query", "-s", "whatever2");
    }

    @Test(expected = MalformedQueryException.class)
    public void testInvalidQuery() throws Exception {
        runUpdate("-s", TABLE, "-q", "construct {?s ?p ?o} where {?s ?p ?o}");
    }

    @Test
    public void testInsertAndDelete() throws Exception {
        runUpdate("-s", TABLE, "-q", "insert {?o <http://whatever/reverse> ?s} where {?s <http://whatever/pred> ?o}");
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), TABLE, false, 0, true, 0, null, null);
        sail.initialize();
        try {
            CloseableIteration<? extends Statement, SailException> iter = sail.getStatements(null, SimpleValueFactory.getInstance().createIRI("http://whatever/reverse"), null, true);
            int count = 0;
            while (iter.hasNext()) {
                iter.next();
                count++;
            }
            iter.close();
            Assert.assertEquals(25, count);
        } finally {
            sail.shutDown();
        }

        runUpdate("-s", TABLE, "-q", "delete {?s <http://whatever/reverse> ?o} where {?s <http://whatever/reverse> ?o}");
        sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), TABLE, false, 0, true, 0, null, null);
        sail.initialize();
        try {
            CloseableIteration<? extends Statement, SailException> iter = sail.getStatements(null, SimpleValueFactory.getInstance().createIRI("http://whatever/reverse"), null, true);
            Assert.assertFalse(iter.hasNext());
            iter.close();
        } finally {
            sail.shutDown();
        }
    }

    private static int runUpdate(String ... args) throws Exception {
        return ToolRunner.run(HBaseServerTestInstance.getInstanceConfig(), new HalyardUpdate(), args);
    }
}
