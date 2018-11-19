package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.sail.HBaseSail;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.util.ToolRunner;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertTrue;

/**
 * Class for testing the tool HalyardEndpoint.
 *
 * @author sykorjan
 */
public class HalyardEndpointTest {
    private static final String TABLE = "exporttesttable";
    private static String ROOT;

    @Rule
    public TestName name = new TestName();

    /**
     * Create temporary testing folder for testing files, create Sail repository and add testing data
     */
    @BeforeClass
    public static void setup() throws Exception {
        File rf = File.createTempFile("HalyardEndpointTest", "");
        rf.delete();
        rf.mkdirs();
        ROOT = rf.getPath();
        if (!ROOT.endsWith("/")) {
            ROOT = ROOT + "/";
        }
        ValueFactory vf = SimpleValueFactory.getInstance();
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), TABLE, true, 0, true, 0, null, null);
        sail.initialize();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                for (int k = 0; k < 10; k++) {
                    sail.addStatement(
                            vf.createIRI("http://whatever/subj" + i),
                            vf.createIRI("http://whatever/pred" + j),
                            vf.createLiteral("whatever\n\"\\" + k)
                    );
                }
            }
        }
        sail.commit();
        sail.shutDown();
    }

    /**
     * Delete the temporary testing folder
     */
    @AfterClass
    public static void teardown() throws Exception {
        FileUtils.deleteDirectory(new File(ROOT));
    }

    @Test(expected = ParseException.class)
    public void testMissingArgs() throws Exception {
        runEndpoint("-p", "8081");
    }

    @Test(expected = ParseException.class)
    public void testUnknownArg() throws Exception {
        runEndpoint("-y");
    }

    @Test(expected = ParseException.class)
    public void testDupArgs() throws Exception {
        runEndpoint("-s", "whatever", "-p", "8081", "-s", "whatever2");
    }

    @Test(expected = HalyardEndpoint.EndpointException.class)
    public void testInvalidTimeout() throws Exception {
        runEndpoint("-s", "whatever", "-p", "8081", "-t", "1234abc");
    }

    @Test(expected = HalyardEndpoint.EndpointException.class)
    public void testInvalidPort() throws Exception {
        runEndpoint("-s", "whatever", "-p", "abc8081");
    }

    /**
     * Missing required option '-s' due to stopping parsing after unrecognized option 'script'
     * (User's custom arguments have to be after all tool options)
     */
    @Test(expected = MissingOptionException.class)
    public void testCustomArgumentsNotLast() throws Exception {
        runEndpoint("script arg1 arg2 arg3", "-s", TABLE);
    }

    /**
     * Cannot execute subprocess (non-existing command "tmp.tmp arg1 arg2 arg3")
     */
    @Test(expected = HalyardEndpoint.EndpointException.class)
    public void testRunMissingScript() throws Exception {
        File tmp = File.createTempFile("tmp", "tmp");
        tmp.delete();
        runEndpoint("-s", TABLE, "-p", "8081", tmp.getPath() + " arg1 arg2 arg3");
    }

    /**
     * Positive test - HalyardEndpoint is run with valid arguments. Checking for positive subprocess output.
     */
    @Test
    public void testSelect() throws Exception {
        File script = new File(this.getClass().getResource("testScript.sh").getPath());
        script.setExecutable(true);
        Path path = Paths.get(ROOT + name.getMethodName());
        runEndpoint("-s", TABLE, "--verbose", script.getPath(), path.toString());
        assertTrue(Files.exists(path));
        assertTrue(Files.lines(path).count() >= 10);
    }

    /**
     * Run HalyardEndpoint tool with provided arguments via Hadoop ToolRunner
     *
     * @param args provided command line arguments
     * @return Exit code of the run method
     */
    private int runEndpoint(String... args) throws Exception {
        return ToolRunner.run(HBaseServerTestInstance.getInstanceConfig(), new HalyardEndpoint(), args);
    }
}
