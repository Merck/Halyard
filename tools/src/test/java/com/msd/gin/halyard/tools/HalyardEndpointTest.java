package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.sail.HBaseSail;
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
 *
 */
public class HalyardEndpointTest {
    private static final String PORT = "0";
    private static final String TABLE = "exporttesttable";
    private static String ROOT;

    @Rule
    public TestName name = new TestName();

    /**
     * Create temporary testing folder for testing files, create Sail repository and add testing data
     *
     * @throws Exception
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

    @AfterClass
    public static void teardown() throws Exception {
        FileUtils.deleteDirectory(new File(ROOT));
    }

    @Test(expected = HalyardEndpoint.EndpointException.class)
    public void testScriptNotFound() throws Exception {
        File tmp = File.createTempFile("tmp", "");
        tmp.delete();
        runEndpoint("-s", TABLE, "-x", tmp.getPath());
    }

    @Test(expected = HalyardEndpoint.EndpointException.class)
    public void testScriptNotExecutable() throws Exception {
        File notExecutable = File.createTempFile("notExecutable", "");
        notExecutable.setExecutable(false);
        runEndpoint("-s", TABLE, "-x", notExecutable.getPath());
    }

    @Test(expected = HalyardEndpoint.EndpointException.class)
    public void testNotWritableOutputDirectory() throws Exception {
        File outputDirectory = new File(ROOT + "outputDirectory");
        outputDirectory.mkdir();
        outputDirectory.setWritable(false);
        String directoryPath = outputDirectory.getPath();
        if (!directoryPath.endsWith("/")) {
            directoryPath = directoryPath + "/";
        }
        File script = File.createTempFile("script", "");
        script.setExecutable(true);
        runEndpoint("-s", TABLE, "-x", script.getPath(), "-o", directoryPath + "output.txt");
    }

    @Test(expected = HalyardEndpoint.EndpointException.class)
    public void testNotExistingOutputDirectory() throws Exception {
        File outputDirectory = new File(ROOT + "outputDirectory");
        outputDirectory.mkdir();
        String directoryPath = outputDirectory.getPath();
        if (!directoryPath.endsWith("/")) {
            directoryPath = directoryPath + "/";
        }
        outputDirectory.delete();
        File script = File.createTempFile("script", "");
        script.setExecutable(true);
        runEndpoint("-s", TABLE, "-x", script.getPath(), "-o", directoryPath + "output.txt");
    }

    @Test
    public void testCorrectScript() throws Exception {
        File script = new File(this.getClass().getResource("testScript.sh").getPath());
        script.setExecutable(true);
        runEndpoint("-p", PORT, "-s", TABLE, "-x", script.getPath(), "-o", ROOT + name.getMethodName(), "--verbose");
        Path path = Paths.get(ROOT + name.getMethodName());
        assertTrue(Files.exists(path));
        assertTrue(Files.lines(path).count() >= 10);
    }

    private int runEndpoint(String... args) throws Exception {
        return ToolRunner.run(HBaseServerTestInstance.getInstanceConfig(), new HalyardEndpoint(), args);
    }
}
