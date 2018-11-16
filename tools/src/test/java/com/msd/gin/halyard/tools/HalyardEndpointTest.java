package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.sail.HBaseSail;
import org.apache.hadoop.util.ToolRunner;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;

/**
 *
 */
public class HalyardEndpointTest {
    private static final String PORT = "0";
    private static final String TABLE = "exporttesttable";

    /**
     * Create temporary testing folder for testing files, create Sail repository and add testing data
     *
     * @throws Exception
     */
    @BeforeClass
    public static void setup() throws Exception {
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

    @Test
    public void testOneScript() throws Exception {
        URL url = this.getClass().getResource("endpoint/testScript.sh");
        File file = new File(url.toURI());
        // Grant permission to the script to be executable
        Files.setPosixFilePermissions(Paths.get(url.getPath()), PosixFilePermissions.fromString("rwxrwxrwx"));

//        System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));

        runEndpoint("-p", PORT, "-s", TABLE, "-x", file.getPath(), "--verbose");
    }

    private static int runEndpoint(String... args) throws Exception {
        HalyardEndpoint endpoint = new HalyardEndpoint();

        return ToolRunner.run(HBaseServerTestInstance.getInstanceConfig(), endpoint, args);
    }
}
