package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.endpoint.HttpSparqlHandler;
import com.msd.gin.halyard.endpoint.SimpleHttpServer;
import com.msd.gin.halyard.sail.HBaseSail;
import org.apache.hadoop.util.ToolRunner;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.net.URL;

public class HalyardEndpointTest {
    private static final String PORT = "8080";
    private static final String TABLE = "exporttesttable";
    private static final String TRIPLE = "<http://a> <http://b> <http://c> .";

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
        URL url = this.getClass().getResource("testScript.sh");
        File file = new File(url.toURI());
        runEndpoint("-p", PORT, "-s", TABLE, "-x", file.getPath());
    }

//    @Test
//    public void testFoo() {
//        try {
//            URL url = this.getClass().getResource("testScript.sh");
//            File file = new File(url.toURI());
//            SailRepository rep = new SailRepository(new MemoryStore());
//            rep.initialize();
//            SailRepositoryConnection connection = rep.getConnection();
//            connection.begin();
//            connection.add(new StringReader(TRIPLE), "http://whatever/", RDFFormat.NTRIPLES);
//            HttpSparqlHandler handler = new HttpSparqlHandler(connection);
//
//            Integer port = Integer.parseInt(PORT);
//            if (port == null) {
//                port = 0; // system will automatically assign a new, free port
//            }
//
//            SimpleHttpServer server = new SimpleHttpServer(port, "/", handler);
//            server.start();
//
//            ProcessBuilder pb = new ProcessBuilder(file.getPath()).inheritIO();
//            pb.environment().put("ENDPOINT", "http://localhost" + ":" + server.getAddress().getPort() + "/");
//            System.exit(pb.start().waitFor());
//            server.stop();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    private static int runEndpoint(String... args) throws Exception {
        return ToolRunner.run(HBaseServerTestInstance.getInstanceConfig(), new HalyardEndpoint(), args);
    }
}
