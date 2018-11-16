package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.endpoint.HttpSparqlHandler;
import com.msd.gin.halyard.endpoint.SimpleHttpServer;
import com.msd.gin.halyard.sail.HBaseSail;
import org.apache.commons.cli.CommandLine;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;

import java.io.IOException;

public final class HalyardEndpoint extends AbstractHalyardTool {
    private static final String CONTEXT = "/";
    private static final String HOSTNAME = "http://localhost";
    /**
     * Default port number
     * When port number is zero, system will automatically assign a new free port
     */
    private static final int DEFAULT_PORT = 0;

    /**
     * Default evaluation timeout
     * When timeout is zero, then the evaluation timeout is unlimited
     */
    private static final int DEFAULT_TIMEOUT = 0;

    private String script;

    public HalyardEndpoint() {
        super(
                "endpoint",
                "Halyard Endpoint is a command-line application designed to launch a simple SPARQL " +
                        "Endpoint to serve SPARQL Queries. If no port is specified, system will automatically " +
                        "select a new port number",
                "Example: halyard endpoint -p 8000 -s TABLE -x /tmp/script.sh --verbose"
        );
        addOption(
                "p", "port", "http_server_port", "HTTP server port number", false, true);
        addOption("s", "source-dataset", "dataset_table", "Source HBase table with Halyard RDF store", true, true);
        addOption("x", "executable-script", "executable_script", "Executable script to be run on the server", true,
                true);
        addOption("i", "elastic-index", "elastic_index_url", "Optional ElasticSearch index URL", false, true);
        addOption("t", "timeout", "evaluation_timeout", "Timeout in seconds for each query evaluation (default is " +
                "unlimited timeout)", false, true);
        // cannot use short option 'v' due to conflict with the super "--version" option
        addOption(null, "verbose", null, "Logging mode that records all logging information (by default only " +
                "important informative and error messages are printed)", false, false);
    }

    @Override
    protected int run(CommandLine cmd) throws EndpointException {
        try {
            int timeout = parseTimeout(cmd);
            int port = parsePort(cmd);
            String table = cmd.getOptionValue('s');
            String elasticIndexURL = cmd.getOptionValue('i');
            String script = cmd.getOptionValue('x');
            boolean verbose = false;
            if (cmd.hasOption("verbose")) {
                verbose = true;
            }
            SailRepository rep = new SailRepository(
                    new HBaseSail(getConf(), table, false, 0, true, timeout, elasticIndexURL, null));
            rep.initialize();
            try {
                SailRepositoryConnection connection = rep.getConnection();
                connection.begin();
                HttpSparqlHandler handler = new HttpSparqlHandler(connection, verbose);
                SimpleHttpServer server = new SimpleHttpServer(port, CONTEXT, handler);
                server.start();
                try {
                    ProcessBuilder pb = new ProcessBuilder(script).inheritIO();
                    pb.environment().put("ENDPOINT", HOSTNAME + ":" + server.getAddress().getPort() + "/");
                    int returnCode = pb.start().waitFor();
                    return returnCode;
                } finally {
                    server.stop();
                }
            } finally {
                rep.shutDown();
            }
        } catch (IOException | InterruptedException  e) {
            throw new EndpointException(e);
        }
    }

    private int parseTimeout(CommandLine cmd) throws EndpointException {
        String timeoutString = cmd.getOptionValue('t');
        if (timeoutString == null) {
            return DEFAULT_TIMEOUT; // When no timeout specified, return default unlimited timeout (zero)
        } else {
            try {
                return Integer.parseInt(timeoutString);
            } catch (NumberFormatException e) {
                throw new EndpointException("Failed to parse timeout number from the input string: " + timeoutString);
            }
        }
    }

    private int parsePort(CommandLine cmd) throws EndpointException {
        String portString = cmd.getOptionValue('p');
        if (portString == null) {
            return DEFAULT_PORT;
        } else {
            try {
                return Integer.parseInt(portString);
            } catch (NumberFormatException e) {
                throw new EndpointException("Failed to parse port number from the input string: " + portString);
            }
        }
    }

    /**
     * A generic exception during using Halyard endpoint
     */
    public static final class EndpointException extends Exception {
        private static final long serialVersionUID = -8594289635052879832L;

        public EndpointException(String message) {
            super(message);
        }

        public EndpointException(Throwable cause) {
            super(cause);
        }
    }
}
