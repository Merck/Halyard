package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.sail.HBaseSail;
import org.apache.commons.cli.CommandLine;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;

import java.io.*;
import java.util.Arrays;
import java.util.List;

/**
 * @author sykorjan
 */
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

    public HalyardEndpoint() {
        super(
                "endpoint",
                "Halyard Endpoint is a command-line application designed to launch a simple SPARQL Endpoint to serve " +
                        "SPARQL Queries. Any custom commands that are to be run as a new subprocess internally by " +
                        "this tool have to be passed at the end of this tool.\n" +
                        "Endpoint URI is accessible via the system environment variable ENDPOINT.\n" +
                        "Warning: All options following a first unrecognized option are ignored and are considered" +
                        " as part of the custom command.",
                "Example: halyard endpoint -p 8000 -s TABLE --verbose customScript.sh customArg1 customArg2"
        );
        // allow passing unspecified additional parameters for the executable script
        cmdMoreArgs = true;
        addOption(
                "p", "port", "http_server_port", "HTTP server port number. If no port number is specified, system " +
                        "will automatically select a new port number", false, true);
        addOption("s", "source-dataset", "dataset_table", "Source HBase table with Halyard RDF store", true, true);
        addOption("i", "elastic-index", "elastic_index_url", "Optional ElasticSearch index URL", false, true);
        addOption("t", "timeout", "evaluation_timeout", "Timeout in seconds for each query evaluation (default is " +
                "unlimited timeout)", false, true);
        // cannot use short option 'v' due to conflict with the super "--version" option
        addOption(null, "verbose", null, "Logging mode that records all logging information (by default only " +
                "important informative and error messages are printed)", false, false);
    }

    /**
     * Parse options and user's custom command, create SPARQL endpoint connected to a specified Sail repository and run
     * user's custom command as a new subprocess.
     *
     * @param cmd list of halyard endpoint tool arguments
     * @return exit value of the created subprocess
     * @throws EndpointException if any error occurs during parsing, setting up server/endpoint or running the
     *                           subprocess
     */
    @Override
    protected int run(CommandLine cmd) throws EndpointException {
        try {
            int timeout = parseTimeout(cmd);
            int port = parsePort(cmd);
            String table = cmd.getOptionValue('s');
            String elasticIndexURL = cmd.getOptionValue('i');

            boolean verbose = false;
            if (cmd.hasOption("verbose")) {
                verbose = true;
            }

            // Any left-over non-recognized options and arguments are considered as part of user's custom commands
            // that are to be run by this tool
            List<String> cmdArgs = Arrays.asList(cmd.getArgs());

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
                    ProcessBuilder pb = new ProcessBuilder(cmdArgs).inheritIO();
                    pb.environment().put("ENDPOINT", HOSTNAME + ":" + server.getAddress().getPort() + "/");
                    return pb.start().waitFor();
                } finally {
                    server.stop();
                }
            } finally {
                rep.shutDown();
            }
        } catch (IOException | InterruptedException e) {
            throw new EndpointException(e);
        }
    }

    /**
     * Parse timeout number
     *
     * @param cmd
     * @return
     * @throws EndpointException Provided timeout value is not a number
     */
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

    /**
     * Parse port number
     *
     * @param cmd
     * @return
     * @throws EndpointException Provided port value is not a number
     */
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
