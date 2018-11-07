package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.endpoint.HttpSparqlHandler;
import com.msd.gin.halyard.endpoint.SimpleHttpServer;
import com.msd.gin.halyard.sail.HBaseSail;
import org.apache.commons.cli.CommandLine;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;

public final class HalyardEndpoint extends AbstractHalyardTool {
    private static final String CONTEXT = "/";
    private static final String HOSTNAME = "http://localhost";

    public HalyardEndpoint() {
        super(
                "endpoint",
                "Halyard Endpoint is a command-line application designed to launch a simple SPARQL " +
                        "Endpoint to serve SPARQL Queries. If no port is specified, system will automatically " +
                        "select a new port number",
                "Example: halyard endpoint -p 8000 -s TABLE -x script.sh"
        );
        addOption(
                "p", "port", "http_server_port", "HTTP server port number", false, true);
        addOption("s", "source-dataset", "dataset_table", "Source HBase table with Halyard RDF store", true, true);
        addOption("x", "executable-script", "executable_script", "Executable script to be run on the server", true,
                true);
        addOption("i", "elastic-index", "elastic_index_url", "Optional ElasticSearch index URL", false, true);
        addOption("t", "timeout", "evaluation_timeout", "Timeout in seconds for each query evaluation (default is " +
                "unlimited timeout)", false, true);
    }

    @Override
    protected int run(CommandLine cmd) throws Exception {
        int timeout;
        String timeoutString = cmd.getOptionValue('t');
        if(timeoutString == null) {
            timeout = 0; // Default unlimited timeout
        } else {
            try {
                timeout = Integer.parseInt(timeoutString);
            } catch (NumberFormatException e) {
                throw new EndpointException("Failed to parse timeout number from the input string: " + timeoutString);
            }
        }

        int port;
        String portString = cmd.getOptionValue('p');
        if (portString == null) {
            port = 0; // System will automatically assign a new free port
        } else {
            try {
                port = Integer.parseInt(portString);
            } catch (NumberFormatException e) {
                throw new EndpointException("Failed to parse port number from the input string: " + portString);
            }
        }

        SailRepository rep = new SailRepository(new HBaseSail(getConf(), cmd.getOptionValue('s'), false, 0, true, timeout,
                cmd.getOptionValue('i'), null));
        rep.initialize();
        SailRepositoryConnection connection = rep.getConnection();
        connection.begin();
        HttpSparqlHandler handler = new HttpSparqlHandler(connection);


        SimpleHttpServer server = new SimpleHttpServer(port, CONTEXT, handler);
        server.start();
        ProcessBuilder pb = new ProcessBuilder(cmd.getOptionValue('x'), "-i").inheritIO();
        pb.environment().put("ENDPOINT", HOSTNAME + ":" + server.getAddress().getPort() + "/");
        pb.start().waitFor();
        server.stop();
        rep.shutDown();
        return 0;
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
