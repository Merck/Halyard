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

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.activation.MimeType;
import javax.activation.MimeTypeParseException;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.common.lang.FileFormat;
import org.eclipse.rdf4j.common.lang.service.FileFormatServiceRegistry;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.impl.SimpleDataset;
import org.eclipse.rdf4j.query.resultio.BooleanQueryResultFormat;
import org.eclipse.rdf4j.query.resultio.BooleanQueryResultWriter;
import org.eclipse.rdf4j.query.resultio.BooleanQueryResultWriterRegistry;
import org.eclipse.rdf4j.query.resultio.QueryResultFormat;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultFormat;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriter;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriterRegistry;
import org.eclipse.rdf4j.repository.sail.SailBooleanQuery;
import org.eclipse.rdf4j.repository.sail.SailGraphQuery;
import org.eclipse.rdf4j.repository.sail.SailQuery;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailTupleQuery;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.RDFWriterRegistry;
import org.eclipse.rdf4j.rio.RioSetting;
import org.eclipse.rdf4j.rio.WriterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles HTTP requests containing SPARQL queries.
 *
 * <p> Supported are only SPARQL queries. SPARQL updates are <b>not</b> supported. One request <b>must</b> include
 * exactly one SPARQL query string. Supported are only HTTP GET and HTTP POST methods. For each detailed HTTP method
 * specification see <a href="https://www.w3.org/TR/sparql11-protocol/#protocol">SPARQL Protocol Operations</a></p>
 *
 * @author sykorjan
 */
public final class HttpSparqlHandler implements HttpHandler {
    private static final String AND_DELIMITER = "&";
    private static final String CHARSET = StandardCharsets.UTF_8.name();
    private static final ValueFactory SVF = SimpleValueFactory.getInstance();
    private static final Pattern UNRESOLVED_PARAMETERS = Pattern.compile("\\{\\{(\\w+)\\}\\}");


    // Query parameter prefixes
    private static final String QUERY_PREFIX = "query=";
    private static final String DEFAULT_GRAPH_PREFIX = "default-graph-uri=";
    private static final String NAMED_GRAPH_PREFIX = "named-graph-uri=";

    // Request content type (only for POST requests)
    private static final String ENCODED_CONTENT = "application/x-www-form-urlencoded";
    private static final String UNENCODED_CONTENT = "application/sparql-query";


    // Service successfully executes the query
    private static final int HTTP_OK_STATUS = 200;
    // Client does not provide a correct request, including incorrect request parameter content (e.g. illegal request
    // parameters or illegal sequence of query characters defined by the SPARQL query grammar)
    private static final int HTTP_BAD_REQUEST = 400;
    // Client provides a correct request but service fails to process it (e.g. server failed to send the response)
    private static final int HTTP_INTERNAL_SERVER_ERROR = 500;

    // Sail repository
    private final SailRepository repository;
    // Stored SPARQL queries
    private final Properties storedQueries;
    // Writer config
    private final WriterConfig writerConfig;
    private final boolean verbose;
    // Logger
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSparqlHandler.class);

    /**
     * @param rep              Sail repository
     * @param storedQueries    pre-defined stored SPARQL query templates
     * @param writerProperties RDF4J RIO WriterConfig properties
     * @param verbose          true when verbose mode enabled, otherwise false
     */
    @SuppressWarnings("unchecked")
    public HttpSparqlHandler(SailRepository rep, Properties storedQueries, Properties writerProperties, boolean verbose) {
        this.repository = rep;
        // Verbose mode disabled --> logs with level lower than WARNING will be discarded
        this.verbose = verbose;
        this.storedQueries = storedQueries;
        this.writerConfig = new WriterConfig();
        if (writerProperties != null) {
            for (Map.Entry<Object, Object> me : writerProperties.entrySet()) {
                writerConfig.set((RioSetting) getStaticField(me.getKey().toString()), getStaticField(me.getValue().toString()));
            }
        }
    }

    /**
     * retrieves public static field using reflection
     *
     * @param fqn fully qualified class.field name
     * @return content of the field
     * @throws IllegalArgumentException in case of any reflection errors, class not found, field not found, illegal access...
     */
    private static Object getStaticField(String fqn) {
        try {
            int i = fqn.lastIndexOf('.');
            if (i < 1) throw new IllegalArgumentException("Invalid fully qualified name of a config field: " + fqn);
            return Class.forName(fqn.substring(0, i)).getField(fqn.substring(i + 1)).get(null);
        } catch (Exception e) {
            throw new IllegalArgumentException("Exception while looking for public static field: " + fqn, e);
        }
    }

    /**
     * Handle HTTP requests containing SPARQL queries.
     *
     * <p>First, SPARQL query is retrieved from the HTTP request. Then the query is evaluated towards a
     * specified Sail repository. Finally, the query result is sent back as an HTTP response.</p>
     *
     * @param exchange HTTP exchange
     * @throws IOException If an error occurs during sending the error response
     */
    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            SparqlQuery sparqlQuery = retrieveQuery(exchange);
        	try(SailRepositoryConnection connection = repository.getConnection()) {
        		evaluateQuery(connection, sparqlQuery, exchange);
        	}
        } catch (IllegalArgumentException | RDF4JException e) {
            StringWriter sw = new StringWriter();
            PrintWriter w = new PrintWriter(sw);
            e.printStackTrace(w);
            LOGGER.warn(sw.toString());
            sendResponse(exchange, HTTP_BAD_REQUEST, sw.toString());
        } catch (IOException | RuntimeException e) {
            StringWriter sw = new StringWriter();
            PrintWriter w = new PrintWriter(sw);
            e.printStackTrace(w);
            LOGGER.warn(sw.toString());
            sendResponse(exchange, HTTP_INTERNAL_SERVER_ERROR, sw.toString());
        }

    }

    /**
     * Retrieve SPARQL query from the HTTP request.
     *
     * @param exchange HTTP request
     * @return SPARQL query
     * @throws IOException              If an error occurs during reading the request
     * @throws IllegalArgumentException If the request does not follow the SPARQL Protocol Operation specification
     */
    private SparqlQuery retrieveQuery(HttpExchange exchange) throws IOException, IllegalArgumentException {
        String requestMethod = exchange.getRequestMethod();
        SparqlQuery sparqlQuery = new SparqlQuery();
        // Help variable for checking for multiple query parameters
        int queryCount = 0;
        String path = exchange.getRequestURI().getPath();
        // retrieve query from stored queries based on non-root request URL path
        if (path != null && path.length() > 1) {
            String query = storedQueries.getProperty(path.substring(1));
            if (query == null) {
                //try to cut the extension
                int i = path.lastIndexOf('.');
                if (i > 0) {
                    query = storedQueries.getProperty(path.substring(1, i));
                }
            }
            if (query == null) {
                throw new IllegalArgumentException("No stored query for path: " + path);
            }
            sparqlQuery.setQuery(query);
            queryCount++;
        }
        if ("GET".equalsIgnoreCase(requestMethod)) {
            // Retrieve from the request URI parameter query and optional parameters defaultGraphs and namedGraphs
            // Cannot apply directly exchange.getRequestURI().getQuery() since getQuery() method
            // automatically decodes query (requestQuery must remain unencoded due to parsing by '&' delimiter)
            String requestQueryRaw = exchange.getRequestURI().getRawQuery();
            if (requestQueryRaw != null) {
                StringTokenizer stk = new StringTokenizer(requestQueryRaw, AND_DELIMITER);
                while (stk.hasMoreTokens()) {
                    queryCount += parseParameter(stk.nextToken(), sparqlQuery);
                }
            }
            try (InputStream requestBody = exchange.getRequestBody()) {
                if (requestBody.available() > 0) {
                    throw new IllegalArgumentException("Request via GET must not include a message body");
                }
            }
        } else if ("POST".equalsIgnoreCase(requestMethod)) {
            Headers headers = exchange.getRequestHeaders();

            // Check for presence of the Content-Type header
            if (!headers.containsKey("Content-Type")) {
                throw new IllegalArgumentException("POST request has to contain header \"Content-Type\"");
            }

            // Should not happen but better be safe than sorry
            if (headers.get("Content-Type").size() != 1) {
                throw new IllegalArgumentException("POST request has to contain header \"Content-Type\" exactly once");
            }

            // Check Content-Type header content
            try {
                MimeType mimeType = new MimeType(headers.getFirst("Content-Type"));
                String baseType = mimeType.getBaseType();
                String charset = mimeType.getParameter("charset");
                if (charset != null && !charset.equalsIgnoreCase(CHARSET)) {
                    throw new IllegalArgumentException("Illegal Content-Type charset. Only UTF-8 is supported");
                }

                // Request message body is processed based on the value of Content-Type property
                if (baseType.equals(ENCODED_CONTENT)) {
                    // Retrieve from the message body parameter query and optional parameters defaultGraphs and
                    // namedGraphs
                    try (InputStream requestBody = exchange.getRequestBody()) {
                        Scanner requestBodyScanner = new Scanner(requestBody, CHARSET).useDelimiter(AND_DELIMITER);
                        while (requestBodyScanner.hasNext()) {
                            queryCount += parseParameter(requestBodyScanner.next(), sparqlQuery);
                        }
                    }
                } else if (baseType.equals(UNENCODED_CONTENT)) {
                    // Retrieve from the request URI optional parameters defaultGraphs and namedGraphs
                    // Cannot apply directly exchange.getRequestURI().getQuery() since getQuery() method
                    // automatically decodes query (requestQuery must remain unencoded due to parsing by '&' delimiter)
                    String requestQueryRaw = exchange.getRequestURI().getRawQuery();
                    if (requestQueryRaw != null) {
                        StringTokenizer stk = new StringTokenizer(requestQueryRaw, AND_DELIMITER);
                        while (stk.hasMoreTokens()) {
                            queryCount += parseParameter(stk.nextToken(), sparqlQuery);
                        }
                    }

                    // Retrieve from the message body parameter query
                    try (InputStream requestBody = exchange.getRequestBody()) {
                        sparqlQuery.setQuery(new Scanner(requestBody, CHARSET).useDelimiter("\\A").next());
                    }
                } else {
                    throw new IllegalArgumentException("Content-Type of POST request has to be either " + ENCODED_CONTENT
                            + " or " + UNENCODED_CONTENT);
                }
            } catch (MimeTypeParseException e) {
                throw new IllegalArgumentException("Illegal Content-Type header content");
            }
        } else {
            throw new IllegalArgumentException("Request method has to be only either GET or POST");
        }

        String query = sparqlQuery.getQuery();
        if (query == null || query.length() <= 0) {
            throw new IllegalArgumentException("Missing parameter query");
        }

        Matcher m = UNRESOLVED_PARAMETERS.matcher(query);
        if (m.find()) {
            throw new IllegalArgumentException("Missing query parameter: " + m.group(1));
        }

        if (queryCount > 1) {
            throw new IllegalArgumentException("Cannot invoke query operation with more than one query string");
        }

        return sparqlQuery;
    }

    /**
     * Parse single parameter from HTTP request parameters or body.
     *
     * @param param       single raw String parameter
     * @param sparqlQuery SparqlQuery to fill from the parsed parameter
     * @throws UnsupportedEncodingException which never happens
     * @returns number of found SPARQL queries
     */
    private static int parseParameter(String param, SparqlQuery sparqlQuery) throws UnsupportedEncodingException {
        int queryCount = 0;
        if (param.startsWith(QUERY_PREFIX)) {
            queryCount++;
            sparqlQuery.setQuery(URLDecoder.decode(param.substring(QUERY_PREFIX.length()),
                    CHARSET));
        } else if (param.startsWith(DEFAULT_GRAPH_PREFIX)) {
            sparqlQuery.addDefaultGraph(SVF.createIRI(
                    URLDecoder.decode(param.substring(DEFAULT_GRAPH_PREFIX.length()), CHARSET)));
        } else if (param.startsWith(NAMED_GRAPH_PREFIX)) {
            sparqlQuery.addNamedGraph(SVF.createIRI(
                    URLDecoder.decode(param.substring(NAMED_GRAPH_PREFIX.length()), CHARSET)));
        } else {
            int i = param.indexOf("=");
            if (i >= 0) {
                sparqlQuery.addParameter(URLDecoder.decode(param.substring(0, i), CHARSET), URLDecoder.decode(param.substring(i + 1), CHARSET));
            } else {
                throw new IllegalArgumentException("Invalid request parameter: " + param);
            }
        }
        return queryCount;
    }

    /**
     * Evaluate query towards a Sail repository and send response (the query result) back to client
     *
     * @param sparqlQuery query to be evaluated
     * @param exchange    HTTP exchange for sending the response
     * @throws IOException    If an error occurs during sending response to the client
     * @throws RDF4JException If an error occurs due to illegal SPARQL query (e.g. incorrect syntax)
     */
    private void evaluateQuery(SailRepositoryConnection connection, SparqlQuery sparqlQuery, HttpExchange exchange) throws IOException, RDF4JException {
        SailQuery query = connection.prepareQuery(QueryLanguage.SPARQL, sparqlQuery.getQuery(), null);
        Dataset dataset = sparqlQuery.getDataset();
        if (!dataset.getDefaultGraphs().isEmpty() || !dataset.getNamedGraphs().isEmpty()) {
            // This will include default graphs and named graphs from  the request parameters but default graphs and
            // named graphs contained in the string query will be ignored
            query.getParsedQuery().setDataset(dataset);
        }
        List<String> acceptedMimeTypes = new ArrayList<>();
        List<String> acceptHeaders = exchange.getRequestHeaders().get("Accept");
        if (acceptHeaders != null) for (String header : acceptHeaders) {
            acceptedMimeTypes.addAll(parseAcceptHeader(header));
        }
        if (query instanceof SailTupleQuery) {
        	if (verbose) {
        		LOGGER.info("Evaluating tuple query: {}", sparqlQuery.getQuery());
        	}
            QueryResultFormat format = getFormat(TupleQueryResultWriterRegistry.getInstance(), exchange.getRequestURI().getPath(),
                    acceptedMimeTypes, TupleQueryResultFormat.CSV, exchange.getResponseHeaders());
            exchange.sendResponseHeaders(HTTP_OK_STATUS, 0);
            try (BufferedOutputStream response = new BufferedOutputStream(exchange.getResponseBody())) {
                TupleQueryResultWriter w = TupleQueryResultWriterRegistry.getInstance().get(format).get().getWriter(response);
                w.setWriterConfig(writerConfig);
                ((TupleQuery) query).evaluate(w);
            }
        } else if (query instanceof SailGraphQuery) {
        	if (verbose) {
        		LOGGER.info("Evaluating graph query: {}", sparqlQuery.getQuery());
        	}
            RDFFormat format = getFormat(RDFWriterRegistry.getInstance(), exchange.getRequestURI().getPath(),
                    acceptedMimeTypes, RDFFormat.NTRIPLES, exchange.getResponseHeaders());
            exchange.sendResponseHeaders(HTTP_OK_STATUS, 0);
            try (BufferedOutputStream response = new BufferedOutputStream(exchange.getResponseBody())) {
                RDFWriter w = RDFWriterRegistry.getInstance().get(format).get().getWriter(response);
                w.setWriterConfig(writerConfig);
                ((GraphQuery) query).evaluate(w);
            }
        } else if (query instanceof SailBooleanQuery) {
        	if (verbose) {
        		LOGGER.info("Evaluating boolean query: {}", sparqlQuery.getQuery());
        	}
            QueryResultFormat format = getFormat(BooleanQueryResultWriterRegistry.getInstance(), exchange.getRequestURI().getPath(),
                    acceptedMimeTypes, BooleanQueryResultFormat.JSON, exchange.getResponseHeaders());
            exchange.sendResponseHeaders(HTTP_OK_STATUS, 0);
            try (BufferedOutputStream response = new BufferedOutputStream(exchange.getResponseBody())) {
                BooleanQueryResultWriter w = BooleanQueryResultWriterRegistry.getInstance().get(format).get().getWriter(response);
                w.setWriterConfig(writerConfig);
                w.handleBoolean(((BooleanQuery) query).evaluate());
            }
        }
        if (verbose) {
        	LOGGER.info("Request successfully processed");
        }
    }

    /**
     * Send response of the processed request to the client
     *
     * @param exchange HttpExchange wrapper encapsulating response
     * @param code     HTTP code
     * @param message  Content of the response
     * @throws IOException
     */
    private void sendResponse(HttpExchange exchange, int code, String message) throws IOException {
        byte[] payload = message.getBytes(CHARSET);
        exchange.sendResponseHeaders(code, payload.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(payload);
        }
    }

    /**
     * Get file format of the response and set its MIME type as a response content-type header value.
     * <p>
     * If no accepted MIME types are specified, a default file format for the default MIME type is chosen.
     *
     * @param reg           requested result writer registry
     * @param path          requested file path
     * @param mimeTypes     requested/accepted response MIME types (e.g. application/sparql-results+xml,
     *                      application/xml)
     * @param defaultFormat default file format (e.g. SPARQL/XML)
     * @param h             response headers for setting response content-type value
     * @param <FF>          file format
     * @param <S>
     * @return
     */
    private <FF extends FileFormat, S extends Object> FF getFormat(FileFormatServiceRegistry<FF, S> reg,
                                                                   String path, List<String> mimeTypes,
                                                                   FF defaultFormat, Headers h) {
        if (path != null) {
            Optional<FF> o = reg.getFileFormatForFileName(path);
            if (o.isPresent()) {
                Charset chs = o.get().getCharset();
                h.set("Content-Type", o.get().getDefaultMIMEType() + (chs == null ? "" : ("; charset=" + chs.name())));
                return o.get();
            }
        }
        if (mimeTypes != null) {
            for (String mimeType : mimeTypes) {
                Optional<FF> o = reg.getFileFormatForMIMEType(mimeType);
                if (o.isPresent()) {
                    Charset chs = o.get().getCharset();
                    h.set("Content-Type", mimeType + (chs == null ? "" : ("; charset=" + chs.name())));
                    return o.get();
                }
            }
        }
        h.set("Content-Type", defaultFormat.getDefaultMIMEType());
        return defaultFormat;
    }

    /**
     * Parse Accept header in case it contains multiple mime types. Relative quality factors ('q') are removed.
     * <p>
     * Example:
     * Accept header: "text/html, application/xthml+xml, application/xml;q=0.9, image/webp;q=0.1"
     * ---> parse to List<String>:
     * [0] "text/html"
     * [1] "application/xthml+xml"
     * [2] "application/xml"
     * [3] "image/webp"
     *
     * @param acceptHeader
     * @return parsed Accept header
     */
    private List<String> parseAcceptHeader(String acceptHeader) {
        List<String> mimeTypes = Arrays.asList(acceptHeader.trim().split(","));
        for (int i = 0; i < mimeTypes.size(); i++) {
            mimeTypes.set(i, mimeTypes.get(i).trim().split(";", 0)[0]);
        }
        return mimeTypes;
    }

    /**
     * Help class for retrieving the whole SPARQL query, including optional parameters defaultGraphs and namedGraphs,
     * from the HTTP request.
     */
    private static class SparqlQuery {
        // SPARQL query string, has to be exactly one
        private String query;
        // SPARQL query template parameters for substitution
        private final List<String> parameterNames, parameterValues;
        // Dataset containing default graphs and named graphs
        private final SimpleDataset dataset;

        public SparqlQuery() {
            query = null;
            parameterNames = new ArrayList<>();
            parameterValues = new ArrayList<>();
            dataset = new SimpleDataset();
        }

        public String getQuery() {
            //replace all tokens matchig {{parameterName}} inside the SPARQL query with corresponding parameterValues
            return StringUtils.replaceEach(query, parameterNames.toArray(new String[parameterNames.size()]), parameterValues.toArray(new String[parameterValues.size()]));
        }

        public void setQuery(String query) {
            this.query = query;
        }

        public void addDefaultGraph(IRI defaultGraph) {
            dataset.addDefaultGraph(defaultGraph);
        }

        public void addNamedGraph(IRI namedGraph) {
            dataset.addNamedGraph(namedGraph);
        }

        public void addParameter(String name, String value) {
            parameterNames.add("{{" + name + "}}");
            parameterValues.add(value);
        }

        public Dataset getDataset() {
            return dataset;
        }

    }

}
