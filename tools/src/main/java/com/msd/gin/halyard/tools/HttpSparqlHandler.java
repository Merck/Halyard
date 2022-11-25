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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.msd.gin.halyard.sail.ResultTrackingSailConnection;
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
import java.net.HttpURLConnection;
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

import org.apache.commons.io.output.StringBuilderWriter;
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
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.algebra.Modify;
import org.eclipse.rdf4j.query.algebra.UpdateExpr;
import org.eclipse.rdf4j.query.impl.SimpleDataset;
import org.eclipse.rdf4j.query.parser.ParsedBooleanQuery;
import org.eclipse.rdf4j.query.parser.ParsedGraphQuery;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.ParsedUpdate;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.query.resultio.BooleanQueryResultFormat;
import org.eclipse.rdf4j.query.resultio.BooleanQueryResultWriter;
import org.eclipse.rdf4j.query.resultio.BooleanQueryResultWriterFactory;
import org.eclipse.rdf4j.query.resultio.BooleanQueryResultWriterRegistry;
import org.eclipse.rdf4j.query.resultio.QueryResultFormat;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultFormat;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriter;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriterFactory;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriterRegistry;
import org.eclipse.rdf4j.repository.sail.SailQuery;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailUpdate;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.RDFWriterFactory;
import org.eclipse.rdf4j.rio.RDFWriterRegistry;
import org.eclipse.rdf4j.rio.RioSetting;
import org.eclipse.rdf4j.rio.WriterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles HTTP requests containing SPARQL queries.
 *
 * <p> Supported are only SPARQL queries. One request <b>must</b> include
 * exactly one SPARQL query string. Supported are only HTTP GET and HTTP POST methods. For each detailed HTTP method
 * specification see <a href="https://www.w3.org/TR/sparql11-protocol/#protocol">SPARQL Protocol Operations</a></p>
 *
 * @author sykorjan
 */
public final class HttpSparqlHandler implements HttpHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSparqlHandler.class);

    private static final String AND_DELIMITER = "&";
    private static final String CHARSET = StandardCharsets.UTF_8.name();
    private static final ValueFactory SVF = SimpleValueFactory.getInstance();
    private static final Pattern UNRESOLVED_PARAMETERS = Pattern.compile("\\{\\{(\\w+)\\}\\}");


    // Query parameter prefixes
    private static final String QUERY_PREFIX = "query=";
    private static final String DEFAULT_GRAPH_PREFIX = "default-graph-uri=";
    private static final String NAMED_GRAPH_PREFIX = "named-graph-uri=";
    private static final String UPDATE_PREFIX = "update=";
    private static final String USING_GRAPH_URI_PREFIX = "using-graph-uri=";
    private static final String USING_NAMED_GRAPH_PREFIX = "using-named-graph-uri=";

    private static final String TRACK_RESULT_SIZE = "track-result-size=";

    // Request content type (only for POST requests)
    static final String ENCODED_CONTENT = "application/x-www-form-urlencoded";
    static final String UNENCODED_QUERY_CONTENT = "application/sparql-query";
    static final String UNENCODED_UPDATE_CONTENT = "application/sparql-update";


    private final SailRepository repository;
    private final Properties storedQueries;
    private final WriterConfig writerConfig;
    private final Runnable stopAction;

    /**
     * @param rep              Sail repository
     * @param storedQueries    pre-defined stored SPARQL query templates
     * @param writerProperties RDF4J RIO WriterConfig properties
     */
    @SuppressWarnings("unchecked")
    public HttpSparqlHandler(SailRepository rep, Properties storedQueries, Properties writerProperties, Runnable stopAction) {
        this.repository = rep;
        this.storedQueries = storedQueries;
        this.writerConfig = new WriterConfig();
        if (writerProperties != null) {
            for (Map.Entry<Object, Object> me : writerProperties.entrySet()) {
                writerConfig.set((RioSetting) getStaticField(me.getKey().toString()), getStaticField(me.getValue().toString()));
            }
        }
        this.stopAction = stopAction;
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
        String path = exchange.getRequestURI().getPath();
        boolean doStop = false;
        try {
        	if ("/_health".equals(path)) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_NO_CONTENT, -1);
        	} else if ("/_stop".equals(path) && exchange.getLocalAddress().getAddress().isLoopbackAddress()) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_NO_CONTENT, -1);
                doStop = true;
        	} else {
	            SparqlQuery sparqlQuery = retrieveQuery(exchange);
        		if (sparqlQuery.getQuery() != null) {
        			evaluateQuery(sparqlQuery, exchange);
        		} else if (sparqlQuery.getUpdate() != null) {
        			evaluateUpdate(sparqlQuery, exchange);
        		}
        	}
        } catch (IllegalArgumentException | RDF4JException e) {
            LOGGER.debug("Bad request", e);
            sendErrorResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, e);
        } catch (IOException | RuntimeException e) {
            LOGGER.warn("Internal error", e);
            sendErrorResponse(exchange, HttpURLConnection.HTTP_INTERNAL_ERROR, e);
        }
        exchange.close();
        if (doStop) {
        	stopAction.run();
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
        }
        if ("GET".equalsIgnoreCase(requestMethod)) {
            // Retrieve from the request URI parameter query and optional parameters defaultGraphs and namedGraphs
            // Cannot apply directly exchange.getRequestURI().getQuery() since getQuery() method
            // automatically decodes query (requestQuery must remain unencoded due to parsing by '&' delimiter)
            String requestQueryRaw = exchange.getRequestURI().getRawQuery();
            if (requestQueryRaw != null) {
                StringTokenizer stk = new StringTokenizer(requestQueryRaw, AND_DELIMITER);
                while (stk.hasMoreTokens()) {
                    parseQueryParameter(stk.nextToken(), sparqlQuery);
                }
            }

            String query = sparqlQuery.getQuery();
            if (query == null || query.isEmpty()) {
                throw new IllegalArgumentException("Missing parameter: query");
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
                    try (Scanner requestBodyScanner = new Scanner(exchange.getRequestBody(), CHARSET).useDelimiter(AND_DELIMITER)) {
                        while (requestBodyScanner.hasNext()) {
                            String keyValue = requestBodyScanner.next();
                            parseQueryParameter(keyValue, sparqlQuery);
                            parseUpdateParameter(keyValue, sparqlQuery);
                        }
                    }

                    String query = sparqlQuery.getQuery();
                    String update = sparqlQuery.getUpdate();
                    if ((query == null || query.isEmpty()) && (update == null || update.isEmpty())) {
                        throw new IllegalArgumentException("Missing parameter: query/update");
                    }
                } else if (baseType.equals(UNENCODED_QUERY_CONTENT)) {
                    // Retrieve from the message body parameter query
                    try (Scanner requestBodyScanner = new Scanner(exchange.getRequestBody(), CHARSET).useDelimiter("\\A")) {
                        sparqlQuery.setQuery(requestBodyScanner.next());
                    }

                    // Retrieve from the request URI optional parameters defaultGraphs and namedGraphs
                    // Cannot apply directly exchange.getRequestURI().getQuery() since getQuery() method
                    // automatically decodes query (requestQuery must remain unencoded due to parsing by '&' delimiter)
                    String requestQueryRaw = exchange.getRequestURI().getRawQuery();
                    if (requestQueryRaw != null) {
                        StringTokenizer stk = new StringTokenizer(requestQueryRaw, AND_DELIMITER);
                        while (stk.hasMoreTokens()) {
                            parseQueryParameter(stk.nextToken(), sparqlQuery);
                        }
                    }
                } else if (baseType.equals(UNENCODED_UPDATE_CONTENT)) {
                    // Retrieve from the message body parameter query
                    try (Scanner requestBodyScanner = new Scanner(exchange.getRequestBody(), CHARSET).useDelimiter("\\A")) {
                        sparqlQuery.setUpdate(requestBodyScanner.next());
                    }

                    // Retrieve from the request URI optional parameters defaultGraphs and namedGraphs
                    // Cannot apply directly exchange.getRequestURI().getQuery() since getQuery() method
                    // automatically decodes query (requestQuery must remain unencoded due to parsing by '&' delimiter)
                    String requestQueryRaw = exchange.getRequestURI().getRawQuery();
                    if (requestQueryRaw != null) {
                        StringTokenizer stk = new StringTokenizer(requestQueryRaw, AND_DELIMITER);
                        while (stk.hasMoreTokens()) {
                            parseUpdateParameter(stk.nextToken(), sparqlQuery);
                        }
                    }
                } else {
                    throw new IllegalArgumentException("Content-Type of POST request has to be one of " + Arrays.asList(ENCODED_CONTENT, UNENCODED_QUERY_CONTENT, UNENCODED_UPDATE_CONTENT));
                }
            } catch (MimeTypeParseException e) {
                throw new IllegalArgumentException("Illegal Content-Type header content");
            }
        } else {
            throw new IllegalArgumentException("Request method has to be only either GET or POST");
        }

        String query = sparqlQuery.getQuery();
        if (query != null) {
            Matcher m = UNRESOLVED_PARAMETERS.matcher(query);
            if (m.find()) {
                throw new IllegalArgumentException("Missing query parameter: " + m.group(1));
            }
        }

        String update = sparqlQuery.getUpdate();
        if (update != null) {
            Matcher m = UNRESOLVED_PARAMETERS.matcher(update);
            if (m.find()) {
                throw new IllegalArgumentException("Missing update parameter: " + m.group(1));
            }
        }

        return sparqlQuery;
    }

    /**
     * Parse single parameter from HTTP request parameters or body.
     *
     * @param param       single raw String parameter
     * @param sparqlQuery SparqlQuery to fill from the parsed parameter
     * @throws UnsupportedEncodingException which never happens
     */
    private static void parseQueryParameter(String param, SparqlQuery sparqlQuery) throws UnsupportedEncodingException {
        if (param.startsWith(QUERY_PREFIX)) {
            sparqlQuery.setQuery(getParameterValue(param, QUERY_PREFIX));
        } else if (param.startsWith(DEFAULT_GRAPH_PREFIX)) {
            sparqlQuery.addDefaultGraph(SVF.createIRI(getParameterValue(param, DEFAULT_GRAPH_PREFIX)));
        } else if (param.startsWith(NAMED_GRAPH_PREFIX)) {
            sparqlQuery.addNamedGraph(SVF.createIRI(getParameterValue(param, NAMED_GRAPH_PREFIX)));
        } else if (param.startsWith(TRACK_RESULT_SIZE)) {
        	sparqlQuery.trackResultSize = Boolean.valueOf(getParameterValue(param, TRACK_RESULT_SIZE));
        } else {
            int i = param.indexOf("=");
            if (i >= 0) {
                sparqlQuery.addParameter(URLDecoder.decode(param.substring(0, i), CHARSET), URLDecoder.decode(param.substring(i + 1), CHARSET));
            } else {
                throw new IllegalArgumentException("Invalid request parameter: " + param);
            }
        }
    }

    private static void parseUpdateParameter(String param, SparqlQuery sparqlQuery) throws UnsupportedEncodingException {
        if (param.startsWith(UPDATE_PREFIX)) {
            sparqlQuery.setUpdate(getParameterValue(param, UPDATE_PREFIX));
        } else if (param.startsWith(USING_GRAPH_URI_PREFIX)) {
            sparqlQuery.addDefaultGraph(SVF.createIRI(getParameterValue(param, USING_GRAPH_URI_PREFIX)));
        } else if (param.startsWith(USING_NAMED_GRAPH_PREFIX)) {
            sparqlQuery.addNamedGraph(SVF.createIRI(getParameterValue(param, USING_NAMED_GRAPH_PREFIX)));
        } else if (param.startsWith(TRACK_RESULT_SIZE)) {
        	sparqlQuery.trackResultSize = Boolean.valueOf(getParameterValue(param, TRACK_RESULT_SIZE));
        } else {
            int i = param.indexOf("=");
            if (i >= 0) {
                sparqlQuery.addParameter(URLDecoder.decode(param.substring(0, i), CHARSET), URLDecoder.decode(param.substring(i + 1), CHARSET));
            } else {
                throw new IllegalArgumentException("Invalid request parameter: " + param);
            }
        }
    }

    private static String getParameterValue(String param, String prefix) throws UnsupportedEncodingException {
   		return URLDecoder.decode(param.substring(prefix.length()), CHARSET);
    }

    /**
     * Evaluate query towards a Sail repository and send response (the query result) back to client
     *
     * @param sparqlQuery query to be evaluated
     * @param exchange    HTTP exchange for sending the response
     * @throws IOException    If an error occurs during sending response to the client
     * @throws RDF4JException If an error occurs due to illegal SPARQL query (e.g. incorrect syntax)
     */
    private void evaluateQuery(SparqlQuery sparqlQuery, HttpExchange exchange) throws IOException, RDF4JException {
        String queryString = sparqlQuery.getQuery();
        // sniff query type and perform content negotiation before opening a connection
        ParsedQuery sniffedQuery = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, queryString, null);
        List<String> acceptedMimeTypes = new ArrayList<>();
        List<String> acceptHeaders = exchange.getRequestHeaders().get("Accept");
        if (acceptHeaders != null) for (String header : acceptHeaders) {
            acceptedMimeTypes.addAll(parseAcceptHeader(header));
        }
        QueryEvaluator evaluator;
        if (sniffedQuery instanceof ParsedTupleQuery) {
            TupleQueryResultWriterRegistry registry = TupleQueryResultWriterRegistry.getInstance();
            QueryResultFormat format = setFormat(registry, exchange.getRequestURI().getPath(),
                    acceptedMimeTypes, TupleQueryResultFormat.CSV, exchange.getResponseHeaders());
            TupleQueryResultWriterFactory writerFactory = registry.get(format).orElseThrow(() -> new IOException("Format not supported: "+format));
            evaluator = query -> {
	            LOGGER.info("Evaluating tuple query: {}", queryString);
	            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
	            BufferedOutputStream response = new BufferedOutputStream(exchange.getResponseBody());
                TupleQueryResultWriter w = writerFactory.getWriter(response);
                w.setWriterConfig(writerConfig);
                ((TupleQuery) query).evaluate(w);
	            return response;
            };
        } else if (sniffedQuery instanceof ParsedGraphQuery) {
            RDFWriterRegistry registry = RDFWriterRegistry.getInstance();
            RDFFormat format = setFormat(registry, exchange.getRequestURI().getPath(),
                    acceptedMimeTypes, RDFFormat.NTRIPLES, exchange.getResponseHeaders());
            RDFWriterFactory writerFactory = registry.get(format).orElseThrow(() -> new IOException("Format not supported: "+format));
            evaluator = query -> {
	            LOGGER.info("Evaluating graph query: {}", queryString);
	            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
	            BufferedOutputStream response = new BufferedOutputStream(exchange.getResponseBody());
                RDFWriter w = writerFactory.getWriter(response);
                w.setWriterConfig(writerConfig);
                ((GraphQuery) query).evaluate(w);
	            return response;
            };
        } else if (sniffedQuery instanceof ParsedBooleanQuery) {
            BooleanQueryResultWriterRegistry registry = BooleanQueryResultWriterRegistry.getInstance();
            QueryResultFormat format = setFormat(registry, exchange.getRequestURI().getPath(),
                    acceptedMimeTypes, BooleanQueryResultFormat.JSON, exchange.getResponseHeaders());
            BooleanQueryResultWriterFactory writerFactory = registry.get(format).orElseThrow(() -> new IOException("Format not supported: "+format));
            evaluator = query -> {
	            LOGGER.info("Evaluating boolean query: {}", queryString);
	            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
	            BufferedOutputStream response = new BufferedOutputStream(exchange.getResponseBody());
                BooleanQueryResultWriter w = writerFactory.getWriter(response);
                w.setWriterConfig(writerConfig);
                w.handleBoolean(((BooleanQuery) query).evaluate());
	            return response;
            };
        } else {
        	throw new MalformedQueryException("Invalid query");
        }
        OutputStream response;
        try(SailRepositoryConnection connection = repository.getConnection()) {
	        SailQuery query = connection.prepareQuery(QueryLanguage.SPARQL, queryString, null);
	        Dataset dataset = sparqlQuery.getDataset();
	        if (!dataset.getDefaultGraphs().isEmpty() || !dataset.getNamedGraphs().isEmpty()) {
	            // This will include default graphs and named graphs from  the request parameters but default graphs and
	            // named graphs contained in the string query will be ignored
	            query.getParsedQuery().setDataset(dataset);
	        }
	        response = evaluator.evaluate(query);
    	}
        // commit the response *after* closing the connection
        response.close();
       	LOGGER.info("Query successfully processed");
    }

    private void evaluateUpdate(SparqlQuery sparqlQuery, HttpExchange exchange) throws IOException, RDF4JException {
        String updateString = sparqlQuery.getUpdate();
        ParsedUpdate parsedUpdate;
    	try(SailRepositoryConnection connection = repository.getConnection()) {
    		if (connection.getSailConnection() instanceof ResultTrackingSailConnection) {
    			((ResultTrackingSailConnection)connection.getSailConnection()).setTrackResultSize(sparqlQuery.trackResultSize);
    		}
	        SailUpdate update = (SailUpdate) connection.prepareUpdate(QueryLanguage.SPARQL, updateString, null);
	    	parsedUpdate = update.getParsedUpdate();
	        Dataset dataset = sparqlQuery.getDataset();
	        if (!dataset.getDefaultGraphs().isEmpty() || !dataset.getNamedGraphs().isEmpty()) {
	            // This will include default graphs and named graphs from  the request parameters
	        	if (!parsedUpdate.getDatasetMapping().isEmpty()) {
	        		throw new IllegalArgumentException("Can't provide graph-uri parameters for queries containing USING, USING NAMED or WITH clauses");
	        	}
	        	for (UpdateExpr expr : parsedUpdate.getUpdateExprs()) {
	        		parsedUpdate.map(expr, dataset);
	        	}
	        }
	        LOGGER.info("Executing update: {}", updateString);
	        update.execute();
    	}

    	if (sparqlQuery.trackResultSize) {
	        StringBuilderWriter buf = new StringBuilderWriter(128);
        	JsonGenerator json = new ObjectMapper().createGenerator(buf);
        	json.writeStartObject();
        	json.writeArrayFieldStart("results");
	    	for (UpdateExpr expr : parsedUpdate.getUpdateExprs()) {
	    		if (expr instanceof Modify) {
	    			Modify modify = (Modify) expr;
	    			json.writeStartObject();
	    			if (modify.getDeleteExpr() != null) {
	    				json.writeNumberField("deleted", modify.getDeleteExpr().getResultSizeActual());
	    			}
	    			if (modify.getInsertExpr() != null) {
	    				json.writeNumberField("inserted", modify.getInsertExpr().getResultSizeActual());
	    			}
	    			json.writeEndObject();
	    		}
	    	}
	    	json.writeEndArray();
	    	json.writeEndObject();
	    	json.close();
	    	buf.close();
	    	exchange.getResponseHeaders().set("Content-Type", "application/json");
	    	sendResponse(exchange, HttpURLConnection.HTTP_OK, buf.toString());
        } else {
        	exchange.sendResponseHeaders(HttpURLConnection.HTTP_NO_CONTENT, -1);
        }
       	LOGGER.info("Update successfully processed");
    }

    /**
     * Send response of the processed request to the client
     *
     * @param exchange HttpExchange wrapper encapsulating response
     * @param code     HTTP code
     * @param exception  Content of the response
     * @throws IOException
     */
    private void sendErrorResponse(HttpExchange exchange, int code, Exception exception) throws IOException {
        StringWriter sw = new StringWriter();
        try (PrintWriter w = new PrintWriter(sw)) {
            exception.printStackTrace(w);
        }
        sendResponse(exchange, code, sw.toString());
    }
    private void sendResponse(HttpExchange exchange, int code, String s) throws IOException {
        byte[] payload = s.getBytes(CHARSET);
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
    private <FF extends FileFormat, S extends Object> FF setFormat(FileFormatServiceRegistry<FF, S> reg,
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
    private static final class SparqlQuery {
        // SPARQL query string, has to be exactly one
        private String query, update;
        // SPARQL query template parameters for substitution
        private final List<String> parameterNames = new ArrayList<>(), parameterValues = new ArrayList<>();
        // Dataset containing default graphs and named graphs
        private final SimpleDataset dataset = new SimpleDataset();
        private boolean trackResultSize;

        public String getQuery() {
            //replace all tokens matching {{parameterName}} inside the SPARQL query with corresponding parameterValues
            return StringUtils.replaceEach(query, parameterNames.toArray(new String[parameterNames.size()]), parameterValues.toArray(new String[parameterValues.size()]));
        }

        public void setQuery(String query) {
        	if (this.update != null) {
                throw new IllegalArgumentException("Unexpected update string encountered");
        	}
        	if (this.query != null) {
                throw new IllegalArgumentException("Multiple query strings encountered");
        	}
            this.query = query;
        }

        public String getUpdate() {
            //replace all tokens matching {{parameterName}} inside the SPARQL query with corresponding parameterValues
            return StringUtils.replaceEach(update, parameterNames.toArray(new String[parameterNames.size()]), parameterValues.toArray(new String[parameterValues.size()]));
        }

        public void setUpdate(String update) {
        	if (this.query != null) {
                throw new IllegalArgumentException("Unexpected query string encountered");
        	}
        	if (this.update != null) {
                throw new IllegalArgumentException("Multiple update strings encountered");
        	}
            this.update = update;
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

    private static interface QueryEvaluator {
    	OutputStream evaluate(SailQuery query) throws IOException;
    }
}
