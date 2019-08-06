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

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.*;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.commons.io.IOUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test handling GET and POST requests which contain SPARQL query to be executed towards a Sail repository.
 * Only ASK and SELECT queries are supported (UPDATE queries are not supported).
 * Tests are inspired by the SPARQL 1.0 and 1.1 protocol test suit.
 *
 * @author sykorjan
 * @see <a href="https://w3c.github.io/rdf-tests/sparql11/data-sparql11/protocol/index.html">
 * W3C SPARQL Working Group's SPARQL1.0 test suite</a>
 * @see <a href="https://github.com/kasei/sparql11-protocolvalidator">SPARQL 1.1 Protocol Tests</a>
 */
public class HttpSparqlHandlerTest {

    private static final String SERVER_CONTEXT = "/";
    private static final int PORT = 0;
    private static final String CHARSET = "UTF-8";

    // Request content type (only for POST requests)
    private static final String ENCODED_CONTENT = "application/x-www-form-urlencoded";
    private static final String UNENCODED_CONTENT = "application/sparql-query";

    // Response content type
    private static final String XML_CONTENT = "application/sparql-results+xml";
    private static final String JSON_CONTENT = "application/sparql-results+json";
    private static final String JSONLD_CONTENT = "application/ld+json";
    private static final String TSV_CONTENT = "text/tab-separated-values";
    private static final String CSV_CONTENT = "text/csv";
    private static final String TURTLE_CONTENT = "text/turtle";
    private static final String CHARSER_SUFFIX = "; charset=" + CHARSET;
    // test data
    private static final ValueFactory factory = SimpleValueFactory.getInstance();
    private static final Resource SUBJ = factory.createIRI("http://ginger/subject/");
    private static final IRI PRED = factory.createIRI("http://ginger/pred/");
    private static final Value OBJ = factory.createLiteral("ginger literal");
    private static final Resource CONTEXT = factory.createIRI("http://ginger/");

    private static final Resource SUBJ2 = factory.createIRI("http://carrot/subject2/");
    private static final IRI PRED2 = factory.createIRI("http://carrot/pred2/");
    private static final Value OBJ2 = factory.createLiteral("carrot literal 2");
    private static final Resource CONTEXT2 = factory.createIRI("http://carrot/");

    private static final Resource SUBJ3 = factory.createIRI("http://potato/subject2/");
    private static final IRI PRED3 = factory.createIRI("http://potato/pred2/");
    private static final Value OBJ3 = factory.createLiteral("potato literal 2");
    private static final Resource CONTEXT3 = factory.createIRI("http://potato/");

    private static final Resource NON_EXISTING_CONTEXT = factory.createIRI("http://broccoli/");

    private static SimpleHttpServer server;
    private static SailRepositoryConnection repositoryConnection;
    // SimpleHttpServer URL
    private static String SERVER_URL;

    /**
     * Create HTTP server, handler and repositoryConnection to Sail repository first
     *
     * @throws IOException
     */
    @BeforeClass
    public static void init() throws IOException {
        // Create sail repository
        SailRepository repository = new SailRepository(new MemoryStore());
        repository.initialize();

        // Create repositoryConnection to the sail repository
        repositoryConnection = repository.getConnection();
        repositoryConnection.begin();

        // Add some test data
        repositoryConnection.add(factory.createStatement(SUBJ, PRED, OBJ, CONTEXT));
        repositoryConnection.add(factory.createStatement(SUBJ2, PRED2, OBJ2, CONTEXT2));
        repositoryConnection.add(factory.createStatement(SUBJ3, PRED3, OBJ3, CONTEXT3));
        repositoryConnection.commit();

        // Provide stored query
        Properties storedQueries = new Properties();
        storedQueries.put("test_path", "ask {<{{test_parameter1}}> <{{test_parameter2}}> ?obj}");

        // Alter writer configuration
        Properties writerCfg = new Properties();
        writerCfg.put("org.eclipse.rdf4j.rio.helpers.JSONLDSettings.COMPACT_ARRAYS", "java.lang.Boolean.FALSE");
        writerCfg.put("org.eclipse.rdf4j.rio.helpers.JSONLDSettings.JSONLD_MODE", "org.eclipse.rdf4j.rio.helpers.JSONLDMode.COMPACT");

        // Create handler with the repositoryConnection to the sail repository
        HttpSparqlHandler handler = new HttpSparqlHandler(repositoryConnection, storedQueries, writerCfg, true);

        // Create and start http server
        server = new SimpleHttpServer(PORT, SERVER_CONTEXT, handler);
        server.start();
        SERVER_URL = "http://localhost:" + server.getAddress().getPort();
    }

    /**
     * Stop HTTP server and close repositoryConnection to Sail repository
     */
    @AfterClass
    public static void clean() {
        server.stop();
    }

    /**
     * Invoke query operation with a method other than GET or POST
     *
     * @throws IOException
     */
    @Test
    public void testBadQueryMethod() throws IOException {
        URL url = new URL(SERVER_URL);
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setDoOutput(true);
        urlConnection.setRequestMethod("PUT");
        OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream());
        out.write("query=ASK%20%7B%7D");
        out.close();
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, urlConnection.getResponseCode());
    }

    /**
     * Invoke query operation with more than one query string
     *
     * @throws IOException
     */
    @Test
    public void testBadMultipleQueries() throws IOException {
        String GET_URL = SERVER_URL + "?query=ASK%20%7B%7D&query=SELECT%20%2A%20%7B%7D";
        URL url = new URL(GET_URL);
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setRequestMethod("GET");
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, urlConnection.getResponseCode());
    }

    /**
     * Invoke query operation with a POST with media type that's not url-encoded or application/sparql-query
     */
    @Test
    public void testWrongMediaType() throws IOException {
        URL url = new URL(SERVER_URL);
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setDoOutput(true);
        urlConnection.setRequestMethod("POST");
        urlConnection.setRequestProperty("Content-Type", "text/plain");
        OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream());
        out.write("ASK {}");
        out.close();
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, urlConnection.getResponseCode());
    }

    /**
     * Invoke query operation with url-encoded body, but without application/x-www-url-form-urlencoded media type
     */
    @Test
    @Ignore("Cannot send HTTP POST request without Content-Type header")
    public void testMissingFormType() throws IOException {
        /* Cannot send HTTP POST request without Content-Type header
            curl did not work
            java.net did not work
            org.apache.http did not work

            All of these tools/libraries always add Content-Type header and it cannot be removed
            It is possible to send this header with empty value but it is not goal of this test
         */

//        // curl did not work for me --> still could not get rid of Content-Type header when using -H option
//        Runtime runtime = Runtime.getRuntime();
//        try {
//            Process process = runtime.exec("curl -H \"Content-Type:\" -d \"query=ASK%20%7B%7D\" -X POST http://localhost:8000");
//            int resultCode = process.waitFor();
//
//            if (resultCode == 0) {
//                // all is good
//            }
//        } catch (Throwable cause) {
//            // process cause
//        }


//        // RequestBuilder always adds Content-Type header and it cannot be removed
//        Header h1 = new BasicHeader(HttpHeaders.HOST, SERVER_URL);
//        Header h2 = new BasicHeader(HttpHeaders.USER_AGENT, "sparql-client/0.1");
//
//        List<Header> headers = new ArrayList<>();
//        headers.add(h1);
//        headers.add(h2);
//
//        HttpClient client = HttpClients.custom().setDefaultHeaders(headers).build();
//        HttpUriRequest request =
//                RequestBuilder.post().setUri(SERVER_URL).addParameter("query", "ASK%20%7B%7D").build();
//        HttpResponse response = client.execute(request);
//        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, response.getStatusLine().getStatusCode());


//        // HttpPost always adds Content-Type header and it cannot be removed
//        HttpClient client = HttpClientBuilder.create().setDefaultHeaders(headers).build();
//        HttpPost post = new HttpPost(SERVER_URL);
//        post.setHeader("Content-Type", null);
//
//        for (Header header : post.getHeaders("Content-Type")) {
//            post.removeHeader(header);
//        }
//
//        List<NameValuePair> urlParameters = new ArrayList<>();
//        urlParameters.add(new BasicNameValuePair("query", "ASK {}"));
//        post.setEntity(new UrlEncodedFormEntity(urlParameters));
//        HttpResponse response = client.execute(post);
//        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, response.getStatusLine().getStatusCode());
    }

    /**
     * Invoke query operation with SPARQL body, but without application/sparql-query media type
     */
    @Test
    @Ignore("Cannot send HTTP POST request without Content-Type header")
    public void testMissingDirectType() {
        // TODO same issue testing this case as testMissingFormType()
    }

    /**
     * Invoke query operation with direct POST, but with a non-UTF8 encoding (UTF-16)
     */
    @Test
    public void testNonUTF8() throws IOException {
        URL url = new URL(SERVER_URL);
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setDoOutput(true);
        urlConnection.setRequestMethod("POST");
        urlConnection.setRequestProperty("Content-Type", "application/sparql-query; charset=UTF-16");
        OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream());
        out.write("ASK {}");
        out.close();
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, urlConnection.getResponseCode());
    }

    /**
     * Invoke query operation with invalid query syntax
     *
     * @throws IOException
     */
    @Test
    public void testInvalidQuerySyntax() throws IOException {
        String GET_URL = SERVER_URL + "?query=ASK%20%7B";
        URL url = new URL(GET_URL);
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setRequestMethod("GET");
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, urlConnection.getResponseCode());
    }

    /**
     * Invoke correct query operation via GET method
     */
    @Test
    public void testQueryGet() throws IOException {
        String GET_URL = SERVER_URL + "?query=ASK%20%7B%7D";
        URL url = new URL(GET_URL);
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setRequestMethod("GET");
        urlConnection.setRequestProperty("Accept", XML_CONTENT);
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
        assertEquals(XML_CONTENT + CHARSER_SUFFIX, urlConnection.getContentType());
        checkXMLResponseContent(urlConnection, "true", "/sparql/boolean");
    }

    /**
     * Invoke query without Accept header
     */
    @Test
    public void testMissingAccept() throws IOException {
        String GET_URL = SERVER_URL + "?query=ASK%20%7B%7D";
        URL url = new URL(GET_URL);
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setRequestMethod("GET");
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
    }

    /**
     * Invoke correct query operation via URL-encoded POST
     */
    @Test
    public void testQueryPostForm() throws IOException {
        URL url = new URL(SERVER_URL);
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setDoOutput(true);
        urlConnection.setRequestMethod("POST");
        urlConnection.setRequestProperty("Accept", XML_CONTENT);
        urlConnection.setRequestProperty("Content-Type", ENCODED_CONTENT);
        OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream());
        out.write("query=ASK%20%7B%7D");
        out.close();
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
        assertEquals(XML_CONTENT + CHARSER_SUFFIX, urlConnection.getContentType());
        checkXMLResponseContent(urlConnection, "true", "/sparql/boolean");
    }

    /**
     * Invoke correct query operation via GET method
     */
    @Test
    public void testStoredQueryGet() throws IOException {
        String GET_URL = SERVER_URL + "/test_path?test_parameter1=" + URLEncoder.encode(SUBJ.stringValue(), CHARSET) + "&test_parameter2=" + URLEncoder.encode(PRED.stringValue(), CHARSET);
        URL url = new URL(GET_URL);
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setRequestMethod("GET");
        urlConnection.setRequestProperty("Accept", XML_CONTENT);
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
        assertEquals(XML_CONTENT + CHARSER_SUFFIX, urlConnection.getContentType());
        checkXMLResponseContent(urlConnection, "true", "/sparql/boolean");
    }

    /**
     * Invoke stored query operation with missing parameter
     */
    @Test
    public void testStoredQueryMissingParam() throws IOException {
        String GET_URL = SERVER_URL + "/test_path?test_parameter1=" + URLEncoder.encode(SUBJ.stringValue(), CHARSET);
        URL url = new URL(GET_URL);
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setRequestMethod("GET");
        urlConnection.setRequestProperty("Accept", XML_CONTENT);
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, urlConnection.getResponseCode());
        try (InputStream in  = urlConnection.getErrorStream()) {
            assertTrue(IOUtils.toString(in).contains("test_parameter2"));
        }
    }

    /**
     * Invoke correct query operation via URL-encoded POST
     */
    @Test
    public void testStoredQueryPostForm() throws IOException {
        URL url = new URL(SERVER_URL + "/test_path");
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setDoOutput(true);
        urlConnection.setRequestMethod("POST");
        urlConnection.setRequestProperty("Accept", XML_CONTENT);
        urlConnection.setRequestProperty("Content-Type", ENCODED_CONTENT);
        OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream());
        out.write("test_parameter1=" + URLEncoder.encode(SUBJ.stringValue(), CHARSET) + "&test_parameter2=" + URLEncoder.encode(PRED.stringValue(), CHARSET));
        out.close();
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
        assertEquals(XML_CONTENT + CHARSER_SUFFIX, urlConnection.getContentType());
        checkXMLResponseContent(urlConnection, "true", "/sparql/boolean");
    }

    /**
     * Invoke correct query operation via GET method
     */
    @Test
    public void testStoredQueryWithJsonExtension() throws IOException {
        String GET_URL = SERVER_URL + "/test_path.json?test_parameter1=" + URLEncoder.encode(SUBJ.stringValue(), CHARSET) + "&test_parameter2=" + URLEncoder.encode(PRED.stringValue(), CHARSET);
        URL url = new URL(GET_URL);
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setRequestMethod("GET");
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
        assertEquals(JSON_CONTENT + CHARSER_SUFFIX, urlConnection.getContentType());
    }

    /**
     * Invoke correct query operation via POST directly
     */
    @Test
    public void testQueryPostDirect() throws IOException {
        URL url = new URL(SERVER_URL);
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setDoOutput(true);
        urlConnection.setRequestMethod("POST");
        urlConnection.setRequestProperty("Accept", XML_CONTENT);
        urlConnection.setRequestProperty("Content-Type", UNENCODED_CONTENT);
        OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream());
        out.write("ASK {}");
        out.close();
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
        assertEquals(XML_CONTENT + CHARSER_SUFFIX, urlConnection.getContentType());
        checkXMLResponseContent(urlConnection, "true", "/sparql/boolean");
    }

    /**
     * Invoke correct query and check for appropriate response content type (expect one of: XML, JSON, CSV, TSV)
     */
    @Test
    public void testQueryContentTypeSelect() throws IOException {
        URL url = new URL(SERVER_URL);
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setDoOutput(true);
        urlConnection.setRequestMethod("POST");
        urlConnection.setRequestProperty("Accept", XML_CONTENT);
        urlConnection.setRequestProperty("Content-Type", UNENCODED_CONTENT);
        OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream());
        out.write("SELECT (1 AS ?value) {}");
        out.close();
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
        List<String> validResponseContent = Arrays.asList(XML_CONTENT + CHARSER_SUFFIX, JSON_CONTENT + CHARSER_SUFFIX, TSV_CONTENT + CHARSER_SUFFIX, CSV_CONTENT + CHARSER_SUFFIX);
        assertTrue(validResponseContent.contains(urlConnection.getContentType()));
    }

    /**
     * Invoke graph query with expected compacted JSONLD result
     */
    @Test
    public void testWriterConfig() throws IOException {
        URL url = new URL(SERVER_URL);
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setDoOutput(true);
        urlConnection.setRequestMethod("POST");
        urlConnection.setRequestProperty("Content-Type", UNENCODED_CONTENT);
        urlConnection.setRequestProperty("Accept", JSONLD_CONTENT);
        OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream());
        out.write("prefix w: <http://whatever/> construct {w:a w:b 1.} where {}");
        out.close();
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
        assertEquals(JSONLD_CONTENT + CHARSER_SUFFIX, urlConnection.getContentType());
        boolean w = false, wa = false, wb = false;
        for (String line : IOUtils.readLines(urlConnection.getInputStream())) {
            w |= line.contains("\"w\"");
            wa |= line.contains("\"w:a\"");
            wb |= line.contains("\"w:b\"");
        }
        assertTrue(w);
        assertTrue(wa);
        assertTrue(wb);
    }

    /**
     * Invoke query operation with empty query parameter
     */
    @Test
    public void testEmptyQueryParameter() throws IOException {
        URL url = new URL(SERVER_URL + "?query=");
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setRequestMethod("GET");
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, urlConnection.getResponseCode());
    }

    /**
     * Invoke query operation with an unspecified parameter
     *
     * @throws IOException
     */
    @Test
    public void testUnspecifiedParameterViaGet() throws IOException {
        URL url = new URL(SERVER_URL + "?unspecifiedParameter=");
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setRequestMethod("GET");
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, urlConnection.getResponseCode());
    }

    /**
     * Invoke graph query
     */
    @Test
    public void testGraphQuery() throws IOException {
        URL url = new URL(SERVER_URL +
                "?query=" + URLEncoder.encode("DESCRIBE ?x WHERE { ?x  ?y ?z }", CHARSET));
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setRequestMethod("GET");
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
    }

    /**
     * Invoke correct query operation with multiple named and default graphs via POST headers
     */
    @Test
    public void testDefaultAndNamedGraphsViaPostHeaders() throws IOException {
        URL url = new URL(SERVER_URL
                + "?default-graph-uri=" + URLEncoder.encode(CONTEXT.toString(), CHARSET)
                + "&named-graph-uri=" + URLEncoder.encode(CONTEXT2.toString(), CHARSET)
                + "&named-graph-uri=" + URLEncoder.encode(CONTEXT3.toString(), CHARSET)
        );
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setDoOutput(true);
        urlConnection.setRequestMethod("POST");
        urlConnection.setRequestProperty("Accept", XML_CONTENT);
        urlConnection.setRequestProperty("Content-Type", UNENCODED_CONTENT);
        OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream());
        out.write(
                "SELECT (COUNT(*) AS ?count) " +
                        "WHERE { ?x ?y ?z  GRAPH ?g { ?s ?p ?o } }");
        out.close();
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
        assertEquals(XML_CONTENT + CHARSER_SUFFIX, urlConnection.getContentType());
        checkXMLResponseContent(urlConnection, "2", "/sparql/results/result/binding/literal");
    }

    /**
     * Invoke correct query operation with multiple named and default graphs via POST message body
     */
    @Test
    public void testDefaultAndNamedGraphsViaPostMessageBody() throws IOException {
        URL url = new URL(SERVER_URL);
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setDoOutput(true);
        urlConnection.setRequestMethod("POST");
        urlConnection.setRequestProperty("Accept", XML_CONTENT);
        urlConnection.setRequestProperty("Content-Type", ENCODED_CONTENT);
        OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream());
        out.write(
                "query=" + URLEncoder.encode("SELECT (COUNT(*) AS ?count) WHERE { ?x ?y ?z  GRAPH ?g { ?s ?p ?o } }",
                        CHARSET)
                        + "&default-graph-uri=" + URLEncoder.encode(CONTEXT.toString(), CHARSET)
                        + "&named-graph-uri=" + URLEncoder.encode(CONTEXT2.toString(), CHARSET)
                        + "&named-graph-uri=" + URLEncoder.encode(CONTEXT3.toString(), CHARSET)

        );
        out.close();
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
        assertEquals(XML_CONTENT + CHARSER_SUFFIX, urlConnection.getContentType());
        checkXMLResponseContent(urlConnection, "2", "/sparql/results/result/binding/literal");
    }

    /**
     * Invoke correct query operation with multiple named and default graphs via GET
     */
    @Test
    public void testDefaultAndNamedGraphsViaGet() throws IOException {
        URL url = new URL(SERVER_URL + "?query="
                + URLEncoder.encode("SELECT (COUNT(*) AS ?count) WHERE { ?x ?y ?z GRAPH ?g { ?s ?p ?o } }", CHARSET)
                + "&default-graph-uri=" + URLEncoder.encode(CONTEXT.toString(), CHARSET)
                + "&named-graph-uri=" + URLEncoder.encode(CONTEXT2.toString(), CHARSET)
                + "&named-graph-uri=" + URLEncoder.encode(CONTEXT3.toString(), CHARSET)
        );
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setRequestMethod("GET");
        urlConnection.setRequestProperty("Accept", XML_CONTENT);
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
        assertEquals(XML_CONTENT + CHARSER_SUFFIX, urlConnection.getContentType());
        checkXMLResponseContent(urlConnection, "2", "/sparql/results/result/binding/literal");
    }

    /**
     * Invoke correct query operation with multiple MIME types in one Accept header
     */
    @Test
    public void testMultipleMimeTypes() throws IOException {
        URL url = new URL(SERVER_URL + "?query=" + URLEncoder.encode("CONSTRUCT WHERE { ?s ?p ?o . } LIMIT 10",
                CHARSET));
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setRequestMethod("GET");
        urlConnection.setRequestProperty("Accept", "application/turtle;q=0.7, text/turtle;q=0.3, */*;q=0.8");
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
        assertEquals(TURTLE_CONTENT + CHARSER_SUFFIX, urlConnection.getContentType());
    }

    /**
     * Invoke correct query operation with a GET method containing '&' characters
     *
     * @throws IOException
     */
    @Test
    public void testAmpersandCharacter() throws IOException {
        URL url = new URL(SERVER_URL + "?query=" + URLEncoder.encode("ASK { [] ?p \"&\" . }", CHARSET));
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setRequestMethod("GET");
        urlConnection.setRequestProperty("Accept", XML_CONTENT);
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
    }

    /**
     * Help method for checking response content, only boolean value is expected (result of ASK query)
     *
     * @param urlConnection
     * @param expected
     * @param xpathExpr     XPath expression for searching the result in the XML document
     */
    private void checkXMLResponseContent(HttpURLConnection urlConnection, String expected, String xpathExpr) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(urlConnection.getInputStream());
            XPathFactory xPathfactory = XPathFactory.newInstance();
            XPath xpath = xPathfactory.newXPath();
            XPathExpression expr = xpath.compile(xpathExpr);
            String result = expr.evaluate(doc);
            assertEquals(expected, result);
        } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
            e.printStackTrace();
        }
    }
}
