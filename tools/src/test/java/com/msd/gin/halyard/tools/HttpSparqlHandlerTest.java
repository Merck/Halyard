package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.tools.HttpSparqlHandler;
import com.msd.gin.halyard.tools.SimpleHttpServer;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
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
import java.util.Arrays;
import java.util.List;

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

    private static final String CONTEXT = "/";
    private static final int PORT = 8000;
    private static final String SERVER_URL = "http://localhost:" + PORT;

    // Request content type (only for POST requests)
    private static final String ENCODED_CONTENT = "application/x-www-form-urlencoded";
    private static final String UNENCODED_CONTENT = "application/sparql-query";

    // Response content type
    private static final String XML_CONTENT = "application/sparql-results+xml";
    private static final String JSON_CONTENT = "application/sparql-results+json";
    private static final String TSV_CONTENT = "text/tab-separated-values";
    private static final String CSV_CONTENT = "text/csv";

    private static final String TRIPLE = "<http://a> <http://b> <http://c> .";


    private static SimpleHttpServer server;
    private static SailRepositoryConnection repositoryConnection;

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
        repositoryConnection.add(new StringReader(TRIPLE), "http://whatever/", RDFFormat.NTRIPLES);

        // Create handler with the repositoryConnection to the sail repository
        HttpSparqlHandler handler = new HttpSparqlHandler(repositoryConnection);

        // Create and start http server
        server = new SimpleHttpServer(PORT, CONTEXT, handler);
        server.start();


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
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
        List<String> validResponseContent = Arrays.asList(XML_CONTENT, JSON_CONTENT);
        assertTrue(validResponseContent.contains(urlConnection.getContentType()));
        if (urlConnection.getContentType().equals(XML_CONTENT)) {
            checkXMLResponseContent(urlConnection, true);
        }
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
        urlConnection.setRequestProperty("Content-Type", ENCODED_CONTENT);
        OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream());
        out.write("query=ASK%20%7B%7D");
        out.close();
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
        List<String> validResponseContent = Arrays.asList(XML_CONTENT, JSON_CONTENT);
        assertTrue(validResponseContent.contains(urlConnection.getContentType()));
        if (urlConnection.getContentType().equals(XML_CONTENT)) {
            checkXMLResponseContent(urlConnection, true);
        }
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
        urlConnection.setRequestProperty("Content-Type", UNENCODED_CONTENT);
        OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream());
        out.write("ASK {}");
        out.close();
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
        List<String> validResponseContent = Arrays.asList(XML_CONTENT, JSON_CONTENT);
        assertTrue(validResponseContent.contains(urlConnection.getContentType()));
        if (urlConnection.getContentType().equals(XML_CONTENT)) {
            checkXMLResponseContent(urlConnection, true);
        }
    }


    /**
     * Invoke correct query operation with a protocol-specified default graph via POST
     */
    @Ignore("Not ready yet")
    @Test
    public void testQueryDatasetDefaultGraph() throws IOException {
        URL url = new URL(SERVER_URL +
                "?default-graph-uri=http%3A%2F%2Fkasei.us%2F2009%2F09%2Fsparql%2Fdata%2Fdata1.rdf");
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setDoOutput(true);
        urlConnection.setRequestMethod("POST");
        urlConnection.setRequestProperty("Content-Type", UNENCODED_CONTENT);
        OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream());
        out.write("ASK { <http://kasei.us/2009/09/sparql/data/data1.rdf> ?p ?o }");
        out.close();
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
        List<String> validResponseContent = Arrays.asList(XML_CONTENT, JSON_CONTENT);
        assertTrue(validResponseContent.contains(urlConnection.getContentType()));
        if (urlConnection.getContentType().equals(XML_CONTENT)) {
            checkXMLResponseContent(urlConnection, true);
        }
    }

    /**
     * Invoke correct query operation with multiple protocol-specified default graphs via GET
     */
    @Ignore("Not ready yet")
    @Test
    public void testQueryDatasetDefaultGraphsGet() throws IOException {
        String GET_URL = SERVER_URL + "?query=ASK%20%7B%20%3Chttp%3A%2F%2Fkasei" +
                ".us%2F2009%2F09%2Fsparql%2Fdata%2Fdata1.rdf%3E%20a%20%3Ftype%20.%20%3Chttp%3A%2F%2Fkasei" +
                ".us%2F2009%2F09%2Fsparql%2Fdata%2Fdata2.rdf%3E%20a%20%3Ftype%20.%20%7D" +
                "&default-graph-uri=http%3A%2F%2Fkasei.us%2F2009%2F09%2Fsparql%2Fdata%2Fdata1.rdf" +
                "&default-graph-uri=http%3A%2F%2Fkasei.us%2F2009%2F09%2Fsparql%2Fdata%2Fdata2.rdf";
        URL url = new URL(GET_URL);
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setRequestMethod("GET");
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
        List<String> validResponseContent = Arrays.asList(XML_CONTENT, JSON_CONTENT);
        assertTrue(validResponseContent.contains(urlConnection.getContentType()));
        if (urlConnection.getContentType().equals(XML_CONTENT)) {
            checkXMLResponseContent(urlConnection, true);
        }
    }

    /**
     * Invoke correct query operation with multiple protocol-specified default graphs via POST
     */
    @Ignore("Not ready yet")
    @Test
    public void testQueryDatasetDefaultGraphsPost() throws IOException {
        URL url = new URL(SERVER_URL +
                "?default-graph-uri=http%3A%2F%2Fkasei.us%2F2009%2F09%2Fsparql%2Fdata%2Fdata1.rdf" +
                "&default-graph-uri=http%3A%2F%2Fkasei.us%2F2009%2F09%2Fsparql%2Fdata%2Fdata2.rdf");
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setDoOutput(true);
        urlConnection.setRequestMethod("POST");
        urlConnection.setRequestProperty("Content-Type", UNENCODED_CONTENT);
        OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream());
        out.write("ASK { <http://kasei.us/2009/09/sparql/data/data1.rdf> ?p ?o . " +
                "<http://kasei.us/2009/09/sparql/data/data2.rdf> ?p ?o }");
        out.close();
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
        List<String> validResponseContent = Arrays.asList(XML_CONTENT, JSON_CONTENT);
        assertTrue(validResponseContent.contains(urlConnection.getContentType()));
        if (urlConnection.getContentType().equals(XML_CONTENT)) {
            checkXMLResponseContent(urlConnection, true);
        }
    }

    /**
     * Invoke correct query operation with multiple protocol-specified named graphs via GET
     */
    @Ignore("Not ready yet")
    @Test
    public void testQueryDatasetNamedGraphsGet() throws IOException {
        String GET_URL = SERVER_URL + "?query=ASK%20%7B%20GRAPH%20%3Fg1%20%7B%20%3Chttp%3A%2F%2Fkasei" +
                ".us%2F2009%2F09%2Fsparql%2Fdata%2Fdata1" +
                ".rdf%3E%20a%20%3Ftype%20%7D%20GRAPH%20%3Fg2%20%7B%20%3Chttp%3A%2F%2Fkasei" +
                ".us%2F2009%2F09%2Fsparql%2Fdata%2Fdata2.rdf%3E%20a%20%3Ftype%20%7D%20%7D" +
                "&named-graph-uri=http%3A%2F%2Fkasei.us%2F2009%2F09%2Fsparql%2Fdata%2Fdata1.rdf" +
                "&named-graph-uri=http%3A%2F%2Fkasei.us%2F2009%2F09%2Fsparql%2Fdata%2Fdata2.rdf";
        URL url = new URL(GET_URL);
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setRequestMethod("GET");
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
        List<String> validResponseContent = Arrays.asList(XML_CONTENT, JSON_CONTENT);
        assertTrue(validResponseContent.contains(urlConnection.getContentType()));
        if (urlConnection.getContentType().equals(XML_CONTENT)) {
            checkXMLResponseContent(urlConnection, true);
        }
    }

    /**
     * Invoke correct query operation with multiple protocol-specified named graphs via POST
     */
    @Ignore("Not ready yet")
    @Test
    public void testQueryDatasetNamedGraphsPost() throws IOException {
        URL url = new URL(SERVER_URL +
                "?named-graph-uri=http%3A%2F%2Fkasei.us%2F2009%2F09%2Fsparql%2Fdata%2Fdata1.rdf" +
                "&named-graph-uri=http%3A%2F%2Fkasei.us%2F2009%2F09%2Fsparql%2Fdata%2Fdata2.rdf");
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setDoOutput(true);
        urlConnection.setRequestMethod("POST");
        urlConnection.setRequestProperty("Content-Type", UNENCODED_CONTENT);
        OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream());
        out.write("ASK { GRAPH ?g { ?s ?p ?o } }");
        out.close();
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
        List<String> validResponseContent = Arrays.asList(XML_CONTENT, JSON_CONTENT);
        assertTrue(validResponseContent.contains(urlConnection.getContentType()));
        if (urlConnection.getContentType().equals(XML_CONTENT)) {
            checkXMLResponseContent(urlConnection, true);
        }
    }

    /**
     * Invoke correct query operation with protocol-specified dataset (both named and default graphs)
     */
    @Ignore("Not ready yet")
    @Test
    public void testQueryDatasetFull() throws IOException {
        URL url = new URL(SERVER_URL +
                "?default-graph-uri=http%3A%2F%2Fkasei.us%2F2009%2F09%2Fsparql%2Fdata%2Fdata3.rdf" +
                "&named-graph-uri=http%3A%2F%2Fkasei.us%2F2009%2F09%2Fsparql%2Fdata%2Fdata1.rdf" +
                "&named-graph-uri=http%3A%2F%2Fkasei.us%2F2009%2F09%2Fsparql%2Fdata%2Fdata2.rdf");
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setDoOutput(true);
        urlConnection.setRequestMethod("POST");
        urlConnection.setRequestProperty("Content-Type", UNENCODED_CONTENT);
        OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream());
        out.write("ASK { GRAPH ?g { ?s ?p ?o } }");
        out.close();
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
        List<String> validResponseContent = Arrays.asList(XML_CONTENT, JSON_CONTENT);
        assertTrue(validResponseContent.contains(urlConnection.getContentType()));
        if (urlConnection.getContentType().equals(XML_CONTENT)) {
            checkXMLResponseContent(urlConnection, true);
        }
    }

    /**
     * Invoke query specifying dataset in both query string and protocol; test for use of protocol-specified dataset
     * (test relies on the endpoint allowing client-specified RDF datasets; returns 400 otherwise)
     */
    @Ignore("Not ready yet")
    @Test
    public void testQueryMultipleDataset() throws IOException {
        URL url = new URL(SERVER_URL +
                "?default-graph-uri=http%3A%2F%2Fkasei.us%2F2009%2F09%2Fsparql%2Fdata%2Fdata2.rdf");
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setDoOutput(true);
        urlConnection.setRequestMethod("POST");
        urlConnection.setRequestProperty("Content-Type", UNENCODED_CONTENT);
        OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream());
        out.write("ASK FROM <http://kasei.us/2009/09/sparql/data/data1.rdf> { <data1.rdf> ?p ?o }");
        out.close();
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
        List<String> validResponseContent = Arrays.asList(XML_CONTENT, JSON_CONTENT);
        assertTrue(validResponseContent.contains(urlConnection.getContentType()));
        if (urlConnection.getContentType().equals(XML_CONTENT)) {
            checkXMLResponseContent(urlConnection, true);
        }
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
        urlConnection.setRequestProperty("Content-Type", UNENCODED_CONTENT);
        OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream());
        out.write("SELECT (1 AS ?value) {}");
        out.close();
        assertEquals(HttpURLConnection.HTTP_OK, urlConnection.getResponseCode());
        List<String> validResponseContent = Arrays.asList(XML_CONTENT, JSON_CONTENT, TSV_CONTENT, CSV_CONTENT);
        assertTrue(validResponseContent.contains(urlConnection.getContentType()));
    }

    /**
     * Help method for checking response content, only boolean value is expected (result of ASK query)
     *
     * @param urlConnection
     * @param expected
     */
    private void checkXMLResponseContent(HttpURLConnection urlConnection, Boolean expected) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(urlConnection.getInputStream());
            XPathFactory xPathfactory = XPathFactory.newInstance();
            XPath xpath = xPathfactory.newXPath();
            XPathExpression expr = xpath.compile("/sparql/boolean");
            Boolean result = Boolean.valueOf(expr.evaluate(doc));
            assertEquals(expected, result);
        } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
            e.printStackTrace();
        }
    }
}
