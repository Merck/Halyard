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

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.sail.HBaseSail;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.SailConnection;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardElasticIndexerTest extends AbstractHalyardToolTest {
	private static final String ES_VERSION = "7.17.0";
	private static final String NODE_ID = UUID.randomUUID().toString();
	private static final String INDEX_NAME = "my_index";
	private static final String INDEX_PATH = "/"+INDEX_NAME;

	@Override
	protected AbstractHalyardTool createTool() {
		return new HalyardElasticIndexer();
	}

	@Test
    public void testElasticIndexer() throws Exception {
		Configuration conf = HBaseServerTestInstance.getInstanceConfig();
        HBaseSail sail = new HBaseSail(conf, "elasticTable", true, 0, true, 0, null, null);
        sail.initialize();
        ValueFactory vf = SimpleValueFactory.getInstance();
		try (SailConnection conn = sail.getConnection()) {
			for (int i = 0; i < 100; i++) {
				conn.addStatement(vf.createIRI("http://whatever/NTsubj"), vf.createIRI("http://whatever/NTpred" + i), vf.createLiteral("whatever NT value " + i), (i % 4 == 0) ? null : vf.createIRI("http://whatever/graph#" + (i % 4)));
			}
			// add some non-literal data
			for (int i = 0; i < 100; i++) {
				conn.addStatement(vf.createIRI("http://whatever/NTsubj"), vf.createIRI("http://whatever/NTpred" + i), vf.createIRI("http://whatever/NTobj" + i), (i % 4 == 0) ? null : vf.createIRI("http://whatever/graph#" + (i % 4)));
			}
		}
		RDFFactory rdfFactory = sail.getRDFFactory();
        testElasticIndexer(false, vf, rdfFactory);
        testElasticIndexer(true, vf, rdfFactory);
    }

    public void testElasticIndexer(boolean namedGraphOnly, ValueFactory vf, RDFFactory rdfFactory) throws Exception {
        final String[] requestUri = new String[2];
        final JSONObject[] createRequest = new JSONObject[1];
        final List<String> bulkBody = new ArrayList<>(200);

        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/", new HttpHandler() {
            @Override
            public void handle(HttpExchange he) throws IOException {
            	String path = he.getRequestURI().getPath();
                if ("GET".equalsIgnoreCase(he.getRequestMethod())) {
                	JSONObject response;
                	switch (path) {
                		case "/":
		                    JSONObject version = new JSONObject();
		                    version.put("number", ES_VERSION);
		                    version.put("lucene_version", "8.11.1");
		                    version.put("minimum_wire_compatibility_version", "6.8.0");
		                    version.put("minimum_index_compatibiltiy_version", "6.0.0-beta1");
		                    response = new JSONObject();
		                    response.put("name", "localhost");
		                    response.put("cluster_name", "halyard-test");
		                    response.put("cluster_uuid", "_na_");
		                    response.put("version", version);
		                    response.put("tagline", "You Know, for Search");
		                    response.put("build_flavor", "default");
		                    he.getResponseHeaders().set("X-elastic-product", "Elasticsearch");
		                    break;
                		case "/_nodes/http":
                			JSONObject nodeInfo = new JSONObject();
                			nodeInfo.put("version", ES_VERSION);
                			nodeInfo.put("name", "test-es-node");
                			nodeInfo.put("host", "localhost");
                			nodeInfo.put("ip", server.getAddress().getAddress().getHostAddress());
                			nodeInfo.put("roles", Arrays.asList("master", "data", "ingest"));
                			JSONObject httpNode = new JSONObject();
                			httpNode.put("publish_address", server.getAddress().toString());
                			nodeInfo.put("http", httpNode);
                			JSONObject node = new JSONObject();
                			node.put(NODE_ID, nodeInfo);
		                    response = new JSONObject();
                			response.put("nodes", node);
                			break;
                		default:
                			response = null;
                	}
                	if (response != null) {
                		he.sendResponseHeaders(200, 0);
	                    try (OutputStreamWriter writer = new OutputStreamWriter(he.getResponseBody(), StandardCharsets.UTF_8)) {
	                        response.write(writer);
	                    }
                	} else {
                		he.sendResponseHeaders(404, 0);
                	}
                }
            }
        });
        server.createContext(INDEX_PATH, new HttpHandler() {
            @Override
            public void handle(HttpExchange he) throws IOException {
            	String subpath = he.getRequestURI().getPath().substring(INDEX_PATH.length());
                if ("PUT".equalsIgnoreCase(he.getRequestMethod())) {
                	JSONObject response;
                	switch (subpath) {
                		case "":
                            requestUri[0] = he.getRequestURI().getPath();
                            try (InputStream in  = he.getRequestBody()) {
                                createRequest[0] = new JSONObject(IOUtils.toString(in, StandardCharsets.UTF_8));
                            }
                            response = new JSONObject();
                            response.put("acknowledged", true);
                            response.put("shards_acknowledged", true);
                            response.put("index", INDEX_NAME);
                			break;
                		case "/_bulk":
		                    requestUri[1] = he.getRequestURI().getPath();
		                    try (BufferedReader br = new BufferedReader(new InputStreamReader(he.getRequestBody(), StandardCharsets.UTF_8))) {
		                        String line;
		                        while ((line = br.readLine()) != null) {
		                        	if (!line.isEmpty()) {
		                        		bulkBody.add(line);
		                        	}
		                        }
		                    }
		                    response = new JSONObject();
		                    response.put("took", 17);
		                    response.put("errors", false);
		                    response.put("items", Arrays.asList());
		                    break;
                		default:
                			response = null;
                	}
                	if (response != null) {
                		he.sendResponseHeaders(200, 0);
	                    try (OutputStreamWriter writer = new OutputStreamWriter(he.getResponseBody(), StandardCharsets.UTF_8)) {
	                        response.write(writer);
	                    }
                	} else {
                		he.sendResponseHeaders(404, 0);
                	}
                } else if ("POST".equalsIgnoreCase(he.getRequestMethod())) {
                	JSONObject response;
                	switch (subpath) {
                		case "/_refresh":
                			JSONObject shards = new JSONObject();
                			shards.put("total", 1);
                			shards.put("successful", 1);
                			shards.put("failed", 0);
                			response = new JSONObject();
                			response.put("_shards", shards);
                			break;
                		default:
                			response = null;
                	}
                	if (response != null) {
                		he.sendResponseHeaders(200, 0);
	                    try (OutputStreamWriter writer = new OutputStreamWriter(he.getResponseBody(), StandardCharsets.UTF_8)) {
	                        response.write(writer);
	                    }
                	} else {
                		he.sendResponseHeaders(404, 0);
                	}
                } else if ("HEAD".equalsIgnoreCase(he.getRequestMethod())) {
                	he.sendResponseHeaders(200, -1);
                } else if ("GET".equalsIgnoreCase(he.getRequestMethod())) {
                	JSONObject response;
                	switch (subpath) {
                		case "/_search_shards":
                			JSONObject shard = new JSONObject();
                			shard.put("shard", 0);
                			shard.put("state", "STARTED");
                			shard.put("primary", true);
                			shard.put("index", INDEX_NAME);
                			shard.put("node", NODE_ID);
                			response = new JSONObject();
                			response.put("shards", Arrays.asList(Arrays.asList(shard)));
                			break;
                		default:
                			response = null;
                	}
                	if (response != null) {
                		he.sendResponseHeaders(200, 0);
	                    try (OutputStreamWriter writer = new OutputStreamWriter(he.getResponseBody(), StandardCharsets.UTF_8)) {
	                        response.write(writer);
	                    }
                	} else {
                		he.sendResponseHeaders(404, 0);
                	}
                }
            }
        });
        server.start();
        try {
            // fix elasticsearch classpath issues
            System.setProperty("exclude.es-hadoop", "true");
            int serverPort = server.getAddress().getPort();
            String indexUrl = "http://localhost:" + serverPort + INDEX_PATH;
            String[] cmdLineArgs = namedGraphOnly ? new String[]{"-s", "elasticTable", "-t", indexUrl, "-c", "-g", "<http://whatever/graph#1>"}
            : new String[]{"-s", "elasticTable", "-t", indexUrl, "-c"};
            int rc = run(cmdLineArgs);
            assertEquals(0, rc);
        } finally {
            server.stop(0);
        }
        assertEquals(INDEX_PATH, requestUri[0]);
        JSONObject mappingProps = createRequest[0].getJSONObject("mappings").getJSONObject("properties");
        assertNotNull(createRequest[0].toString(), mappingProps.getJSONObject("label"));
        assertEquals(INDEX_PATH+"/_bulk", requestUri[1]);
        assertEquals(bulkBody.toString(), (namedGraphOnly ? 50 : 200), bulkBody.size());
        for (int i=0; i<bulkBody.size(); i+=2) {
            String id = new JSONObject(bulkBody.get(i)).getJSONObject("index").getString("_id");
            JSONObject fields = new JSONObject(bulkBody.get(i+1));
            Literal literal = vf.createLiteral(fields.getString("label"), vf.createIRI(fields.getString("datatype")));
            assertEquals("Invalid hash for literal " + literal, rdfFactory.id(literal).toString(), id);
        }
    }
}
