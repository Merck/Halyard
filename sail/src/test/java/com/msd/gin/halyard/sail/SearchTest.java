/*
 * Copyright 2016 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
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
package com.msd.gin.halyard.sail;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.vocab.HALYARD;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class SearchTest {

	private static final String INDEX = "myIndex";
	private Configuration conf;

	@Before
	public void setup() throws Exception {
		conf = HBaseServerTestInstance.getInstanceConfig();
	}

	private Repository createRepo(String tableName, ServerSocket esServer) throws Exception {
		HBaseSail hbaseSail = new HBaseSail(conf, tableName, true, 0, true, 0, new URL("http", InetAddress.getLoopbackAddress().getHostAddress(), esServer.getLocalPort(), "/" + INDEX), null);
		Repository hbaseRepo = new SailRepository(hbaseSail);
		hbaseRepo.init();
		return hbaseRepo;
    }

    @Test
    public void statementLiteralSearchTest() throws Exception {
		ValueFactory vf = SimpleValueFactory.getInstance();
		Literal val1 = vf.createLiteral("Whatever Text");
		Literal val2 = vf.createLiteral("Whatever Text", "en");
		Literal val3 = vf.createLiteral("Que sea", "es");
		try (ServerSocket server = startElasticsearch(val1, val2)) {
			IRI whatever = vf.createIRI("http://whatever");
			Repository hbaseRepo = createRepo("testSimpleLiteralSearch", server);
			try (RepositoryConnection conn = hbaseRepo.getConnection()) {
				conn.add(whatever, whatever, val1);
				conn.add(whatever, whatever, val2);
				conn.add(whatever, whatever, val3);
				try (RepositoryResult<Statement> iter = conn.getStatements(null, null, vf.createLiteral("what", HALYARD.SEARCH))) {
					assertTrue(iter.hasNext());
					iter.next();
					assertTrue(iter.hasNext());
					iter.next();
					assertFalse(iter.hasNext());
				}
			}
			hbaseRepo.shutDown();
		}
	}

	@Test
	public void advancedSearchTest() throws Exception {
		ValueFactory vf = SimpleValueFactory.getInstance();
		Literal val1 = vf.createLiteral("Whatever Text");
		Literal val2 = vf.createLiteral("Whatever Text", "en");
		Literal val3 = vf.createLiteral("Que sea", "es");
		try (ServerSocket server = startElasticsearch(val1, val2)) {
			IRI whatever = vf.createIRI("http://whatever");
			Repository hbaseRepo = createRepo("testAdvancedLiteralSearch", server);
			try (RepositoryConnection conn = hbaseRepo.getConnection()) {
				conn.add(whatever, whatever, val1);
				conn.add(whatever, whatever, val2);
				conn.add(whatever, whatever, val3);
				TupleQuery q = conn.prepareTupleQuery(
						"PREFIX halyard: <http://merck.github.io/Halyard/ns#> select * { [] a halyard:Query; halyard:query 'what'; halyard:limit 5; halyard:matches [rdf:value ?v; halyard:score ?score; halyard:index ?index ] }");
				try (TupleQueryResult iter = q.evaluate()) {
					assertTrue(iter.hasNext());
					BindingSet bs = iter.next();
					assertEquals(2, ((Literal) bs.getBinding("score").getValue()).intValue());
					assertEquals(INDEX, ((Literal) bs.getBinding("index").getValue()).stringValue());
					assertTrue(iter.hasNext());
					bs = iter.next();
					assertEquals(1, ((Literal) bs.getBinding("score").getValue()).intValue());
					assertEquals(INDEX, ((Literal) bs.getBinding("index").getValue()).stringValue());
					assertFalse(iter.hasNext());
				}
			}
			hbaseRepo.shutDown();
		}
	}

	private ServerSocket startElasticsearch(Literal... values) throws IOException {
		RDFFactory rdfFactory = RDFFactory.create(conf);
		StringWriter jsonBuf = new StringWriter();
		JsonGenerator jsonGen = new JsonFactory().createGenerator(jsonBuf);
		jsonGen.writeStartObject();
		jsonGen.writeNumberField("took", 34);
		jsonGen.writeBooleanField("timed_out", false);
		jsonGen.writeObjectFieldStart("_shards");
		jsonGen.writeNumberField("total", 5);
		jsonGen.writeNumberField("successful", 5);
		jsonGen.writeNumberField("skipped", 0);
		jsonGen.writeNumberField("failed", 0);
		jsonGen.writeEndObject();
		jsonGen.writeObjectFieldStart("hits");
		jsonGen.writeArrayFieldStart("hits");
		for (int i = 0; i < values.length; i++) {
			Literal val = values[i];
			jsonGen.writeStartObject();
			jsonGen.writeStringField("_index", INDEX);
			jsonGen.writeStringField("_id", rdfFactory.id(val).toString());
			jsonGen.writeNumberField("_score", values.length - i);
			jsonGen.writeObjectFieldStart("_source");
			jsonGen.writeStringField("label", val.getLabel());
			jsonGen.writeStringField("datatype", val.getDatatype().stringValue());
			if (val.getLanguage().isPresent()) {
				jsonGen.writeStringField("lang", val.getLanguage().get());
			}
			jsonGen.writeEndObject();
			jsonGen.writeEndObject();
		}
		jsonGen.writeEndArray();
		jsonGen.writeEndObject();
		jsonGen.writeEndObject();
		jsonGen.close();
		String json = jsonBuf.toString();
		final String response = "HTTP/1.1 200 OK\ncontent-type: application/json; charset=UTF-8\ncontent-length: " + json.length() + "\nX-elastic-product: Elasticsearch\n\r\n" + json;
		final ServerSocket server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
		Thread t = new Thread(() -> {
			try (Socket s = server.accept()) {
				try (BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), "UTF-8"))) {
					String line;
					int length = 0;
					while ((line = in.readLine()) != null && line.length() > 0) {
						if (line.startsWith("Content-Length:")) {
							length = Integer.parseInt(line.substring(15).trim());
						}
						System.out.println(line);
					}
					char b[] = new char[length];
					in.read(b);
					System.out.println(b);
					try (OutputStream out = s.getOutputStream()) {
						IOUtils.write(response, out, StandardCharsets.UTF_8);
					}
				}
			} catch (IOException ex) {
				LoggerFactory.getLogger(SearchTest.class).error("Error reading from socket", ex);
			}
		});
		t.setDaemon(true);
		t.start();
		return server;
    }
}
