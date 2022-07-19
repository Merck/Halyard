package com.msd.gin.halyard.sail;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.sail.search.SearchDocument;

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
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.junit.Before;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public abstract class AbstractSearchTest {

	protected static final String INDEX = "myIndex";
	protected Configuration conf;
	private RDFFactory rdfFactory;

	@Before
	public void setup() throws Exception {
		conf = HBaseServerTestInstance.getInstanceConfig();
		rdfFactory = RDFFactory.create(conf);
	}

	protected final Repository createRepo(String tableName, ServerSocket esServer) throws Exception {
		HBaseSail hbaseSail = new HBaseSail(conf, tableName, true, 0, true, 10, ElasticSettings.from(new URL("http", InetAddress.getLoopbackAddress().getHostAddress(), esServer.getLocalPort(), "/" + INDEX)), null);
		Repository hbaseRepo = new SailRepository(hbaseSail);
		hbaseRepo.init();
		return hbaseRepo;
	}

	protected final ServerSocket startElasticsearch(Literal... response) throws IOException, InterruptedException {
		return startElasticsearch(Collections.singletonList(response));
	}

	protected final ServerSocket startElasticsearch(List<Literal[]> responses) throws IOException, InterruptedException {
		final ServerSocket server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
		Thread t = new Thread(() -> {
			int requestIdx = 0;
			while (!server.isClosed()) {
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

						if (requestIdx < responses.size()) {
							String response = createResponse(responses.get(requestIdx++));
							try (OutputStream out = s.getOutputStream()) {
								IOUtils.write(response, out, StandardCharsets.UTF_8);
							}
						} else {
							fail("Unexpected request");
						}
					}
				} catch (IOException ex) {
					if (!server.isClosed()) {
						LoggerFactory.getLogger(getClass()).error("Error reading from socket", ex);
					} else {
						assertEquals("Expecting more requests", responses.size(), requestIdx);
					}
				}
			}
		});
		t.setDaemon(true);
		t.start();
		return server;
	}

	private String createResponse(Literal[] values) throws IOException {
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
			String id = rdfFactory.id(val).toString();
			jsonGen.writeStringField("_id", id);
			jsonGen.writeNumberField("_score", values.length - i);
			jsonGen.writeObjectFieldStart("_source");
			jsonGen.writeStringField(SearchDocument.ID_FIELD, id);
			jsonGen.writeStringField(SearchDocument.LABEL_FIELD, val.getLabel());
			jsonGen.writeStringField(SearchDocument.DATATYPE_FIELD, val.getDatatype().stringValue());
			if (val.getLanguage().isPresent()) {
				jsonGen.writeStringField(SearchDocument.LANG_FIELD, val.getLanguage().get());
			}
			jsonGen.writeEndObject();
			jsonGen.writeEndObject();
		}
		jsonGen.writeEndArray();
		jsonGen.writeEndObject();
		jsonGen.writeEndObject();
		jsonGen.close();
		String json = jsonBuf.toString();
		String response = "HTTP/1.1 200 OK\ncontent-type: application/json; charset=UTF-8\ncontent-length: " + json.length() + "\nX-elastic-product: Elasticsearch\n\r\n" + json;
		return response;
	}
}
