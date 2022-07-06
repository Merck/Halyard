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
package com.msd.gin.halyard.tools;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.helpers.NTriplesUtil;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.json.JSONObject;

import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.common.Keyspace;
import com.msd.gin.halyard.common.KeyspaceConnection;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.SSLSettings;
import com.msd.gin.halyard.common.StatementIndex;

/**
 * MapReduce tool indexing all RDF literals in Elasticsearch
 * @author Adam Sotona (MSD)
 */
public final class HalyardElasticIndexer extends AbstractHalyardTool {
	private static final String TOOL_NAME = "esindex";

	private static final String INDEX_URL_PROPERTY = confProperty(TOOL_NAME, "index.url");
	private static final String CREATE_INDEX_PROPERTY = confProperty(TOOL_NAME, "index.create");
	private static final String NAMED_GRAPH_PROPERTY = confProperty(TOOL_NAME, "named-graph");
	private static final String ALIAS_PROPERTY = confProperty(TOOL_NAME, "alias");

	enum Counters {
		INDEXED_LITERALS
	}

    static final class IndexerMapper extends RdfTableMapper<NullWritable, Text>  {

        final Text outputJson = new Text();
        String source;
        int objectKeySize;
        int contextKeySize;
        long counter = 0, exports = 0, statements = 0;
        byte[] lastHash;
        Set<Literal> literals;

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            openKeyspace(conf, conf.get(SOURCE_NAME_PROPERTY), conf.get(SNAPSHOT_PATH_PROPERTY));
            objectKeySize = rdfFactory.getObjectRole().keyHashSize();
            contextKeySize = rdfFactory.getContextRole().keyHashSize();
            lastHash = new byte[objectKeySize];
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context output) throws IOException, InterruptedException {
            if ((counter++ % 100000) == 0) {
                output.setStatus(MessageFormat.format("{0} st:{1} exp:{2} ", counter, statements, exports));
            }

            byte[] hash = new byte[objectKeySize];
            System.arraycopy(key.get(), key.getOffset() + 1 + (StatementIndex.toIndex(key.get()[key.getOffset()], rdfFactory).isQuadIndex() ? contextKeySize : 0), hash, 0, objectKeySize);
            if (!Arrays.equals(hash, lastHash)) {
            	literals = new HashSet<>();
            	lastHash = hash;
            }

            for (Statement st : HalyardTableUtils.parseStatements(null, null, null, null, value, valueReader, rdfFactory)) {
                statements++;
            	Literal l = (Literal) st.getObject();
                if (literals.add(l)) {
            		try(StringBuilderWriter json = new StringBuilderWriter(128)) {
		                json.append("{\"id\":");
		                String id = rdfFactory.id(l).toString();
		                JSONObject.quote(id, json);
		                json.append(",\"label\":");
		                JSONObject.quote(l.getLabel(), json);
		                json.append(",\"datatype\":");
		                JSONObject.quote(l.getDatatype().stringValue(), json);
		                if(l.getLanguage().isPresent()) {
			                json.append(",\"lang\":");
			                JSONObject.quote(l.getLanguage().get(), json);
		                }
		                json.append("}\n");
		                outputJson.set(json.toString());
		                output.write(NullWritable.get(), outputJson);
            		}
	                exports++;
                }
            }
        }

        @Override
        protected void cleanup(Context output) throws IOException {
        	closeKeyspace();
        	output.getCounter(Counters.INDEXED_LITERALS).setValue(exports);
        }
    }

    public HalyardElasticIndexer() {
        super(
            TOOL_NAME,
            "Halyard ElasticSearch Index is a MapReduce application that indexes all literals in the given dataset into a supplementary ElasticSearch server/cluster. "
                + "A Halyard repository configured with such supplementary ElasticSearch index can then provide more advanced text search features over the indexed literals.",
            "Default index configuration is:\n"
            + getMappingConfig("\u00A0", "2*<num_of_region_servers>", "1", null)
            + "Example: halyard esindex -s my_dataset -t http://my_elastic.my.org:9200/my_index"
        );
        addOption("s", "source-dataset", "dataset_table", SOURCE_NAME_PROPERTY, "Source HBase table with Halyard RDF store", true, true);
        addOption("t", "target-index", "target_url", INDEX_URL_PROPERTY, "Elasticsearch target index url <server>:<port>/<index_name>", true, true);
        addOption("c", "create-index", null, CREATE_INDEX_PROPERTY, "Optionally create Elasticsearch index", false, true);
        addOption("g", "named-graph", "named_graph", NAMED_GRAPH_PROPERTY, "Optionally restrict indexing to the given named graph only", false, true);
        addOption("u", "restore-dir", "restore_folder", SNAPSHOT_PATH_PROPERTY, "If specified then -s is a snapshot name and this is the restore folder on HDFS", false, true);
        addOption("a", "alias", "alias", ALIAS_PROPERTY, "If creating an index, optionally add it to an alias", false, true);
    }

    private static String getMappingConfig(String linePrefix, String shards, String replicas, String alias) {
        String config =
                  linePrefix + "{\n"
                + linePrefix + "    \"mappings\" : {\n"
                + linePrefix + "        \"properties\" : {\n"
                + linePrefix + "            \"id\" : { \"type\" : \"keyword\", \"index\" : false },\n"
                + linePrefix + "            \"label\" : { \"type\" : \"text\" },\n"
                + linePrefix + "            \"datatype\" : { \"type\" : \"keyword\", \"index\" : false },\n"
                + linePrefix + "            \"lang\" : { \"type\" : \"keyword\", \"index\" : false }\n"
                + linePrefix + "        }\n"
                + linePrefix + "    },\n";
    if (alias != null) {
        config += linePrefix + "    \"aliases\" : {\n"
                + linePrefix + "         \""+alias+"\" : {}\n"
                + linePrefix + "    },\n";
    }
        config += linePrefix + "   \"settings\": {\n"
                + linePrefix + "       \"index.query.default_field\": \"label\",\n"
                + linePrefix + "       \"refresh_interval\": \"1h\",\n"
                + linePrefix + "       \"number_of_shards\": " + shards + ",\n"
                + linePrefix + "       \"number_of_replicas\": " + replicas + "\n"
                + linePrefix + "    }\n"
                + linePrefix + "}\n";
        return config;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        configureString(cmd, 's', null);
        configureString(cmd, 't', null);
        configureString(cmd, 'u', null);
        configureBoolean(cmd, 'c');
        configureString(cmd, 'a', null);
        configureString(cmd, 'g', null);
        String source = getConf().get(SOURCE_NAME_PROPERTY);
        String target = getConf().get(INDEX_URL_PROPERTY);
        URL targetUrl = new URL(target);
        boolean createIndex = getConf().getBoolean(CREATE_INDEX_PROPERTY, false);
        String snapshotPath = getConf().get(SNAPSHOT_PATH_PROPERTY);
        if (snapshotPath != null) {
			FileSystem fs = CommonFSUtils.getRootDirFileSystem(getConf());
        	if (fs.exists(new Path(snapshotPath))) {
        		throw new IOException("Snapshot restore directory already exists");
        	}
        }
        String namedGraph = getConf().get(NAMED_GRAPH_PROPERTY);

        getConf().set("es.nodes", targetUrl.getHost()+":"+targetUrl.getPort());
        getConf().set("es.resource", targetUrl.getPath());
        getConf().set("es.mapping.id", "id");
        getConf().set("es.input.json", "yes");
        getConf().setIfUnset("es.batch.size.bytes", Integer.toString(5*1024*1024));
        getConf().setIfUnset("es.batch.size.entries", Integer.toString(10000));

        TableMapReduceUtil.addDependencyJarsForClasses(getConf(),
               NTriplesUtil.class,
               Rio.class,
               AbstractRDFHandler.class,
               RDFFormat.class,
               RDFParser.class,
               Table.class,
               HBaseConfiguration.class,
               AuthenticationProtos.class);
        if (System.getProperty("exclude.es-hadoop") == null) {
        	TableMapReduceUtil.addDependencyJarsForClasses(getConf(), EsOutputFormat.class);
        }
        HBaseConfiguration.addHbaseResources(getConf());
        Job job = Job.getInstance(getConf(), "HalyardElasticIndexer " + source + " -> " + target);
        if (createIndex) {
            int shards;
            int replicas = 1;
            try (Connection conn = ConnectionFactory.createConnection(getConf())) {
                try (Admin admin = conn.getAdmin()) {
                    shards = 2 * admin.getRegionServers().size(); // 2 shards per node to allow for growth
                }
            }
            HttpURLConnection http = (HttpURLConnection)targetUrl.openConnection();
            if (http instanceof HttpsURLConnection) {
            	configureSSL((HttpsURLConnection) http);
            }
            http.setRequestMethod("PUT");
            http.setDoOutput(true);
            http.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
            String esUser = getConf().get("es.net.http.auth.user");
            if (esUser != null) {
            	String esPassword = getConf().get("es.net.http.auth.pass");
            	String userPass = esUser + ':' + esPassword;
            	String basicAuth = "Basic " + Base64.getEncoder().encodeToString(userPass.getBytes(StandardCharsets.UTF_8));
            	http.setRequestProperty("Authorization", basicAuth);
            }
            String alias = getConf().get(ALIAS_PROPERTY);
            byte b[] = Bytes.toBytes(getMappingConfig("", Integer.toString(shards), Integer.toString(replicas), alias));
            http.setFixedLengthStreamingMode(b.length);
            http.connect();
            try {
                try (OutputStream post = http.getOutputStream()) {
                    post.write(b);
                }
                int response = http.getResponseCode();
                String msg = http.getResponseMessage();
                if (response == 200) {
                    LOG.info("Elastic index succesfully configured.");
                } else {
                    InputStream errStream = http.getErrorStream();
                    String resp = (errStream != null) ? IOUtils.toString(errStream, StandardCharsets.UTF_8) : "";
                    LOG.warn("Elastic index responded with {}: {}", response, resp);
                    boolean alreadyExist = false;
                    if (response == 400) try {
                        alreadyExist = new JSONObject(resp).getJSONObject("error").getString("type").contains("exists");
                    } catch (Exception ex) {
                        //ignore
                    }
                    if (!alreadyExist) throw new IOException(msg);
                }
            } finally {
                http.disconnect();
            }
        }
        job.setJarByClass(HalyardElasticIndexer.class);
        TableMapReduceUtil.initCredentials(job);

        RDFFactory rdfFactory;
        Keyspace keyspace = HalyardTableUtils.getKeyspace(getConf(), source, snapshotPath);
        try {
        	try (KeyspaceConnection kc = keyspace.getConnection()) {
        		rdfFactory = RDFFactory.create(kc);
        	}
		} finally {
			keyspace.close();
		}
        Scan scan;
        if (namedGraph != null) {
            //scan only given named graph from COSP literal region(s)
        	Resource graph = NTriplesUtil.parseResource(namedGraph, SimpleValueFactory.getInstance());
        	scan = StatementIndex.scanLiterals(graph, rdfFactory);
        } else {
            //scan OSP literal region(s)
        	scan = StatementIndex.scanLiterals(rdfFactory);
        }
        keyspace.initMapperJob(
            scan,
            IndexerMapper.class,
            NullWritable.class,
            Text.class,
            job);
        job.setOutputFormatClass(EsOutputFormat.class);
        job.setNumReduceTasks(0);
        job.setSpeculativeExecution(false);
	    try {
	        if (job.waitForCompletion(true)) {
	            HttpURLConnection http = (HttpURLConnection)new URL(target + "_refresh").openConnection();
	            if (http instanceof HttpsURLConnection) {
	            	configureSSL((HttpsURLConnection) http);
	            }
	            http.connect();
	            http.disconnect();
	            LOG.info("Elastic indexing completed.");
	            return 0;
	        } else {
	    		LOG.error("Elastic indexing failed to complete.");
	            return -1;
	        }
        } finally {
        	keyspace.destroy();
        }
    }

    private void configureSSL(HttpsURLConnection https) throws IOException, GeneralSecurityException {
    	String keyLoc = getConf().get("es.net.ssl.keystore.location");
    	String keyPass = getConf().get("es.net.ssl.keystore.pass");
    	String trustLoc = getConf().get("es.net.ssl.truststore.location");
    	String trustPass = getConf().get("es.net.ssl.truststore.pass");
    	SSLSettings sslSettings = new SSLSettings();
    	sslSettings.keyStoreLocation = (keyLoc != null && !keyLoc.isEmpty()) ? new URL(keyLoc) : null;
    	sslSettings.keyStoreType = getConf().get("es.net.ssl.keystore.type", "jks");
    	sslSettings.keyStorePassword = (keyPass != null && !keyPass.isEmpty()) ? keyPass.toCharArray() : null;
    	sslSettings.trustStoreLocation = (trustLoc != null && !trustLoc.isEmpty()) ? new URL(trustLoc) : null;
    	sslSettings.trustStorePassword = (trustPass != null && !trustPass.isEmpty()) ? trustPass.toCharArray() : null;
    	sslSettings.sslProtocol = getConf().get("es.net.ssl.protocol", "TLS");
    	SSLContext sslContext = sslSettings.createSSLContext();
    	https.setSSLSocketFactory(sslContext.getSocketFactory());
    }
}
