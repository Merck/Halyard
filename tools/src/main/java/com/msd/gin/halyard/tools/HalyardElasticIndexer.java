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

import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.common.Keyspace;
import com.msd.gin.halyard.common.KeyspaceConnection;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.StatementIndex;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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

/**
 * MapReduce tool indexing all RDF literals in Elasticsearch
 * @author Adam Sotona (MSD)
 */
public final class HalyardElasticIndexer extends AbstractHalyardTool {
	private static final String TOOL_NAME = "esindex";

	private static final String NAMED_GRAPH_PROPERTY = confProperty(TOOL_NAME, "namedGraph");
	private static final String MULTITABLE_PROPERTY = confProperty(TOOL_NAME, "multitable");

    static final class IndexerMapper extends RdfTableMapper<NullWritable, Text>  {

        final Text outputJson = new Text();
        String source;
        boolean isMultitable;
        int objectKeySize;
        int contextKeySize;
        long counter = 0, exports = 0, statements = 0;
        byte[] lastHash;
        Set<Literal> literals;

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            source =  conf.get(SOURCE_NAME_PROPERTY);
            isMultitable = conf.getBoolean(MULTITABLE_PROPERTY, false);
            openKeyspace(conf, source, conf.get(SNAPSHOT_PATH_PROPERTY));
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
		                if (isMultitable) {
		                	id = source + ':' + id;
		                }
		                JSONObject.quote(id, json);
		                json.append(",\"label\":");
		                JSONObject.quote(l.getLabel(), json);
		                if(l.getLanguage().isPresent()) {
			                json.append(",\"lang\":");
			                JSONObject.quote(l.getLanguage().get(), json);
		                } else {
			                json.append(",\"datatype\":");
			                JSONObject.quote(l.getDatatype().stringValue(), json);
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
        }
    }

    public HalyardElasticIndexer() {
        super(
            TOOL_NAME,
            "Halyard ElasticSearch Index is a MapReduce application that indexes all literals in the given dataset into a supplementary ElasticSearch server/cluster. "
                + "A Halyard repository configured with such supplementary ElasticSearch index can then provide more advanced text search features over the indexed literals.",
            "Default index configuration is:\n"
            + getMappingConfig("\u00A0", "2*<num_of_region_servers>", "1", "<table_name>")
            + "Example: halyard esindex -s my_dataset -t http://my_elastic.my.org:9200/my_index"
        );
        addOption("s", "source-dataset", "dataset_table", SOURCE_NAME_PROPERTY, "Source HBase table with Halyard RDF store", true, true);
        addOption("t", "target-index", "target_url", "Elasticsearch target index url <server>:<port>/<index_name>", true, true);
        addOption("c", "create-index", null, "Optionally create Elasticsearch index", false, true);
        addOption("g", "named-graph", "named_graph", NAMED_GRAPH_PROPERTY, "Optionally restrict indexing to the given named graph only", false, true);
        addOption("u", "restore-dir", "restore_folder", SNAPSHOT_PATH_PROPERTY, "If specified then -s is a snapshot name and this is the restore folder on HDFS", false, true);
        addOption("m", "multitable-index", null, MULTITABLE_PROPERTY, "Create a multitable index", false, true);
    }

    private static String getMappingConfig(String linePrefix, String shards, String replicas, String tableName) {
        String config =
                  linePrefix + "{\n"
                + linePrefix + "    \"mappings\" : {\n"
                + linePrefix + "        \"properties\" : {\n"
                + linePrefix + "            \"id\" : { \"type\" : \"keyword\", \"index\" : false },\n"
                + linePrefix + "            \"label\" : { \"type\" : \"text\" },\n"
                + linePrefix + "            \"datatype\" : { \"type\" : \"keyword\", \"index\" : false },\n"
                + linePrefix + "            \"lang\" : { \"type\" : \"keyword\", \"index\" : false },\n";
        if (tableName != null) {
        config += linePrefix + "            \"table\" : { \"type\" : \"constant_keyword\", \"value\" : \"" + tableName + "\" }\n";
        } else {
        config += linePrefix + "            \"table\" : { \"type\" : \"keyword\", \"index\" : false }\n";
        }
        config += linePrefix + "        }\n"
                + linePrefix + "    },\n"
                + linePrefix + "   \"settings\": {\n"
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
        configureString(cmd, 'u', null);
        configureBoolean(cmd, 'm');
        configureString(cmd, 'g', null);
        String source = getConf().get(SOURCE_NAME_PROPERTY);
        String target = cmd.getOptionValue('t');
        URL targetUrl = new URL(target);
        String snapshotPath = getConf().get(SNAPSHOT_PATH_PROPERTY);
        if (snapshotPath != null) {
			FileSystem fs = CommonFSUtils.getRootDirFileSystem(getConf());
        	if (fs.exists(new Path(snapshotPath))) {
        		throw new IOException("Snapshot restore directory already exists");
        	}
        }
        String namedGraph = getConf().get(NAMED_GRAPH_PROPERTY);
        boolean isMultitable = getConf().getBoolean(MULTITABLE_PROPERTY, false);
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
        if (cmd.hasOption('c')) {
            int shards;
            int replicas = 1;
            try (Connection conn = ConnectionFactory.createConnection(getConf())) {
                try (Admin admin = conn.getAdmin()) {
                    shards = 2 * admin.getRegionServers().size(); // 2 shards per node to allow for growth
                }
            }
            HttpURLConnection http = (HttpURLConnection)targetUrl.openConnection();
            http.setRequestMethod("PUT");
            http.setDoOutput(true);
            http.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
            byte b[] = Bytes.toBytes(getMappingConfig("", Integer.toString(shards), Integer.toString(replicas), isMultitable ? null : source));
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
                    String resp = IOUtils.toString(http.getErrorStream(), StandardCharsets.UTF_8);
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
        job.getConfiguration().setBoolean("mapreduce.map.speculative", false);    
        job.getConfiguration().setBoolean("mapreduce.reduce.speculative", false); 
        job.getConfiguration().set("es.nodes", targetUrl.getHost()+":"+targetUrl.getPort());
        job.getConfiguration().set("es.resource", targetUrl.getPath());
        job.getConfiguration().set("es.mapping.id", "id");
        job.getConfiguration().setInt("es.batch.size.bytes", 5*1024*1024);
        job.getConfiguration().setInt("es.batch.size.entries", 10000);
        job.getConfiguration().set("es.input.json", "yes");
        job.setOutputFormatClass(EsOutputFormat.class);
        job.setNumReduceTasks(0);
	    try {
	        if (job.waitForCompletion(true)) {
	            HttpURLConnection http = (HttpURLConnection)new URL(target + "_refresh").openConnection();
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
}
