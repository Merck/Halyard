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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.htrace.Trace;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.json.JSONArray;
import org.json.JSONObject;

import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.common.RDFContext;
import com.msd.gin.halyard.common.RDFObject;
import com.msd.gin.halyard.common.RDFPredicate;
import com.msd.gin.halyard.common.RDFSubject;
import com.yammer.metrics.core.Gauge;

/**
 * MapReduce tool indexing all RDF literals in Elasticsearch
 * @author Adam Sotona (MSD)
 */
public final class HalyardElasticIndexer extends AbstractHalyardTool {

    private static final String SOURCE = "halyard.elastic.source";
    private static final String TARGET = "halyard.elastic.target";
    private static final String BUFFER_LIMIT = "halyard.elastic.buffer";

    static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    static final class IndexerMapper extends TableMapper<ImmutableBytesWritable, LongWritable>  {

        long counter = 0, exports = 0, batches = 0, statements = 0;
		byte[] lastHash;
        Set<Literal> literals = new HashSet<>();
        List<String> esNodes;
        ByteArrayOutputStream batch;
        Writer batchWriter;
        URL indexUrl;
        int bufferLimit;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            indexUrl = new URL(context.getConfiguration().get(TARGET));
            bufferLimit = context.getConfiguration().getInt(BUFFER_LIMIT, 100000);
            batch = new ByteArrayOutputStream(bufferLimit);
            batchWriter = new OutputStreamWriter(new GZIPOutputStream(batch), StandardCharsets.UTF_8);
            try(Reader in = new InputStreamReader(new URL(indexUrl.getProtocol(), indexUrl.getHost(), indexUrl.getPort(), "_nodes").openStream(), StandardCharsets.UTF_8)) {
            	JSONObject response = new JSONObject(in);
            	JSONArray nodes = response.getJSONArray("nodes");
            	esNodes = new ArrayList<>(nodes.length());
            	for(int i=0; i<nodes.length(); i++) {
            		JSONObject node = nodes.getJSONObject(i);
            		JSONArray roles = node.getJSONArray("roles");
            		for(int j=0; j<roles.length(); j++) {
            			if("data".equals(roles.getString(j))) {
                    		esNodes.add(node.getString("ip"));
                    		break;
            			}
            		}
            	}
            }
        }


        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context output) throws IOException, InterruptedException {
            if ((counter++ % 100000) == 0) {
                output.setStatus(MessageFormat.format("{0} st:{1} exp:{2} batch:{3} ", counter, statements, exports, batches));
            }
			byte[] hash = new byte[RDFObject.KEY_SIZE];
			System.arraycopy(key.get(), key.getOffset() + 1 + (key.get()[key.getOffset()] == HalyardTableUtils.OSP_PREFIX ? 0 : RDFContext.KEY_SIZE), hash, 0, RDFObject.KEY_SIZE);
            if (!Arrays.equals(hash, lastHash)) {
                export(false);
                lastHash = hash;
            }
            for (Statement st : HalyardTableUtils.parseStatements(null, null, null, null, value, SVF)) {
                statements++;
                if (st.getObject() instanceof Literal) {
                    literals.add((Literal) st.getObject());
                }
            }
        }

        private void export(boolean flush) throws IOException {
            if (literals.size() > 0) {
            	for(Literal l : literals) {
	                batchWriter.write("{\"index\":{\"_id\":\"");
	                batchWriter.write(Hex.encodeHex(HalyardTableUtils.id(l)));
	                batchWriter.write("\"}}\n{\"label\":");
	                batchWriter.write(JSONObject.quote(l.getLabel()));
	                batchWriter.write(",\"datatype\":");
	                batchWriter.write(JSONObject.quote(l.getDatatype().stringValue()));
	                batchWriter.write("}\n");
            	}
                literals = new HashSet<>();
                exports++;
            }
            if ((flush && batch.size() > 0) || batch.size() > bufferLimit) {
            	batchWriter.close();
            	String esNode = esNodes.get((int)(batches % esNodes.size()));
                HttpURLConnection http = (HttpURLConnection)new URL(indexUrl.getProtocol(), esNode, indexUrl.getPort(), "_bulk").openConnection();
                http.setRequestMethod("POST");
                http.setDoOutput(true);
                http.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
                http.setRequestProperty("Content-Encoding", "gzip");
                http.setFixedLengthStreamingMode(batch.size());
                http.connect();
                try {
                    try (OutputStream post = http.getOutputStream()) {
                        post.write(batch.toByteArray());
                    }
                    int response = http.getResponseCode();
                    String msg = http.getResponseMessage();
                    if (exports > 0 && response != 200) {
                        LOG.severe(IOUtils.toString(http.getErrorStream()));
                        throw new IOException(msg);
                    }
                    batches++;
                } finally {
                    http.disconnect();
                }
                batch = new ByteArrayOutputStream(bufferLimit);
                batchWriter = new OutputStreamWriter(new GZIPOutputStream(batch), StandardCharsets.UTF_8);
            }
        }

        @Override
        protected void cleanup(Context output) throws IOException, InterruptedException {
            export(true);
        }
    }

    public HalyardElasticIndexer() {
        super(
            "esindex",
            "Halyard ElasticSearch Index is a MapReduce application that indexes all literals in the given dataset into a supplementary ElasticSearch server/cluster. "
                + "A Halyard repository configured with such supplementary ElasticSearch index can then provide more advanced text search features over the indexed literals.",
            "Default index configuration is:\n"
            + "\u00A0{\n"
            + "\u00A0   \"mappings\" : {\n"
            + "\u00A0       \"properties\" : {\n"
            + "\u00A0           \"label\" : { \"type\" : \"text\" }\n"
            + "\u00A0           \"datatype\" : { \"type\" : \"keyword\" }\n"
            + "\u00A0        }\n"
            + "\u00A0    },\n"
            + "\u00A0   \"settings\": {\n"
            + "\u00A0       \"index.query.default_field\": \"label\",\n"
            + "\u00A0       \"refresh_interval\": \"1h\",\n"
            + "\u00A0       \"number_of_shards\": 1+(<dataset_table_regions>/256),\n"
            + "\u00A0       \"number_of_replicas\": 0\n"
            + "\u00A0    }\n"
            + "\u00A0}\n"
            + "Example: halyard esindex -s my_dataset -t http://my_elastic.my.org:9200/my_index"
        );
        addOption("s", "source-dataset", "dataset_table", "Source HBase table with Halyard RDF store", true, true);
        addOption("t", "target-index", "target_url", "Elasticsearch target index url <server>:<port>/<index_name>", true, true);
        addOption("c", "create-index", null, "Optionally create Elasticsearch index", false, true);
        addOption("b", "batch-size", "batch_size", "Number of literals sent to Elasticsearch for indexing in one batch (default is 100000)", false, true);
        addOption("g", "named-graph", "named_graph", "Optional restrict indexing to the given named graph only", false, true);
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        String source = cmd.getOptionValue('s');
        String target = cmd.getOptionValue('t');
        TableMapReduceUtil.addDependencyJars(getConf(),
               HalyardExport.class,
               NTriplesUtil.class,
               Rio.class,
               AbstractRDFHandler.class,
               RDFFormat.class,
               RDFParser.class,
               HTable.class,
               HBaseConfiguration.class,
               AuthenticationProtos.class,
               Trace.class,
               Gauge.class);
        HBaseConfiguration.addHbaseResources(getConf());
        Job job = Job.getInstance(getConf(), "HalyardElasticIndexer " + source + " -> " + target);
        job.getConfiguration().set(SOURCE, source);
        job.getConfiguration().set(TARGET, target);
        if (cmd.hasOption('b')) {
            job.getConfiguration().setInt(BUFFER_LIMIT, Integer.parseInt(cmd.getOptionValue('b')));
        }
        if (cmd.hasOption('c')) {
            int shards;
            try (Connection conn = ConnectionFactory.createConnection(getConf())) {
                try (RegionLocator rl = conn.getRegionLocator(TableName.valueOf(source))) {
                    shards = 1 + (rl.getStartKeys().length >> 8);
                }
            }
            HttpURLConnection http = (HttpURLConnection)new URL(target).openConnection();
            http.setRequestMethod("PUT");
            http.setDoOutput(true);
            http.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
            byte b[] = ("{\n"
                + "    \"mappings\" : {\n"
                + "        \"properties\" : {\n"
                + "            \"label\" : { \"type\" : \"text\" },\n"
                + "            \"datatype\" : { \"type\" : \"keyword\" }\n"
                + "        }\n"
                + "    },\n"
                + "   \"settings\": {\n"
                + "       \"index.query.default_field\": \"label\",\n"
                + "       \"refresh_interval\": \"1h\",\n"
                + "       \"number_of_shards\": " + shards + ",\n"
                + "       \"number_of_replicas\": 0\n"
                + "    }\n"
                + "}").getBytes(StandardCharsets.UTF_8);
            http.setFixedLengthStreamingMode(b.length);
            http.connect();
            try {
                try (OutputStream post = http.getOutputStream()) {
                    post.write(b);
                }
                int response = http.getResponseCode();
                String msg = http.getResponseMessage();
                if (response != 200) {
                    String resp = IOUtils.toString(http.getErrorStream());
                    LOG.warning(resp);
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

        Scan scan = HalyardTableUtils.scan(null, null);
        if (cmd.hasOption('g')) {
            //scan only given named graph from COSP region(s)
            byte[] graphHash = RDFContext.hash(NTriplesUtil.parseResource(cmd.getOptionValue('g'), SimpleValueFactory.getInstance()));
            scan.setStartRow(HalyardTableUtils.concat(HalyardTableUtils.COSP_PREFIX, false, graphHash));
            scan.setStopRow(HalyardTableUtils.concat(HalyardTableUtils.COSP_PREFIX, true, graphHash, RDFObject.STOP_KEY, RDFSubject.STOP_KEY, RDFPredicate.END_STOP_KEY));
        } else {
            //scan all OSP region(s)
            scan.setStartRow(new byte[]{HalyardTableUtils.OSP_PREFIX});
            scan.setStopRow(new byte[]{HalyardTableUtils.OSP_PREFIX+1});
        }
        TableMapReduceUtil.initTableMapperJob(
                source,
                scan,
                IndexerMapper.class,
                ImmutableBytesWritable.class,
                LongWritable.class,
                job);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Void.class);
        if (job.waitForCompletion(true)) {
            HttpURLConnection http = (HttpURLConnection)new URL(target + "_refresh").openConnection();
            http.connect();
            http.disconnect();
            LOG.info("Elastic Indexing Completed..");
            return 0;
        }
        return -1;
    }
}
