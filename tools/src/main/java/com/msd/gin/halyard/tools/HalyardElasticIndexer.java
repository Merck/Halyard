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
import com.msd.gin.halyard.common.RDFContext;
import com.msd.gin.halyard.common.RDFObject;
import com.yammer.metrics.core.Gauge;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;

import org.apache.commons.cli.CommandLine;
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.htrace.Trace;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.json.JSONObject;

/**
 * MapReduce tool indexing all RDF literals in Elasticsearch
 * @author Adam Sotona (MSD)
 */
public final class HalyardElasticIndexer extends AbstractHalyardTool {

    private static final String SOURCE = "halyard.elastic.source";

    static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    static final class IndexerMapper extends TableMapper<NullWritable, Text>  {

        long counter = 0, exports = 0, statements = 0;
        Literal lastLiteral;

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context output) throws IOException, InterruptedException {
            if ((counter++ % 100000) == 0) {
                output.setStatus(MessageFormat.format("{0} st:{1} exp:{2} ", counter, statements, exports));
            }

            for (Statement st : HalyardTableUtils.parseStatements(null, null, null, null, value, SVF)) {
                statements++;
            	Literal l = (Literal) st.getObject();
                if (!l.equals(lastLiteral)) {
            		StringBuilder json = new StringBuilder(128);
	                json.append("{\"id\":\"").append(HalyardTableUtils.encode(HalyardTableUtils.id(l)));
	                json.append("\",\"label\":").append(JSONObject.quote(l.getLabel()));
	                if(l.getLanguage().isPresent()) {
		                json.append(",\"lang\":").append(l.getLanguage().get());
	                } else {
		                json.append(",\"datatype\":").append(JSONObject.quote(l.getDatatype().stringValue()));
	                }
	                json.append("}\n");
	                output.write(NullWritable.get(), new Text(json.toString()));
	                lastLiteral = l;
	                exports++;
                }
            }
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
        addOption("g", "named-graph", "named_graph", "Optional restrict indexing to the given named graph only", false, true);
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        String source = cmd.getOptionValue('s');
        String target = cmd.getOptionValue('t');
        URL targetUrl = new URL(target);
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
        if (System.getProperty("exclude.es-hadoop") == null) {
        	TableMapReduceUtil.addDependencyJars(getConf(), EsOutputFormat.class);
        }
        HBaseConfiguration.addHbaseResources(getConf());
        Job job = Job.getInstance(getConf(), "HalyardElasticIndexer " + source + " -> " + target);
        job.getConfiguration().set(SOURCE, source);
        if (cmd.hasOption('c')) {
            int shards;
            try (Connection conn = ConnectionFactory.createConnection(getConf())) {
                try (RegionLocator rl = conn.getRegionLocator(TableName.valueOf(source))) {
                    shards = 1 + (rl.getStartKeys().length >> 8);
                }
            }
            HttpURLConnection http = (HttpURLConnection)targetUrl.openConnection();
            http.setRequestMethod("PUT");
            http.setDoOutput(true);
            http.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
            byte b[] = ("{\n"
                + "    \"mappings\" : {\n"
                + "        \"properties\" : {\n"
                + "            \"label\" : { \"type\" : \"text\" }\n"
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
                    LOG.warn(resp);
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
            //scan only given named graph from COSP literal region(s)
            byte[] graphHash = RDFContext.create(NTriplesUtil.parseResource(cmd.getOptionValue('g'), SVF)).getHash();
            scan.setStartRow(HalyardTableUtils.concat(HalyardTableUtils.COSP_PREFIX, false, graphHash));
            scan.setStopRow(HalyardTableUtils.concat(HalyardTableUtils.COSP_PREFIX, false, graphHash, RDFObject.LITERAL_STOP_KEY));
        } else {
            //scan OSP literal region(s)
            scan.setStartRow(HalyardTableUtils.concat(HalyardTableUtils.OSP_PREFIX, false));
            scan.setStopRow(HalyardTableUtils.concat(HalyardTableUtils.OSP_PREFIX, false, RDFObject.LITERAL_STOP_KEY));
        }
        TableMapReduceUtil.initTableMapperJob(
                source,
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
        job.getConfiguration().set("es.input.json", "yes");
        job.setOutputFormatClass(EsOutputFormat.class);
        job.setNumReduceTasks(0);
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
