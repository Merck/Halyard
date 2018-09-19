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
import com.yammer.metrics.core.Gauge;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
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
import org.json.JSONObject;

/**
 * MapReduce tool indexing all RDF literals in Elasticsearch
 * @author Adam Sotona (MSD)
 */
public final class HalyardElasticIndexer extends AbstractHalyardTool {

    private static final String SOURCE = "halyard.elastic.source";
    private static final String TARGET = "halyard.elastic.target";
    private static final String DOCUMENT = "halyard.elastic.document";
    private static final String ATTRIBUTE = "halyard.elastic.attribute";
    private static final String BUFFER_LIMIT = "halyard.elastic.buffer";

    static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    static final class IndexerMapper extends TableMapper<ImmutableBytesWritable, LongWritable>  {

        final SimpleValueFactory ssf = SimpleValueFactory.getInstance();
        long counter = 0, exports = 0, batches = 0, statements = 0;
        byte[] lastHash = new byte[20], hash = new byte[20];
        ArrayList<String> literals = new ArrayList<>();
        StringBuilder batch = new StringBuilder();
        URL url;
        int bufferLimit;
        String doc, attr;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            doc = context.getConfiguration().get(DOCUMENT, "l");
            attr = context.getConfiguration().get(ATTRIBUTE, "l");
            url = new URL(context.getConfiguration().get(TARGET)+"/" +doc + "/_bulk");
            bufferLimit = context.getConfiguration().getInt(BUFFER_LIMIT, 100000);
        }


        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context output) throws IOException, InterruptedException {
            if ((counter++ % 100000) == 0) {
                output.setStatus(MessageFormat.format("{0} st:{1} exp:{2} batch:{3} ", counter, statements, exports, batches));
            }
            hash = new byte[20];
            System.arraycopy(key.get(), key.getOffset() + 1, hash, 0, 20);
            if (!Arrays.equals(hash, lastHash)) {
                export(false);
                lastHash = hash;
            }
            for (Statement st : HalyardTableUtils.parseStatements(value)) {
                statements++;
                if (st.getObject() instanceof Literal) {
                    String l = st.getObject().stringValue();
                    if (!literals.contains(l)) {
                        literals.add(l);
                    }
                }
            }
        }

        private void export(boolean flush) throws IOException {
            if (literals.size() > 0) {
                batch.append("{\"index\":{\"_id\":\"").append(Hex.encodeHex(lastHash)).append("\"}}\n{");
                for (int i = 0; i < literals.size(); i++) {
                    if (i > 0) {
                        batch.append(',');
                    }
                    batch.append('\"').append(attr).append("\":").append(JSONObject.quote(literals.get(i)));
                }
                batch.append("}\n");
                literals.clear();
                exports++;
            }
            if ((flush && batch.length() > 0) || batch.length() > bufferLimit) {
                HttpURLConnection http = (HttpURLConnection)url.openConnection();
                http.setRequestMethod("POST");
                http.setDoOutput(true);
                http.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
                byte b[] = batch.toString().getBytes(StandardCharsets.UTF_8);
                batch = new StringBuilder();
                http.setFixedLengthStreamingMode(b.length);
                http.connect();
                try {
                    try (OutputStream post = http.getOutputStream()) {
                        post.write(b);
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
            "Default index mapping is:\n"
            + "\u00A0{\n"
            + "\u00A0    \"mappings\" : {\n"
            + "\u00A0        \"l\" : {\n"
            + "\u00A0            \"properties\" : {\n"
            + "\u00A0                \"l\" : { \"type\" : \"text\" }\n"
            + "\u00A0            },\n"
            + "\u00A0            \"_source\" : {\n"
            + "\u00A0              \"enabled\" : false\n"
            + "\u00A0            }\n"
            + "\u00A0        }\n"
            + "\u00A0    }\n"
            + "\u00A0}\n"
            + "Example: halyard esindex -s my_dataset -t http://my_elastic.my.org:9200/my_index"
        );
        addOption("s", "source-dataset", "dataset_table", "Source HBase table with Halyard RDF store", true, true);
        addOption("t", "target-index", "target_url", "Elasticsearch target index url <server>:<port>/<index_name>", true, true);
        addOption("c", "create-index", null, "Optionally create Elasticsearch index", false, true);
        addOption("d", "document-type", "document_type", "Optionally specify document type within the index, default is 'l'", false, true);
        addOption("a", "attribute-name", "attribute_name", "Optionally specify attribute name to index literals within the document, default is 'l'", false, true);
        addOption("b", "batch-size", "batch_size", "Number of literals sent to Elasticsearch for indexing in one batch (default is 100000)", false, true);
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        String source = cmd.getOptionValue('s');
        String target = cmd.getOptionValue('t');
        String doc = cmd.getOptionValue('d', "l");
        String attr = cmd.getOptionValue('a', "l");
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
        job.getConfiguration().set(DOCUMENT, doc);
        job.getConfiguration().set(ATTRIBUTE, attr);
        if (cmd.hasOption('b')) {
            job.getConfiguration().setInt(BUFFER_LIMIT, Integer.parseInt(cmd.getOptionValue('b')));
        }
        if (cmd.hasOption('c')) {
            HttpURLConnection http = (HttpURLConnection)new URL(target).openConnection();
            http.setRequestMethod("PUT");
            http.setDoOutput(true);
            http.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
            byte b[] = ("{\n"
                + "    \"mappings\" : {\n"
                + "        \"" + doc + "\" : {\n"
                + "            \"properties\" : {\n"
                + "                \"" + attr + "\" : { \"type\" : \"text\" }\n"
                + "            },\n"
                + "            \"_source\" : {\n"
                + "              \"enabled\" : false\n"
                + "            }\n"
                + "        }\n"
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

        Scan scan = new Scan();
        scan.addFamily("e".getBytes(StandardCharsets.UTF_8));
        scan.setMaxVersions(1);
        scan.setBatch(10);
        scan.setAllowPartialResults(true);
        scan.setStartRow(new byte[]{HalyardTableUtils.OSP_PREFIX});
        scan.setStopRow(new byte[]{HalyardTableUtils.OSP_PREFIX+1});

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
            LOG.info("Elastic Indexing Completed..");
            return 0;
        }
        return -1;
    }
}
