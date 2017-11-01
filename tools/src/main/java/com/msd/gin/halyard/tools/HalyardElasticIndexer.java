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
import com.msd.gin.halyard.tools.HalyardExport.ExportException;
import com.yammer.metrics.core.Gauge;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
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
public class HalyardElasticIndexer implements Tool {

    private static final String SOURCE = "halyard.elastic.source";
    private static final String TARGET = "halyard.elastic.target";
    private static final int BUFFER_LIMIT = 1000000;

    private static final Logger LOG = Logger.getLogger(HalyardElasticIndexer.class.getName());
    private static final Charset UTF8 = Charset.forName("UTF-8");

    static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    private Configuration conf;

    static final class IndexerMapper extends TableMapper<ImmutableBytesWritable, LongWritable>  {

        final SimpleValueFactory ssf = SimpleValueFactory.getInstance();
        long counter = 0, exports = 0, batches = 0, statements = 0;
        byte[] lastHash = new byte[20], hash = new byte[20];
        ArrayList<String> literals = new ArrayList<>();
        StringBuilder batch = new StringBuilder();
        URL url;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            url = new URL(context.getConfiguration().get(TARGET)+"/l/_bulk");
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
                    batch.append("\"l\":").append(JSONObject.quote(literals.get(i)));
                }
                batch.append("}\n");
                literals.clear();
                exports++;
            }
            if ((flush && batch.length() > 0) || batch.length() > BUFFER_LIMIT) {
                HttpURLConnection http = (HttpURLConnection)url.openConnection();
                http.setRequestMethod("POST");
                http.setDoOutput(true);
                http.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
                byte b[] = batch.toString().getBytes(UTF8);
                batch = new StringBuilder();
                http.setFixedLengthStreamingMode(b.length);
                http.connect();
                try (OutputStream post = http.getOutputStream()) {
                    post.write(b);
                }
                int response = http.getResponseCode();
                String msg = http.getResponseMessage();
                http.disconnect();
                if (exports > 0 && response != 200) {
                    throw new IOException(msg);
                }
                batches++;
            }
        }

        @Override
        protected void cleanup(Context output) throws IOException, InterruptedException {
            export(true);
        }
    }

    private static Option newOption(String opt, String argName, String description) {
        Option o = new Option(opt, null, argName != null, description);
        o.setArgName(argName);
        return o;
    }

    private static void printHelp(Options options) {
        new HelpFormatter().printHelp(100, "stats", "Indexes all literals from Halyard dataset into Elasticsearch.", options, "Example: esindex -s my_dataset -t http://my_elastic.my.org:9200/my_index", true);
    }

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(newOption("h", null, "Prints this help"));
        options.addOption(newOption("v", null, "Prints version"));
        options.addOption(newOption("s", "source_htable", "Source HBase table with Halyard RDF store"));
        options.addOption(newOption("t", "target_url", "Elasticsearch target index url <server>:<port>/<index_name>"));
        try {
            CommandLine cmd = new PosixParser().parse(options, args);
            if (args.length == 0 || cmd.hasOption('h')) {
                printHelp(options);
                return -1;
            }
            if (cmd.hasOption('v')) {
                Properties p = new Properties();
                try (InputStream in = HalyardElasticIndexer.class.getResourceAsStream("/META-INF/maven/com.msd.gin.halyard/halyard-tools/pom.properties")) {
                    if (in != null) p.load(in);
                }
                System.out.println("Halyard Elastic Indexer version " + p.getProperty("version", "unknown"));
                return 0;
            }
            if (!cmd.getArgList().isEmpty()) throw new ExportException("Unknown arguments: " + cmd.getArgList().toString());
            for (char c : "st".toCharArray()) {
                if (!cmd.hasOption(c))  throw new ExportException("Missing mandatory option: " + c);
            }
            for (char c : "st".toCharArray()) {
                String s[] = cmd.getOptionValues(c);
                if (s != null && s.length > 1)  throw new ExportException("Multiple values for option: " + c);
            }
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
            job.setJarByClass(HalyardElasticIndexer.class);
            TableMapReduceUtil.initCredentials(job);

            Scan scan = new Scan();
            scan.addFamily("e".getBytes(UTF8));
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
        } catch (RuntimeException exp) {
            System.out.println(exp.getMessage());
            printHelp(options);
            throw exp;
        }

    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public void setConf(final Configuration c) {
        this.conf = c;
    }

    /**
     * Main of the HalyardElasticIndexer
     * @param args String command line arguments
     * @throws Exception throws Exception in case of any problem
     */
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new HalyardElasticIndexer(), args));
    }
}
