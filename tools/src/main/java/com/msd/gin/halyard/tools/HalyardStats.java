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
import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.tools.HalyardExport.ExportException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.WeakHashMap;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.htrace.Trace;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;

/**
 * Map-only MapReduce tool providing parallel SPARQL export functionality
 * @author Adam Sotona (MSD)
 */
public class HalyardStats implements Tool {

    private static final String SOURCE = "halyard.stats.source";
    private static final String TARGET = "halyard.stats.target";
    private static final String GRAPH_CONTEXT = "halyard.stats.graph.context";

    private static final Logger LOG = Logger.getLogger(HalyardStats.class.getName());
    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final byte[] TYPE_HASH = HalyardTableUtils.hashKey(NTriplesUtil.toNTriplesString(RDF.TYPE).getBytes(UTF8));


    private Configuration conf;

    static String getRoot(Configuration cfg) {
        String hbaseRoot = cfg.getTrimmed("hbase.rootdir");
        if (!hbaseRoot.endsWith("/")) hbaseRoot = hbaseRoot + "/";
        return hbaseRoot + cfg.getTrimmed(SOURCE).replace(':', '/');
    }

    static class StatsMapper extends TableMapper<Text, LongWritable>  {

        final SimpleValueFactory ssf = SimpleValueFactory.getInstance();

        class GraphCounter {
            final String graph;
            long triples, subjects, predicates, objects, classes;

            public GraphCounter(String graph) {
                this.graph = graph;
            }

            private void _report(Context output, String key, long value) throws IOException, InterruptedException {
                if (value > 0) {
                    output.write(new Text(key), new LongWritable(value));
                }
            }

            public void report(Context output) throws IOException, InterruptedException {
                _report(output, graph + ":triples", triples);
                _report(output, graph + ":distinctSubjects", subjects);
                _report(output, graph + ":properties", predicates);
                _report(output, graph + ":distinctObjects", objects);
                _report(output, graph + ":classes", classes);
            }
        }

        final byte[] lastKeyFragment = new byte[20], lastCtxFragment = new byte[20], lastClassFragment = new byte[20];
        GraphCounter rootCounter, ctxCounter;
        byte lastRegion = -1;
        long counter = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.rootCounter = new GraphCounter(getRoot(context.getConfiguration()));
        }

        private boolean matchAndCopyKey(byte[] source, int offset, byte[] target) {
            boolean match = true;
            for (int i=0; i<20; i++) {
                byte b = source[i + offset];
                if (b != target[i]) {
                    target[i] = b;
                    match = false;
                }
            }
            return match;
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context output) throws IOException, InterruptedException {
            byte region = key.get()[key.getOffset()];
            if (region < 3) {
                if (!matchAndCopyKey(key.get(), key.getOffset() + 1, lastKeyFragment) || region != lastRegion) {
                    switch (region) {
                        case HalyardTableUtils.SPO_PREFIX:
                            rootCounter.subjects++;
                            break;
                        case HalyardTableUtils.POS_PREFIX:
                            rootCounter.predicates++;
                            break;
                        case HalyardTableUtils.OSP_PREFIX:
                            rootCounter.objects++;
                            break;
                    }
                }
                if (region == HalyardTableUtils.SPO_PREFIX) {
                    rootCounter.triples += value.rawCells().length;
                } else if (region == HalyardTableUtils.POS_PREFIX
                        && Arrays.equals(TYPE_HASH, lastKeyFragment)
                        && (!matchAndCopyKey(key.get(), key.getOffset() + 21, lastClassFragment) || region != lastRegion)) {
                    rootCounter.classes++;
                }
            } else {
                if (!matchAndCopyKey(key.get(), key.getOffset() + 1, lastCtxFragment) || region != lastRegion) {
                    if (ctxCounter != null) {
                        ctxCounter.report(output);
                    }
                    Cell c[] = value.rawCells();
                    ByteBuffer bb = ByteBuffer.wrap(c[0].getQualifierArray(), c[0].getQualifierOffset(), c[0].getQualifierLength());
                    int skip = bb.getInt() + bb.getInt() + bb.getInt();
                    bb.position(bb.position() + skip);
                    byte[] cb = new byte[bb.remaining()];
                    bb.get(cb);
                    ctxCounter = new GraphCounter(NTriplesUtil.parseURI(new String(cb,UTF8), ssf).stringValue());
                }
                if (!matchAndCopyKey(key.get(), key.getOffset() + 21, lastKeyFragment) || region != lastRegion) {
                    switch (region) {
                        case HalyardTableUtils.CSPO_PREFIX:
                            ctxCounter.subjects++;
                            break;
                        case HalyardTableUtils.CPOS_PREFIX:
                            ctxCounter.predicates++;
                            break;
                        case HalyardTableUtils.COSP_PREFIX:
                            ctxCounter.objects++;
                            break;
                    }
                }
                if (region == HalyardTableUtils.CSPO_PREFIX) {
                    ctxCounter.triples += value.rawCells().length;
                } else if (region == HalyardTableUtils.CPOS_PREFIX
                    && Arrays.equals(TYPE_HASH, lastKeyFragment)
                    && (!matchAndCopyKey(key.get(), key.getOffset() + 41, lastClassFragment) || region != lastRegion)) {
                        ctxCounter.classes++;
                }
            }
            lastRegion = region;
            if ((counter++ % 100000) == 0) {
                switch (region) {
                    case HalyardTableUtils.SPO_PREFIX:
                        output.setStatus(MessageFormat.format("SPO {0} t:{1} s:{2}", counter, rootCounter.triples, rootCounter.subjects));
                        break;
                    case HalyardTableUtils.POS_PREFIX:
                        output.setStatus(MessageFormat.format("POS {0} p:{1} cls:{2}", counter, rootCounter.predicates, rootCounter.classes));
                        break;
                    case HalyardTableUtils.OSP_PREFIX:
                        output.setStatus(MessageFormat.format("OSP {0} o:{1}", counter, rootCounter.objects));
                        break;
                    case HalyardTableUtils.CSPO_PREFIX:
                        output.setStatus(MessageFormat.format("CSPO {0} t:{1} s:{2} ctx:<{3}>", counter, ctxCounter.triples, ctxCounter.subjects, ctxCounter.graph));
                        break;
                    case HalyardTableUtils.CPOS_PREFIX:
                        output.setStatus(MessageFormat.format("CPOS {0} p:{1} cls:{2} ctx:<{3}>", counter, ctxCounter.predicates, ctxCounter.classes, ctxCounter.graph));
                        break;
                    case HalyardTableUtils.COSP_PREFIX:
                        output.setStatus(MessageFormat.format("COSP {0} o:{1} ctx:<{2}>", counter, ctxCounter.objects, ctxCounter.graph));
                        break;
                    default:
                        output.setStatus(MessageFormat.format("{0} invalid region {1}", counter, region));
                }
                Runtime.getRuntime().gc();
            }
        }

        @Override
        protected void cleanup(Context output) throws IOException, InterruptedException {
            rootCounter.report(output);
            if (ctxCounter != null) ctxCounter.report(output);
        }

    }

    static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();
    static final String VOID_PREFIX = "http://rdfs.org/ns/void#";
    static final String SD_PREFIX = "http://www.w3.org/ns/sparql-service-description#";
    static final IRI VOID_DATASET_TYPE = SVF.createIRI(VOID_PREFIX, "Dataset");
    static final IRI SD_DATASET_TYPE = SVF.createIRI(SD_PREFIX, "Dataset");
    static final IRI SD_GRAPH_PRED = SVF.createIRI(SD_PREFIX, "graph");
    static final IRI SD_GRAPH_TYPE = SVF.createIRI(SD_PREFIX, "Graph");
    static final IRI SD_DEFAULT_GRAPH_PRED = SVF.createIRI(SD_PREFIX, "defaultGraph");
    static final IRI SD_NAMED_GRAPH_PRED = HBaseSail.SD_NAMED_GRAPH_PRED;
    static final IRI SD_NAMED_GRAPH_TYPE = SVF.createIRI(SD_PREFIX, "NamedGraph");
    static final IRI SD_NAME_PRED = SVF.createIRI(SD_PREFIX, "name");

    static class StatsReducer extends Reducer<Text, LongWritable, NullWritable, NullWritable>  {

        OutputStream out;
        RDFWriter writer;
        IRI rootIRI;
        Map<String, Boolean> graphs;
        IRI statsGraphContext;
        HBaseSail sail;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            statsGraphContext = SVF.createIRI(conf.get(GRAPH_CONTEXT, HBaseSail.STATS_GRAPH_CONTEXT.stringValue()));
            String targetUrl = conf.get(TARGET);
            String root = getRoot(conf);
            if (targetUrl == null) {
                sail = new HBaseSail(conf, conf.get(SOURCE), false, 0, true, 0, null);
                sail.initialize();
                sail.clear(statsGraphContext);
            } else {
                out = FileSystem.get(URI.create(targetUrl), conf).create(new Path(targetUrl));
                try {
                    if (targetUrl.endsWith(".bz2")) {
                        out = new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, out);
                        targetUrl = targetUrl.substring(0, targetUrl.length() - 4);
                    } else if (targetUrl.endsWith(".gz")) {
                        out = new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.GZIP, out);
                        targetUrl = targetUrl.substring(0, targetUrl.length() - 3);
                    }
                } catch (CompressorException ce) {
                    throw new IOException(ce);
                }
                Optional<RDFFormat> form = Rio.getWriterFormatForFileName(targetUrl);
                if (!form.isPresent()) throw new IOException("Unsupported target file format extension: " + targetUrl);
                writer = Rio.createWriter(form.get(), out);
                writer.handleNamespace("", root.substring(0, root.lastIndexOf('/') + 1));
                writer.handleNamespace("namedGraph", root + '/');
                writer.handleNamespace("sd", SD_PREFIX);
                writer.handleNamespace("void", VOID_PREFIX);
                writer.startRDF();
            }
            rootIRI = SVF.createIRI(root);
            writeStatement(rootIRI, RDF.TYPE, VOID_DATASET_TYPE);
            writeStatement(rootIRI, RDF.TYPE, SD_DATASET_TYPE);
            writeStatement(rootIRI, RDF.TYPE, SD_GRAPH_TYPE);
            writeStatement(rootIRI, SD_DEFAULT_GRAPH_PRED, rootIRI);
            graphs = new WeakHashMap<>();
            graphs.put(root, false);
        }

        @Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable val : values) {
                    count += val.get();
            }
            String kp = key.toString();
            int split = kp.lastIndexOf(':');
            String graph = kp.substring(0, split);
            IRI graphIRI;
            if (graph.equals(rootIRI.stringValue())) {
                graphIRI = rootIRI;
            } else {
                graphIRI = SVF.createIRI(rootIRI.stringValue() + '/' + URLEncoder.encode(graph, UTF8.name()));
                if (graphs.putIfAbsent(graph, false) == null) {
                    writeStatement(rootIRI, SD_NAMED_GRAPH_PRED, graphIRI);
                    writeStatement(graphIRI, SD_NAME_PRED, SVF.createIRI(graph));
                    writeStatement(graphIRI, SD_GRAPH_PRED, graphIRI);
                    writeStatement(graphIRI, RDF.TYPE, SD_NAMED_GRAPH_TYPE);
                    writeStatement(graphIRI, RDF.TYPE, SD_GRAPH_TYPE);
                    writeStatement(graphIRI, RDF.TYPE, VOID_DATASET_TYPE);
                }
            }
            writeStatement(graphIRI,
                    SVF.createIRI(VOID_PREFIX, kp.substring(split+1)),
                    SVF.createLiteral(count));
	}

        private void writeStatement(Resource subj, IRI pred, Value obj) {
            if (writer == null) {
                sail.addStatement(subj, pred, obj, statsGraphContext);
            } else {
                writer.handleStatement(SVF.createStatement(subj, pred, obj, statsGraphContext));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (writer == null) {
                sail.commit();
                sail.close();
            } else {
                writer.endRDF();
                out.close();
            }
        }
    }
    private static Option newOption(String opt, String argName, String description) {
        Option o = new Option(opt, null, argName != null, description);
        o.setArgName(argName);
        return o;
    }

    private static void printHelp(Options options) {
        new HelpFormatter().printHelp(100, "stats", "...", options, "Example: stats [-D" + MRJobConfig.QUEUE_NAME + "=proofofconcepts] [-D" + GRAPH_CONTEXT + "='http://whatever/mystats'] -s my_dataset [-t hdfs:/my_folder/my_stats.trig]", true);
    }

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(newOption("h", null, "Prints this help"));
        options.addOption(newOption("v", null, "Prints version"));
        options.addOption(newOption("s", "source_htable", "Source HBase table with Halyard RDF store"));
        options.addOption(newOption("t", "target_url", "hdfs://<path>/<file_name>.<ext>"));
        try {
            CommandLine cmd = new PosixParser().parse(options, args);
            if (args.length == 0 || cmd.hasOption('h')) {
                printHelp(options);
                return -1;
            }
            if (cmd.hasOption('v')) {
                Properties p = new Properties();
                try (InputStream in = HalyardExport.class.getResourceAsStream("/META-INF/maven/com.msd.gin.halyard/hbasesail/pom.properties")) {
                    if (in != null) p.load(in);
                }
                System.out.println("Halyard Stats version " + p.getProperty("version", "unknown"));
                return 0;
            }
            if (!cmd.getArgList().isEmpty()) throw new ExportException("Unknown arguments: " + cmd.getArgList().toString());
            for (char c : "s".toCharArray()) {
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
                   Trace.class);
            HBaseConfiguration.addHbaseResources(getConf());
            Job job = Job.getInstance(getConf(), "HalyardStats " + source + (target == null ? " update" : " -> " + target));
            job.getConfiguration().set(SOURCE, source);
            if (target != null) job.getConfiguration().set(TARGET, target);
            job.setJarByClass(HalyardStats.class);
            TableMapReduceUtil.initCredentials(job);

            Scan scan = new Scan();
            scan.addFamily("e".getBytes(UTF8));
            scan.setMaxVersions(1);
            scan.setBatch(10);
            scan.setAllowPartialResults(true);

            TableMapReduceUtil.initTableMapperJob(
                    source,
                    scan,
                    StatsMapper.class,
                    Text.class,
                    LongWritable.class,
                    job);
            job.setReducerClass(StatsReducer.class);
            job.setNumReduceTasks(1);
            job.setOutputFormatClass(NullOutputFormat.class);
            if (job.waitForCompletion(true)) {
                LOG.info("Stats Generation Completed..");
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
     * Main of the HalyardStats
     * @param args String command line arguments
     * @throws Exception throws Exception in case of any problem
     */
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new HalyardStats(), args));
    }
}
