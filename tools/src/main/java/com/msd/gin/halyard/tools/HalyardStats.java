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
import com.msd.gin.halyard.sail.HALYARD;
import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.sail.VOID_EXT;
import com.yammer.metrics.core.Gauge;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.WeakHashMap;
import org.apache.commons.cli.CommandLine;
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
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.htrace.Trace;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;

/**
 * MapReduce tool providing statistics about a Halyard dataset. Statistics about a dataset are reported in RDF using the VOID ontology. These statistics can be useful
 * to summarize a graph and it implicitly shows how the subjects, predicates and objects are used. In the absence of schema information this information can be vital.
 * @author Adam Sotona (MSD)
 */
public final class HalyardStats extends AbstractHalyardTool {

    private static final String SOURCE = "halyard.stats.source";
    private static final String TARGET = "halyard.stats.target";
    private static final String THRESHOLD = "halyard.stats.threshold";
    private static final String TARGET_GRAPH = "halyard.stats.target.graph";
    private static final String GRAPH_CONTEXT = "halyard.stats.graph.context";

    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final byte[] TYPE_HASH = HalyardTableUtils.hashKey(NTriplesUtil.toNTriplesString(RDF.TYPE).getBytes(UTF8));

    static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    static final class StatsMapper extends TableMapper<ImmutableBytesWritable, LongWritable>  {

        final SimpleValueFactory ssf = SimpleValueFactory.getInstance();

        final byte[] lastKeyFragment = new byte[20], lastCtxFragment = new byte[20], lastClassFragment = new byte[20];
        IRI statsContext, graphContext;
        byte[] statsContextHash;
        byte lastRegion = -1;
        long counter = 0;
        boolean update;

        IRI graph = HALYARD.STATS_ROOT_NODE, lastGraph = HALYARD.STATS_ROOT_NODE;
        long triples, distinctSubjects, properties, distinctObjects, classes, removed;
        long distinctIRIReferenceSubjects, distinctIRIReferenceObjects, distinctBlankNodeObjects, distinctBlankNodeSubjects, distinctLiterals;
        IRI subsetType;
        String subsetId;
        long threshold, setCounter, subsetCounter;
        HBaseSail sail;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            update = conf.get(TARGET) == null;
            threshold = conf.getLong(THRESHOLD, 1000);
            statsContext = ssf.createIRI(conf.get(TARGET_GRAPH, HALYARD.STATS_GRAPH_CONTEXT.stringValue()));
            String gc = conf.get(GRAPH_CONTEXT);
            if (gc != null) graphContext = ssf.createIRI(gc);
            statsContextHash = HalyardTableUtils.hashKey(NTriplesUtil.toNTriplesString(statsContext).getBytes(UTF8));
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

        private boolean matchingGraphContext(Resource subject) {
            return graphContext == null
                || subject.equals(graphContext)
                || subject.stringValue().startsWith(graphContext.stringValue() + "_subject_")
                || subject.stringValue().startsWith(graphContext.stringValue() + "_property_")
                || subject.stringValue().startsWith(graphContext.stringValue() + "_object_");
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context output) throws IOException, InterruptedException {
            byte region = key.get()[key.getOffset()];
            int hashShift;
            if (region < HalyardTableUtils.CSPO_PREFIX) {
                hashShift = 1;
            } else {
                hashShift = 21;
                if (!matchAndCopyKey(key.get(), key.getOffset() + 1, lastCtxFragment) || region != lastRegion) {
                    cleanup(output);
                    Cell c[] = value.rawCells();
                    ByteBuffer bb = ByteBuffer.wrap(c[0].getQualifierArray(), c[0].getQualifierOffset(), c[0].getQualifierLength());
                    int skip = bb.getInt() + bb.getInt() + bb.getInt();
                    bb.position(bb.position() + skip);
                    byte[] cb = new byte[bb.remaining()];
                    bb.get(cb);
                    graph = NTriplesUtil.parseURI(new String(cb,UTF8), ssf);
                }
                if (update && region == HalyardTableUtils.CSPO_PREFIX) {
                    if (Arrays.equals(statsContextHash, lastCtxFragment)) {
                        if (sail == null) {
                            Configuration conf = output.getConfiguration();
                            sail = new HBaseSail(conf, conf.get(SOURCE), false, 0, true, 0, null, null);
                            sail.initialize();
                        }
                        for (Statement st : HalyardTableUtils.parseStatements(value)) {
                            if (statsContext.equals(st.getContext()) && matchingGraphContext(st.getSubject())) {
                                sail.removeStatement(null, st.getSubject(), st.getPredicate(), st.getObject(), st.getContext());
                                removed++;
                            }
                        }
                        lastRegion = region;
                        return; //do no count removed statements
                    }
                }
            }
            boolean hashChange = !matchAndCopyKey(key.get(), key.getOffset() + hashShift, lastKeyFragment) || region != lastRegion || lastGraph != graph;
            if (hashChange) {
                cleanupSubset(output);
                Cell c = value.rawCells()[0];
                ByteBuffer bb = ByteBuffer.wrap(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength());
                byte[] sb = new byte[bb.getInt()];
                byte[] pb = new byte[bb.getInt()];
                byte[] ob = new byte[bb.getInt()];
                bb.get(sb);
                bb.get(pb);
                bb.get(ob);
                switch (region) {
                    case HalyardTableUtils.SPO_PREFIX:
                    case HalyardTableUtils.CSPO_PREFIX:
                        distinctSubjects++;
                        if (sb[0] == '<') {
                            distinctIRIReferenceSubjects++;
                        } else {
                            distinctBlankNodeSubjects++;
                        }
                        subsetType = VOID_EXT.SUBJECT;
                        subsetId = new String(sb, UTF8);
                        break;
                    case HalyardTableUtils.POS_PREFIX:
                    case HalyardTableUtils.CPOS_PREFIX:
                        properties++;
                        subsetType = VOID.PROPERTY;
                        subsetId = new String(pb, UTF8);
                        break;
                    case HalyardTableUtils.OSP_PREFIX:
                    case HalyardTableUtils.COSP_PREFIX:
                        distinctObjects++;
                        String obj = new String(ob, UTF8);
                        if (ob[0] == '<') {
                            distinctIRIReferenceObjects++;
                        } else if (obj.startsWith("_:")) {
                            distinctBlankNodeObjects++;
                        } else {
                            distinctLiterals++;
                        }
                        subsetType = VOID_EXT.OBJECT;
                        subsetId = obj;
                        break;
                    default:
                        throw new IOException("Unknown region #" + region);
                }
            }
            switch (region) {
                case HalyardTableUtils.SPO_PREFIX:
                case HalyardTableUtils.CSPO_PREFIX:
                    triples += value.rawCells().length;
                    break;
                case HalyardTableUtils.POS_PREFIX:
                case HalyardTableUtils.CPOS_PREFIX:
                    if (Arrays.equals(TYPE_HASH, lastKeyFragment) && (!matchAndCopyKey(key.get(), key.getOffset() + hashShift + 20, lastClassFragment) || hashChange)) {
                            classes++;
                    }
                    break;
                default:
            }
            subsetCounter += value.rawCells().length;
            setCounter += value.rawCells().length;
            lastRegion = region;
            lastGraph = graph;
            if ((counter++ % 100000) == 0) {
                output.setStatus(MessageFormat.format("reg:{0} {1} t:{2} s:{3} p:{4} o:{5} c:{6} r:{7}", region, counter, triples, distinctSubjects, properties, distinctObjects, classes, removed));
            }
        }

        private void report(Context output, IRI property, String partitionId, long value) throws IOException, InterruptedException {
            if (value > 0 && (graphContext == null || graphContext.equals(graph))) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (DataOutputStream dos = new DataOutputStream(baos)) {
                    dos.writeUTF(graph.stringValue());
                    dos.writeUTF(property.stringValue());
                    if (partitionId == null) {
                        dos.writeInt(0);
                    } else {
                        byte b[] = partitionId.getBytes(UTF8);
                        dos.writeInt(b.length);
                        dos.write(b);
                    }
                }
                output.write(new ImmutableBytesWritable(baos.toByteArray()), new LongWritable(value));
            }
        }

        protected void cleanupSubset(Context output) throws IOException, InterruptedException {
            if (subsetCounter >= threshold) {
                report(output, subsetType, subsetId, subsetCounter);
            }
            subsetCounter = 0;
        }

        @Override
        protected void cleanup(Context output) throws IOException, InterruptedException {
            if (graph == HALYARD.STATS_ROOT_NODE || setCounter >= threshold) {
                report(output, VOID.TRIPLES, null, triples);
                report(output, VOID.DISTINCT_SUBJECTS, null, distinctSubjects);
                report(output, VOID.PROPERTIES, null, properties);
                report(output, VOID.DISTINCT_OBJECTS, null, distinctObjects);
                report(output, VOID.CLASSES, null, classes);
                report(output, VOID_EXT.DISTINCT_IRI_REFERENCE_OBJECTS, null, distinctIRIReferenceObjects);
                report(output, VOID_EXT.DISTINCT_IRI_REFERENCE_SUBJECTS, null, distinctIRIReferenceSubjects);
                report(output, VOID_EXT.DISTINCT_BLANK_NODE_OBJECTS, null, distinctBlankNodeObjects);
                report(output, VOID_EXT.DISTINCT_BLANK_NODE_SUBJECTS, null, distinctBlankNodeSubjects);
                report(output, VOID_EXT.DISTINCT_LITERALS, null, distinctLiterals);
            } else {
                report(output, SD.NAMED_GRAPH_PROPERTY, null, 1);
            }
            setCounter = 0;
            triples = 0;
            distinctSubjects = 0;
            properties = 0;
            distinctObjects = 0;
            classes = 0;
            distinctIRIReferenceObjects = 0;
            distinctIRIReferenceSubjects = 0;
            distinctBlankNodeObjects = 0;
            distinctBlankNodeSubjects = 0;
            distinctLiterals = 0;
            cleanupSubset(output);
            if (sail != null) {
                sail.commit();
                sail.close();
                sail = null;
            }
        }

    }

    static final class StatsPartitioner extends Partitioner<ImmutableBytesWritable, LongWritable> {
        @Override
        public int getPartition(ImmutableBytesWritable key, LongWritable value, int numPartitions) {
            try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(key.get(), key.getOffset(), key.getLength()))) {
                return (dis.readUTF().hashCode() & Integer.MAX_VALUE) % numPartitions;
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    static final class StatsReducer extends Reducer<ImmutableBytesWritable, LongWritable, NullWritable, NullWritable>  {

        OutputStream out;
        RDFWriter writer;
        Map<String, Boolean> graphs;
        IRI statsGraphContext;
        HBaseSail sail;
        long removed = 0, added = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            statsGraphContext = SVF.createIRI(conf.get(TARGET_GRAPH, HALYARD.STATS_GRAPH_CONTEXT.stringValue()));
            String targetUrl = conf.get(TARGET);
            if (targetUrl == null) {
                sail = new HBaseSail(conf, conf.get(SOURCE), false, 0, true, 0, null, null);
                sail.initialize();
                sail.setNamespace(SD.PREFIX, SD.NAMESPACE);
                sail.setNamespace(VOID.PREFIX, VOID.NAMESPACE);
                sail.setNamespace(VOID_EXT.PREFIX, VOID_EXT.NAMESPACE);
                sail.setNamespace(HALYARD.PREFIX, HALYARD.NAMESPACE);
            } else {
                targetUrl = MessageFormat.format(targetUrl, context.getTaskAttemptID().getTaskID().getId());
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
                writer.handleNamespace(SD.PREFIX, SD.NAMESPACE);
                writer.handleNamespace(VOID.PREFIX, VOID.NAMESPACE);
                writer.handleNamespace(VOID_EXT.PREFIX, VOID_EXT.NAMESPACE);
                writer.handleNamespace(HALYARD.PREFIX, HALYARD.NAMESPACE);
                writer.startRDF();
            }
            if (conf.get(GRAPH_CONTEXT) == null) {
                writeStatement(HALYARD.STATS_ROOT_NODE, RDF.TYPE, VOID.DATASET);
                writeStatement(HALYARD.STATS_ROOT_NODE, RDF.TYPE, SD.DATASET);
                writeStatement(HALYARD.STATS_ROOT_NODE, RDF.TYPE, SD.GRAPH_CLASS);
                writeStatement(HALYARD.STATS_ROOT_NODE, SD.DEFAULT_GRAPH, HALYARD.STATS_ROOT_NODE);
            }
            graphs = new WeakHashMap<>();
        }

        @Override
	public void reduce(ImmutableBytesWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable val : values) {
                    count += val.get();
            }
            String graph;
            String predicate;
            byte partitionId[];
            try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(key.get(), key.getOffset(), key.getLength()))) {
                graph = dis.readUTF();
                predicate = dis.readUTF();
                partitionId = new byte[dis.readInt()];
                dis.readFully(partitionId);
            }
            if (SD.NAMED_GRAPH_PROPERTY.toString().equals(predicate)) { //workaround to at least count all small named graph that are below the treshold
                writeStatement(HALYARD.STATS_ROOT_NODE, SD.NAMED_GRAPH_PROPERTY, SVF.createIRI(graph));
            } else {
                IRI graphNode;
                if (graph.equals(HALYARD.STATS_ROOT_NODE.stringValue())) {
                    graphNode = HALYARD.STATS_ROOT_NODE;
                } else {
                    graphNode = SVF.createIRI(graph);
                    if (graphs.putIfAbsent(graph, false) == null) {
                        writeStatement(HALYARD.STATS_ROOT_NODE, SD.NAMED_GRAPH_PROPERTY, graphNode);
                        writeStatement(graphNode, SD.NAME, SVF.createIRI(graph));
                        writeStatement(graphNode, SD.GRAPH_PROPERTY, graphNode);
                        writeStatement(graphNode, RDF.TYPE, SD.NAMED_GRAPH_CLASS);
                        writeStatement(graphNode, RDF.TYPE, SD.GRAPH_CLASS);
                        writeStatement(graphNode, RDF.TYPE, VOID.DATASET);
                    }
                }
                if (partitionId.length > 0) {
                    IRI pred = SVF.createIRI(predicate);
                    IRI subset = SVF.createIRI(graph + "_" + pred.getLocalName() + "_" + HalyardTableUtils.encode(HalyardTableUtils.hashKey(partitionId)));
                    writeStatement(graphNode, SVF.createIRI(predicate + "Partition"), subset);
                    writeStatement(subset, RDF.TYPE, VOID.DATASET);
                    writeStatement(subset, pred, NTriplesUtil.parseValue(new String(partitionId, UTF8), SVF));
                    writeStatement(subset, VOID.TRIPLES, SVF.createLiteral(count));
                } else {
                    writeStatement(graphNode, SVF.createIRI(predicate), SVF.createLiteral(count));
                }
                if ((added % 1000) == 0) {
                    context.setStatus(MessageFormat.format("statements removed: {0} added: {1}", removed, added));
                }
            }
	}

        private void writeStatement(Resource subj, IRI pred, Value obj) {
            if (writer == null) {
                sail.addStatement(subj, pred, obj, statsGraphContext);
            } else {
                writer.handleStatement(SVF.createStatement(subj, pred, obj, statsGraphContext));
            }
            added++;
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

    public HalyardStats() {
        super(
            "stats",
            "Halyard Stats is a MapReduce application that calculates dataset statistics and stores them in the named graph within the dataset or exports them into a file. The generated statistics are described by the VoID vocabulary, its extensions, and the SPARQL 1.1 Service Description.",
            "Example: halyard stats -s my_dataset [-g 'http://whatever/mystats'] [-t hdfs:/my_folder/my_stats.trig]");
        addOption("s", "source-dataset", "dataset_table", "Source HBase table with Halyard RDF store", true, true);
        addOption("t", "target-file", "target_url", "Optional target file to export the statistics (instead of update) hdfs://<path>/<file_name>[{0}].<RDF_ext>[.<compression>]", false, true);
        addOption("r", "threshold", "size", "Optional minimal size of a named graph to calculate statistics for (default is 1000)", false, true);
        addOption("c", "named-graph", "named_graph", "Optional restrict stats calculation to the given named graph only", false, true);
        addOption("g", "stats-named-graph", "target_graph", "Optional target named graph of the exported statistics (default is '" + HALYARD.STATS_GRAPH_CONTEXT.stringValue() + "')", false, true);
    }

    private static RowRange rowRange(byte prefix, byte[] hash) {
        return new RowRange(HalyardTableUtils.concat(prefix, false, hash), true, HalyardTableUtils.concat(prefix, true, hash, HalyardTableUtils.STOP_KEY, HalyardTableUtils.STOP_KEY, HalyardTableUtils.STOP_KEY), true);
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        String source = cmd.getOptionValue('s');
        String target = cmd.getOptionValue('t');
        String targetGraph = cmd.getOptionValue('g');
        String graphContext = cmd.getOptionValue('c');
        String thresh = cmd.getOptionValue('r');
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
        Job job = Job.getInstance(getConf(), "HalyardStats " + source + (target == null ? " update" : " -> " + target));
        job.getConfiguration().set(SOURCE, source);
        if (target != null) job.getConfiguration().set(TARGET, target);
        if (targetGraph != null) job.getConfiguration().set(TARGET_GRAPH, targetGraph);
        if (graphContext != null) job.getConfiguration().set(GRAPH_CONTEXT, graphContext);
        if (thresh != null) job.getConfiguration().setLong(THRESHOLD, Long.parseLong(thresh));
        job.setJarByClass(HalyardStats.class);
        TableMapReduceUtil.initCredentials(job);

        Scan scan = new Scan();
        scan.addFamily("e".getBytes(UTF8));
        scan.setMaxVersions(1);
        scan.setBatch(10);
        scan.setAllowPartialResults(true);
        if (graphContext != null) { //restricting stats to scan given graph context only
            List<RowRange> ranges = new ArrayList<>();
            byte[] gcHash = HalyardTableUtils.hashKey(NTriplesUtil.toNTriplesString(SimpleValueFactory.getInstance().createIRI(graphContext)).getBytes(UTF8));
            ranges.add(rowRange(HalyardTableUtils.CSPO_PREFIX, gcHash));
            ranges.add(rowRange(HalyardTableUtils.CPOS_PREFIX, gcHash));
            ranges.add(rowRange(HalyardTableUtils.COSP_PREFIX, gcHash));
            if (target == null) { //add stats context to the scanned row ranges (when in update mode) to delete the related stats during MapReduce
                ranges.add(rowRange(HalyardTableUtils.CSPO_PREFIX, HalyardTableUtils.hashKey(NTriplesUtil.toNTriplesString(targetGraph == null ? HALYARD.STATS_GRAPH_CONTEXT : SimpleValueFactory.getInstance().createIRI(targetGraph)).getBytes(UTF8))));
            }
            scan.setFilter(new MultiRowRangeFilter(ranges));
        }
        TableMapReduceUtil.initTableMapperJob(
                source,
                scan,
                StatsMapper.class,
                ImmutableBytesWritable.class,
                LongWritable.class,
                job);
        job.setPartitionerClass(StatsPartitioner.class);
        job.setReducerClass(StatsReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        if (job.waitForCompletion(true)) {
            LOG.info("Stats Generation Completed..");
            return 0;
        }
        return -1;
    }
}
