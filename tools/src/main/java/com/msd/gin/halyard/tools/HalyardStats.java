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
import com.msd.gin.halyard.common.RDFContext;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.RDFPredicate;
import com.msd.gin.halyard.common.SPOC;
import com.msd.gin.halyard.common.StatementIndex;
import com.msd.gin.halyard.common.ValueIO;
import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.vocab.HALYARD;
import com.msd.gin.halyard.vocab.VOID_EXT;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.WeakHashMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.helpers.NTriplesUtil;
import org.eclipse.rdf4j.sail.SailConnection;

/**
 * MapReduce tool providing statistics about a Halyard dataset. Statistics about a dataset are reported in RDF using the VOID ontology. These statistics can be useful
 * to summarize a graph and it implicitly shows how the subjects, predicates and objects are used. In the absence of schema information this information can be vital.
 * @author Adam Sotona (MSD)
 */
public final class HalyardStats extends AbstractHalyardTool {

    private static final String SOURCE = "halyard.stats.source";
    private static final String SNAPSHOT_PATH = "halyard.stats.snapshot";
    private static final String TARGET = "halyard.stats.target";
    private static final String THRESHOLD = "halyard.stats.threshold";
    private static final String TARGET_GRAPH = "halyard.stats.target.graph";
    private static final String GRAPH_CONTEXT = "halyard.stats.graph.context";

    static final class StatsMapper extends RdfTableMapper<ImmutableBytesWritable, LongWritable>  {

        final ImmutableBytesWritable outputKey = new ImmutableBytesWritable();
        final LongWritable outputValue = new LongWritable();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(128);
        ByteBuffer bb = ByteBuffer.allocate(ValueIO.DEFAULT_BUFFER_SIZE);
    	RDFPredicate RDF_TYPE_PREDICATE;
    	byte[] POS_TYPE_HASH;
    	byte[] CPOS_TYPE_HASH;
        IRI statsContext, graphContext;
        byte[] cspoStatsContextHash;
    	StatementIndex<SPOC.S,SPOC.P,SPOC.O,SPOC.C> spo;
    	StatementIndex<SPOC.P,SPOC.O,SPOC.S,SPOC.C> pos;
    	StatementIndex<SPOC.O,SPOC.S,SPOC.P,SPOC.C> osp;
    	StatementIndex<SPOC.C,SPOC.S,SPOC.P,SPOC.O> cspo;
    	StatementIndex<SPOC.C,SPOC.P,SPOC.O,SPOC.S> cpos;
    	StatementIndex<SPOC.C,SPOC.O,SPOC.S,SPOC.P> cosp;
        int subjectKeySize;
        int predicateKeySize;
        int objectKeySize;
        int contextKeySize;
        byte[] lastSubjFragment;
        byte[] lastPredFragment;
        byte[] lastObjFragment;
        byte[] lastCtxFragment;
        byte[] lastClassFragment;
        StatementIndex<?,?,?,?> lastIndex;
        long counter = 0;
        boolean update;

        IRI graph = HALYARD.STATS_ROOT_NODE, lastGraph = HALYARD.STATS_ROOT_NODE;
        long triples, distinctSubjects, properties, distinctObjects, classes, removed;
        long distinctIRIReferenceSubjects, distinctIRIReferenceObjects, distinctBlankNodeObjects, distinctBlankNodeSubjects, distinctLiterals;
        IRI subsetType;
		Value subsetId;
        long threshold, setCounter, subsetCounter;
        HBaseSail sail;
		SailConnection conn;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            openKeyspace(conf, conf.get(SOURCE), conf.get(SNAPSHOT_PATH));
            spo = rdfFactory.getSPOIndex();
            pos = rdfFactory.getPOSIndex();
            osp = rdfFactory.getOSPIndex();
            cspo = rdfFactory.getCSPOIndex();
            cpos = rdfFactory.getCPOSIndex();
            cosp = rdfFactory.getCOSPIndex();
            subjectKeySize = rdfFactory.getSubjectRole().keyHashSize();
            predicateKeySize = rdfFactory.getPredicateRole().keyHashSize();
            objectKeySize = rdfFactory.getObjectRole().keyHashSize();
            contextKeySize = rdfFactory.getContextRole().keyHashSize();
            lastSubjFragment = new byte[subjectKeySize];
            lastPredFragment = new byte[predicateKeySize];
            lastObjFragment = new byte[objectKeySize];
            lastCtxFragment = new byte[contextKeySize];
            lastClassFragment = new byte[objectKeySize];
            spo = rdfFactory.getSPOIndex();
        	RDF_TYPE_PREDICATE = rdfFactory.createPredicate(RDF.TYPE);
        	POS_TYPE_HASH = RDF_TYPE_PREDICATE.getKeyHash(pos);
        	CPOS_TYPE_HASH = RDF_TYPE_PREDICATE.getKeyHash(cpos);
            update = conf.get(TARGET) == null;
            threshold = conf.getLong(THRESHOLD, 1000);
            ValueFactory vf = rdfFactory.getIdValueFactory();
            statsContext = vf.createIRI(conf.get(TARGET_GRAPH, HALYARD.STATS_GRAPH_CONTEXT.stringValue()));
            String gc = conf.get(GRAPH_CONTEXT);
            if (gc != null) {
            	graphContext = vf.createIRI(gc);
            }
			cspoStatsContextHash = rdfFactory.createContext(statsContext).getKeyHash(cspo);
        }

        private boolean matchAndCopyKey(byte[] source, int offset, int len, byte[] target) {
            boolean match = true;
            for (int i=0; i<len; i++) {
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
            StatementIndex<?,?,?,?> index = StatementIndex.toIndex(key.get()[key.getOffset()], rdfFactory);
			List<Statement> stmts = HalyardTableUtils.parseStatements(null, null, null, null, value, valueReader, rdfFactory);
            int hashShift;
            if (!index.isQuadIndex()) {
            	// triple region
                hashShift = 1;
            } else {
            	// quad region
                hashShift = 1 + contextKeySize;
                if (!matchAndCopyKey(key.get(), key.getOffset() + 1, contextKeySize, lastCtxFragment) || index != lastIndex) {
                    cleanup(output);
					graph = (IRI) stmts.get(0).getContext();
                }
                if (update && index == cspo) {
                    if (Arrays.equals(cspoStatsContextHash, lastCtxFragment)) {
                        if (sail == null) {
                            Configuration conf = output.getConfiguration();
                            sail = new HBaseSail(conf, conf.get(SOURCE), false, 0, true, 0, null, null);
                            sail.initialize();
							conn = sail.getConnection();
                        }
						for (Statement st : stmts) {
                            if (statsContext.equals(st.getContext()) && matchingGraphContext(st.getSubject())) {
								conn.removeStatement(null, st.getSubject(), st.getPredicate(), st.getObject(), st.getContext());
                                removed++;
                            }
                        }
                        lastIndex = index;
                        return; //do no count removed statements
                    }
                }
            }

            int keyLen;
            byte[] lastKeyFragment;
            switch (index.getName()) {
                case SPO:
                case CSPO:
                	keyLen = subjectKeySize;
                	lastKeyFragment = lastSubjFragment;
                    break;
                case POS:
                case CPOS:
                	keyLen = predicateKeySize;
                	lastKeyFragment = lastPredFragment;
                    break;
                case OSP:
                case COSP:
                	keyLen = objectKeySize;
                	lastKeyFragment = lastObjFragment;
                    break;
                default:
                    throw new IOException("Unknown region #" + index);
            }
            boolean hashChange = !matchAndCopyKey(key.get(), key.getOffset() + hashShift, keyLen, lastKeyFragment) || index != lastIndex || lastGraph != graph;
            if (hashChange) {
                cleanupSubset(output);
				Statement stmt = stmts.get(0);
                switch (index.getName()) {
                    case SPO:
                    case CSPO:
                        distinctSubjects++;
                        Resource subj = stmt.getSubject();
                        if (subj instanceof IRI) {
                            distinctIRIReferenceSubjects++;
                        } else {
                            distinctBlankNodeSubjects++;
                        }
                        subsetType = VOID_EXT.SUBJECT;
                        subsetId = subj;
                        break;
                    case POS:
                    case CPOS:
                        properties++;
                        subsetType = VOID.PROPERTY;
                        subsetId = stmt.getPredicate();
                        break;
                    case OSP:
                    case COSP:
                        distinctObjects++;
                        Value obj = stmt.getObject();
                        if (obj instanceof IRI) {
                        	distinctIRIReferenceObjects++;
                        } else if (obj instanceof BNode) {
                        	distinctBlankNodeObjects++;
                        } else {
                            distinctLiterals++;
                        }
                        subsetType = VOID_EXT.OBJECT;
                        subsetId = obj;
                        break;
                    default:
                        throw new IOException("Unknown region #" + index);
                }
            }
            int stmtCount = stmts.size();
            switch (index.getName()) {
                case SPO:
                case CSPO:
                    triples += stmtCount;
                    break;
                case POS:
                    if (Arrays.equals(POS_TYPE_HASH, lastKeyFragment) && (!matchAndCopyKey(key.get(), key.getOffset() + hashShift + predicateKeySize, objectKeySize, lastClassFragment) || hashChange)) {
                    	classes++;
                    }
                    break;
                case CPOS:
                    if (Arrays.equals(CPOS_TYPE_HASH, lastKeyFragment) && (!matchAndCopyKey(key.get(), key.getOffset() + hashShift + predicateKeySize, objectKeySize, lastClassFragment) || hashChange)) {
                    	classes++;
                    }
                    break;
                default:
            }
            subsetCounter += stmtCount;
            setCounter += stmtCount;
            lastIndex = index;
            lastGraph = graph;
            if ((counter++ % 100000) == 0) {
                output.setStatus(MessageFormat.format("reg:{0} {1} t:{2} s:{3} p:{4} o:{5} c:{6} r:{7}", index, counter, triples, distinctSubjects, properties, distinctObjects, classes, removed));
            }
        }

		private void report(Context output, IRI property, Value partitionId, long count) throws IOException, InterruptedException {
            if (count > 0 && (graphContext == null || graphContext.equals(graph))) {
            	baos.reset();
                try (DataOutputStream dos = new DataOutputStream(baos)) {
                    dos.writeUTF(graph.stringValue());
                    dos.writeUTF(property.stringValue());
                    if (partitionId == null) {
                        dos.writeInt(0);
                    } else {
                    	bb.clear();
						bb = rdfFactory.idTripleWriter.writeTo(partitionId, bb);
						bb.flip();
						int len = bb.limit();
                        dos.writeInt(len);
                        dos.write(bb.array(), bb.arrayOffset(), len);
                    }
                }
                outputKey.set(baos.toByteArray());
                outputValue.set(count);
                output.write(outputKey, outputValue);
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
				conn.close();
				conn = null;
				sail.shutDown();
                sail = null;
            }
            closeKeyspace();
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

    static final class StatsReducer extends RdfReducer<ImmutableBytesWritable, LongWritable, NullWritable, NullWritable>  {

        OutputStream out;
        RDFWriter writer;
        Map<String, Boolean> graphs;
        IRI statsGraphContext;
        ValueFactory vf;
        HBaseSail sail;
		SailConnection conn;
        long removed = 0, added = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            openKeyspace(conf, conf.get(SOURCE), conf.get(SNAPSHOT_PATH));
            vf = rdfFactory.getIdValueFactory();
            statsGraphContext = vf.createIRI(conf.get(TARGET_GRAPH, HALYARD.STATS_GRAPH_CONTEXT.stringValue()));
            String targetUrl = conf.get(TARGET);
            if (targetUrl == null) {
                sail = new HBaseSail(conf, conf.get(SOURCE), false, 0, true, 0, null, null);
                sail.initialize();
				conn = sail.getConnection();
				conn.setNamespace(SD.PREFIX, SD.NAMESPACE);
				conn.setNamespace(VOID.PREFIX, VOID.NAMESPACE);
				conn.setNamespace(VOID_EXT.PREFIX, VOID_EXT.NAMESPACE);
				conn.setNamespace(HALYARD.PREFIX, HALYARD.NAMESPACE);
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
                if (!form.isPresent()) {
                	throw new IOException("Unsupported target file format extension: " + targetUrl);
                }
                writer = Rio.createWriter(form.get(), out);
                writer.startRDF();
                writer.handleNamespace(SD.PREFIX, SD.NAMESPACE);
                writer.handleNamespace(VOID.PREFIX, VOID.NAMESPACE);
                writer.handleNamespace(VOID_EXT.PREFIX, VOID_EXT.NAMESPACE);
                writer.handleNamespace(HALYARD.PREFIX, HALYARD.NAMESPACE);
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
            ByteBuffer partitionId;
            try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(key.get(), key.getOffset(), key.getLength()))) {
                graph = dis.readUTF();
                predicate = dis.readUTF();
                partitionId = ByteBuffer.allocate(dis.readInt());
                dis.readFully(partitionId.array(), partitionId.arrayOffset(), partitionId.limit());
            }

            if (SD.NAMED_GRAPH_PROPERTY.stringValue().equals(predicate)) { //workaround to at least count all small named graph that are below the treshold
                writeStatement(HALYARD.STATS_ROOT_NODE, SD.NAMED_GRAPH_PROPERTY, vf.createIRI(graph));
            } else {
                IRI graphNode;
                if (HALYARD.STATS_ROOT_NODE.stringValue().equals(graph)) {
                    graphNode = HALYARD.STATS_ROOT_NODE;
                } else {
                    graphNode = vf.createIRI(graph);
                    if (graphs.putIfAbsent(graph, false) == null) {
                        writeStatement(HALYARD.STATS_ROOT_NODE, SD.NAMED_GRAPH_PROPERTY, graphNode);
                        writeStatement(graphNode, SD.NAME, vf.createIRI(graph));
                        writeStatement(graphNode, SD.GRAPH_PROPERTY, graphNode);
                        writeStatement(graphNode, RDF.TYPE, SD.NAMED_GRAPH_CLASS);
                        writeStatement(graphNode, RDF.TYPE, SD.GRAPH_CLASS);
                        writeStatement(graphNode, RDF.TYPE, VOID.DATASET);
                    }
                }
                if (partitionId.hasRemaining()) {
					Value partition = valueReader.readValue(partitionId);
                    IRI pred = vf.createIRI(predicate);
					IRI subset = vf.createIRI(graph + "_" + pred.getLocalName() + "_" + rdfFactory.id(partition));
                    writeStatement(graphNode, vf.createIRI(predicate + "Partition"), subset);
                    writeStatement(subset, RDF.TYPE, VOID.DATASET);
					writeStatement(subset, pred, partition);
                    writeStatement(subset, VOID.TRIPLES, vf.createLiteral(count));
                } else {
                    writeStatement(graphNode, vf.createIRI(predicate), vf.createLiteral(count));
                }
                if ((added % 1000) == 0) {
                    context.setStatus(MessageFormat.format("statements removed: {0} added: {1}", removed, added));
                }
            }
        }

        private void writeStatement(Resource subj, IRI pred, Value obj) {
            if (conn != null) {
				conn.addStatement(subj, pred, obj, statsGraphContext);
            }
            if (writer != null) {
                writer.handleStatement(vf.createStatement(subj, pred, obj, statsGraphContext));
            }
            added++;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (conn != null) {
				conn.close();
				sail.shutDown();
            }
            if (writer != null) {
                writer.endRDF();
                out.close();
            }
            closeKeyspace();
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
        addOption("g", "stats-named-graph", "target_graph", "Optional target named graph of the exported statistics (default value is '" + HALYARD.STATS_GRAPH_CONTEXT.stringValue() + "'), modification is recomended only for external export as internal Halyard optimizers expect the default value", false, true);
        addOption("u", "restore-dir", "restore_folder", "If specified then -s is a snapshot name and this is the restore folder on HDFS", false, true);
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
    	if (cmd.hasOption('u') && !cmd.hasOption('t')) {
    		throw new MissingOptionException("Statistics cannot be written to a snapshot, please specify -t.");
    	}
        String source = cmd.getOptionValue('s');
        String target = cmd.getOptionValue('t');
        String targetGraph = cmd.getOptionValue('g');
        String graphContext = cmd.getOptionValue('c');
        String thresh = cmd.getOptionValue('r');
        TableMapReduceUtil.addDependencyJarsForClasses(getConf(),
               NTriplesUtil.class,
               Rio.class,
               AbstractRDFHandler.class,
               RDFFormat.class,
               RDFParser.class,
               Table.class,
               HBaseConfiguration.class,
               AuthenticationProtos.class);
        HBaseConfiguration.addHbaseResources(getConf());
        Job job = Job.getInstance(getConf(), "HalyardStats " + source + (target == null ? " update" : " -> " + target));
        job.getConfiguration().set(SOURCE, source);
        if (cmd.hasOption('u')) {
			FileSystem fs = CommonFSUtils.getRootDirFileSystem(getConf());
        	if (fs.exists(new Path(cmd.getOptionValue('u')))) {
        		throw new IOException("Snapshot restore directory already exists");
        	}
            job.getConfiguration().set(SNAPSHOT_PATH, cmd.getOptionValue('u'));
        }
        if (target != null) {
            job.getConfiguration().set(TARGET, target);
        }
        if (targetGraph != null) {
            job.getConfiguration().set(TARGET_GRAPH, targetGraph);
        }
        if (graphContext != null) {
            job.getConfiguration().set(GRAPH_CONTEXT, graphContext);
        }
        if (thresh != null) {
            job.getConfiguration().setLong(THRESHOLD, Long.parseLong(thresh));
        }
        job.setJarByClass(HalyardStats.class);
        TableMapReduceUtil.initCredentials(job);

        RDFFactory rdfFactory;
        Keyspace keyspace = HalyardTableUtils.getKeyspace(getConf(), source, cmd.getOptionValue('u'));
        try {
        	try (KeyspaceConnection kc = keyspace.getConnection()) {
        		rdfFactory = RDFFactory.create(kc);
        	}
		} finally {
			keyspace.close();
		}
        List<Scan> scans;
        if (graphContext != null) { //restricting stats to scan given graph context only
            scans = new ArrayList<>(4);
            ValueFactory vf = rdfFactory.getIdValueFactory();
            RDFContext rdfGraphCtx = rdfFactory.createContext(vf.createIRI(graphContext));
            scans.add(rdfFactory.getCSPOIndex().scan(rdfGraphCtx));
            scans.add(rdfFactory.getCPOSIndex().scan(rdfGraphCtx));
            scans.add(rdfFactory.getCOSPIndex().scan(rdfGraphCtx));
            if (target == null) {
                // add stats context to the scanned row ranges (when in update mode) to delete the related stats during MapReduce
				scans.add(rdfFactory.getCSPOIndex().scan(
					rdfFactory.createContext(targetGraph == null ? HALYARD.STATS_GRAPH_CONTEXT : vf.createIRI(targetGraph))
				));
            }
        } else {
            scans = Collections.singletonList(StatementIndex.scanAll(rdfFactory));
        }
        keyspace.initMapperJob(
	        scans,
	        StatsMapper.class,
	        ImmutableBytesWritable.class,
	        LongWritable.class,
	        job);
        job.setPartitionerClass(StatsPartitioner.class);
        job.setReducerClass(StatsReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        try {
	        if (job.waitForCompletion(true)) {
	            LOG.info("Stats Generation completed.");
	            return 0;
	        } else {
	    		LOG.error("Stats Generation failed to complete.");
	            return -1;
	        }
        } finally {
        	keyspace.destroy();
        }
    }
}
