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
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFParserFactory;
import org.eclipse.rdf4j.rio.RDFParserRegistry;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;

/**
 * Apache Hadoop MapReduce Tool for bulk loading RDF into HBase
 * @author Adam Sotona (MSD)
 */
public class HalyardBulkLoad implements Tool {

    /**
     * Property defining number of bits used for HBase region pre-splits calculation for new table
     */
    public static final String SPLIT_BITS_PROPERTY = "halyard.table.splitbits";

    /**
     * Property truncating existing HBase table just before the bulk load
     */
    public static final String TRUNCATE_PROPERTY = "halyard.table.truncate";

    /**
     * Boolean property skipping RDF parsing errors
     */
    public static final String SKIP_INVALID_PROPERTY = "halyard.parser.skipinvalid";

    /**
     * Boolean property enforcing triples and quads context override with the default context
     */
    public static final String OVERRIDE_CONTEXT_PROPERTY = "halyard.parser.context.override";

    /**
     * Property defining default context for triples (or even for quads when context override is set)
     */
    public static final String DEFAULT_CONTEXT_PROPERTY = "halyard.parser.context.default";

    /**
     * Property defining exact timestamp of all loaded triples (System.currentTimeMillis() is the default value)
     */
    public static final String DEFAULT_TIMESTAMP_PROPERTY = "halyard.bulk.timestamp";

    private static final Logger LOG = Logger.getLogger(HalyardBulkLoad.class.getName());

    private Configuration conf;

    /**
     * Mapper class transforming each parsed Statement into set of HBase KeyValues
     */
    public static class RDFMapper extends Mapper<LongWritable, Statement, ImmutableBytesWritable, KeyValue> {

        private IRI defaultRdfContext;
        private boolean overrideRdfContext;
        private long timestamp;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            overrideRdfContext = conf.getBoolean(OVERRIDE_CONTEXT_PROPERTY, false);
            String defCtx = conf.get(DEFAULT_CONTEXT_PROPERTY);
            defaultRdfContext = defCtx == null ? null : SimpleValueFactory.getInstance().createIRI(defCtx);
            timestamp = conf.getLong(DEFAULT_TIMESTAMP_PROPERTY, System.currentTimeMillis());
        }

        @Override
        protected void map(LongWritable key, Statement value, final Context context) throws IOException, InterruptedException {
            Resource rdfContext;
            if (overrideRdfContext || (rdfContext = value.getContext()) == null) {
                rdfContext = defaultRdfContext;
            }
            for (KeyValue keyValue: HalyardTableUtils.toKeyValues(value.getSubject(), value.getPredicate(), value.getObject(), rdfContext, false, timestamp)) {
                context.write(new ImmutableBytesWritable(keyValue.getRowArray(), keyValue.getRowOffset(), keyValue.getRowLength()), keyValue);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: bulkload [-D" + MRJobConfig.QUEUE_NAME + "=proofofconcepts] [-D" + SKIP_INVALID_PROPERTY + "=true] [-D" + SPLIT_BITS_PROPERTY + "=8] [-D" + DEFAULT_CONTEXT_PROPERTY + "=http://new_context] [-D" + OVERRIDE_CONTEXT_PROPERTY + "=true] <input_path(s)> <output_path> <table_name>");
            return -1;
        }
        TableMapReduceUtil.addDependencyJars(getConf(),
                NTriplesUtil.class,
                Rio.class,
                AbstractRDFHandler.class,
                RDFFormat.class,
                RDFParser.class);
        HBaseConfiguration.addHbaseResources(getConf());
        getConf().setLong(DEFAULT_TIMESTAMP_PROPERTY, getConf().getLong(DEFAULT_TIMESTAMP_PROPERTY, System.currentTimeMillis()));
        Job job = Job.getInstance(getConf(), "HalyardBulkLoad -> " + args[1] + " -> " + args[2]);
        job.setJarByClass(HalyardBulkLoad.class);
        job.setMapperClass(RDFMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        job.setInputFormatClass(RioFileInputFormat.class);
        job.setSpeculativeExecution(false);
        job.setReduceSpeculativeExecution(false);
        try (HTable hTable = HalyardTableUtils.getTable(getConf(), args[2], true, getConf().getInt(SPLIT_BITS_PROPERTY, 3))) {
            HFileOutputFormat2.configureIncrementalLoad(job, hTable.getTableDescriptor(), hTable.getRegionLocator());
            FileInputFormat.setInputDirRecursive(job, true);
            FileInputFormat.setInputPaths(job, args[0]);
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            TableMapReduceUtil.addDependencyJars(job);
            TableMapReduceUtil.initCredentials(job);
            if (job.waitForCompletion(true)) {
                if (getConf().getBoolean(TRUNCATE_PROPERTY, false)) {
                    HalyardTableUtils.truncateTable(hTable).close();
                }
                new LoadIncrementalHFiles(getConf()).doBulkLoad(new Path(args[1]), hTable);
                LOG.info("Bulk Load Completed..");
                return 0;
            }
        }
        return -1;
    }

    /**
     * MapReduce FileInputFormat reading and parsing any RDF4J RIO supported RDF format into Statements
     */
    public static final class RioFileInputFormat extends CombineFileInputFormat<LongWritable, Statement> {

        /**
         * Default constructor of RioFileInputFormat
         */
        public RioFileInputFormat() {
            //this is a workaround to avoid autodetection of .xml files as TriX format and hook on .trix file extension only
            RDFParserRegistry reg = RDFParserRegistry.getInstance();
            Optional<RDFParserFactory> trixPF = reg.get(RDFFormat.TRIX);
            if (trixPF.isPresent()) {
                reg.remove(trixPF.get());
                final RDFParser trixParser = trixPF.get().getParser();
                reg.add(new RDFParserFactory() {
                    @Override
                    public RDFFormat getRDFFormat() {
                        RDFFormat t = RDFFormat.TRIX;
                        return new RDFFormat(t.getName(), t.getMIMETypes(), t.getCharset(), Arrays.asList("trix"), t.getStandardURI(), t.supportsNamespaces(), t.supportsNamespaces());
                    }

                    @Override
                    public RDFParser getParser() {
                        return trixParser;
                    }
                });
            }
        }

        @Override
        protected boolean isSplitable(JobContext context, Path file) {
            return false;
        }

        @Override
        protected List<FileStatus> listStatus(JobContext job) throws IOException {
            List<FileStatus> filteredList = new ArrayList<>();
            for (FileStatus fs : super.listStatus(job)) {
                if (Rio.getParserFormatForFileName(fs.getPath().getName()) != null) {
                    filteredList.add(fs);
                }
            }
            return filteredList;
        }

        @Override
        public RecordReader<LongWritable, Statement> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
            return new RecordReader<LongWritable, Statement>() {

                private final AtomicLong key = new AtomicLong();

                private ParserPump pump = null;
                private Statement current = null;
                private Thread pumpThread = null;

                @Override
                public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
                    close();
                    pump = new ParserPump((CombineFileSplit)split, context);
                    pumpThread = new Thread(pump);
                    pumpThread.setDaemon(true);
                    pumpThread.start();
                }

                @Override
                public boolean nextKeyValue() throws IOException, InterruptedException {
                    if (pump == null) return false;
                    current = pump.getNext();
                    key.incrementAndGet();
                    return current != null;
                }

                @Override
                public LongWritable getCurrentKey() throws IOException, InterruptedException {
                    return current == null ? null : new LongWritable(key.get());
                }

                @Override
                public Statement getCurrentValue() throws IOException, InterruptedException {
                    return current;
                }

                @Override
                public float getProgress() throws IOException, InterruptedException {
                    return pump == null ? 0 : pump.getProgress();
                }

                @Override
                public void close() throws IOException {
                    if (pump != null) {
                        pump.close();
                        pump = null;
                    }
                    if (pumpThread != null) {
                        pumpThread.interrupt();
                        pumpThread = null;
                    }
                }
            };
        }
    }

    private static final IRI NOP = SimpleValueFactory.getInstance().createIRI(":");
    private static final Statement END_STATEMENT = SimpleValueFactory.getInstance().createStatement(NOP, NOP, NOP);

    private static final class ParserPump extends AbstractRDFHandler implements Closeable, Runnable {
        private final TaskAttemptContext context;
        private final Path paths[];
        private final long size;
        private final SynchronousQueue<Statement> queue = new SynchronousQueue<>();
        private final boolean skipInvalid;
        private Exception ex = null;
        private long finishedSize = 0;

        private String baseUri = "";
        private Seekable seek;
        private InputStream in;

        public ParserPump(CombineFileSplit split, TaskAttemptContext context) {
            this.context = context;
            this.paths = split.getPaths();
            this.size = split.getLength();
            this.skipInvalid = context.getConfiguration().getBoolean(SKIP_INVALID_PROPERTY, false);
        }

        public Statement getNext() throws IOException, InterruptedException {
            Statement s = queue.take();
            if (ex != null) synchronized (this) {
                throw new IOException("Exception while parsing: " + baseUri, ex);
            }
            return s == END_STATEMENT ? null : s;
        }

        public synchronized float getProgress() {
            try {
                return (float)(finishedSize + seek.getPos()) / (float)size;
            } catch (IOException e) {
                return (float)finishedSize / (float)size;
            }
        }

        @Override
        public void run() {
            try {
                Configuration conf = context.getConfiguration();
                for (Path file : paths) try {
                    RDFParser parser;
                    InputStream localIn;
                    String localBaseUri;
                    synchronized (this) {
                        if (seek != null) try {
                            finishedSize += seek.getPos();
                        } catch (IOException e) {
                            //ignore
                        }
                        close();
                        this.baseUri = file.toString();
                        context.setStatus("Parsing " + baseUri);
                        FileSystem fs = file.getFileSystem(conf);
                        FSDataInputStream fileIn = fs.open(file);
                        this.seek = fileIn;
                        CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(file);
                        if (codec != null) {
                            this.in = codec.createInputStream(fileIn, CodecPool.getDecompressor(codec));
                        } else {
                            this.in = fileIn;
                        }
                        parser = Rio.createParser(Rio.getParserFormatForFileName(baseUri).get());
                        parser.setRDFHandler(this);
                        parser.setStopAtFirstError(!skipInvalid);
                        localIn = this.in; //synchronised parameters must be copied to a local variable for use outide of sync block
                        localBaseUri = this.baseUri;
                    }
                    parser.parse(localIn, localBaseUri);
                } catch (Exception e) {
                    if (skipInvalid) {
                        LOG.log(Level.WARNING, "Exception while parsing RDF", e);
                    } else {
                        throw e;
                    }
                }
            } catch (Exception e) {
                ex = e;
            } finally {
                try {
                    queue.put(END_STATEMENT);
                } catch (InterruptedException ignore) {}
            }
        }

        @Override
        public void handleStatement(Statement st) throws RDFHandlerException {
            try {
                queue.put(st);
            } catch (InterruptedException e) {
                throw new RDFHandlerException(e);
            }
        }

        @Override
        public synchronized void close() throws IOException {
            if (in != null) {
                in.close();
                in = null;
            }
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
     * Main of the HalyardBulkLoad
     * @param args String command line arguments
     * @throws Exception throws Exception in case of any problem
     */
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new HalyardBulkLoad(), args));
    }
}
