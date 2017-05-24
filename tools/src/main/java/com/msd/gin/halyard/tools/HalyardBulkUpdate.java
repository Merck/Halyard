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
import static com.msd.gin.halyard.tools.HalyardBulkLoad.DEFAULT_CONTEXT_PROPERTY;
import static com.msd.gin.halyard.tools.HalyardBulkLoad.OVERRIDE_CONTEXT_PROPERTY;
import com.msd.gin.halyard.sail.HBaseSail;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.eclipse.rdf4j.sail.SailException;

/**
 * Apache Hadoop MapReduce tool performing SPARQL Graph queries and BulkLoading results back into HBase
 * @author Adam Sotona (MSD)
 */
public class HalyardBulkUpdate implements Tool {

    /**
     * String name of a custom SPARQL function to decimate parallel evaluation based on Mapper index
     */
    public static final String DECIMATE_FUNCTION_NAME = "decimateBy";

    /**
     * Full URI of a custom SPARQL function to decimate parallel evaluation based on Mapper index
     */
    public static final String DECIMATE_FUNCTION_URI = HALYARD.NAMESPACE + DECIMATE_FUNCTION_NAME;
    private static final String TABLE_NAME_PROPERTY = "halyard.table.name";
    private static final String CHECK_BEFORE_WRITE_PROPERTY = "halyard.check.before.write";
    private static final Logger LOG = Logger.getLogger(HalyardBulkUpdate.class.getName());
    private Configuration conf;

    /**
     * Mapper class performing SPARQL Graph query evaluation and producing Halyard KeyValue pairs for HBase BulkLoad Reducers
     */
    public static class SPARQLMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

        private IRI defaultRdfContext;
        private boolean overrideRdfContext;
        private String tableName;
        private boolean checkBeforeWrite;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FunctionRegistry.getInstance().add(new Function() {
                @Override
                public String getURI() {
                        return DECIMATE_FUNCTION_URI;
                }

                @Override
                public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
                    if (args.length < 3) throw new ValueExprEvaluationException("Minimal number of arguments for " + DECIMATE_FUNCTION_URI + " function is 3");
                    if (!(args[0] instanceof Literal) || !(args[1] instanceof Literal)) throw new ValueExprEvaluationException("First two two arguments of " + DECIMATE_FUNCTION_URI + " function must be literals");
                    int index = ((Literal)args[0]).intValue();
                    int size = ((Literal)args[1]).intValue();
                    return valueFactory.createLiteral(Arrays.hashCode(args) % size == index);
                }
            });
            Configuration conf = context.getConfiguration();
            overrideRdfContext = conf.getBoolean(OVERRIDE_CONTEXT_PROPERTY, false);
            String defCtx = conf.get(DEFAULT_CONTEXT_PROPERTY);
            defaultRdfContext = defCtx == null ? null : SimpleValueFactory.getInstance().createIRI(defCtx);
            tableName = conf.get(TABLE_NAME_PROPERTY);
            checkBeforeWrite = conf.getBoolean(CHECK_BEFORE_WRITE_PROPERTY, false);
        }

        @Override
        protected void map(LongWritable key, Text value, final Context context) throws IOException, InterruptedException {
            String query = StringEscapeUtils.unescapeJava(value.toString());
            int i = query.indexOf('\n');
            final String fistLine = i > 0 ? query.substring(0, i) : query;
            context.setStatus("Execution of: " + fistLine);
            try {
                final HBaseSail sail = new HBaseSail(context.getConfiguration(), tableName, false, 0, true, 0, new HBaseSail.Ticker() {
                    @Override
                    public void tick() {
                        context.progress();
                    }
                });
                SailRepository rep = new SailRepository(sail);
                try {
                    rep.initialize();
                    GraphQuery gq = rep.getConnection().prepareGraphQuery(QueryLanguage.SPARQL, query);
                    LOG.log(Level.INFO, "Execution of: {0}", query);
                    context.setStatus(fistLine);
                    final AtomicLong counter = new AtomicLong();
                    final AtomicLong newCounter = new AtomicLong();
                    gq.evaluate(new AbstractRDFHandler() {
                        @Override
                        public void handleStatement(Statement statement) throws RDFHandlerException {
                            context.progress();
                            Resource rdfContext;
                            if (overrideRdfContext || (rdfContext = statement.getContext()) == null) {
                                rdfContext = defaultRdfContext;
                            }
                            try {
                                if (checkBeforeWrite) {
                                    try (CloseableIteration<? extends Statement, SailException> iter = sail.getStatements(statement.getSubject(), statement.getPredicate(), statement.getObject(), true, rdfContext)) {
                                        if (!iter.hasNext()) {
                                            newCounter.incrementAndGet();
                                            write(statement, rdfContext);
                                        }
                                    }
                                } else {
                                    newCounter.incrementAndGet();
                                    write(statement, rdfContext);
                                }
                                if (counter.incrementAndGet() % 1000l == 0) {
                                    context.setStatus(fistLine + " - " + newCounter.get() + "/" + counter.get());
                                    LOG.log(Level.INFO, "{0} new out of {1} statements", new Object[] {newCounter.get(), counter.get()});
                                }
                            } catch (IOException | InterruptedException | SailException ex) {
                                throw new RDFHandlerException(ex);
                            }
                        }
                        private void write(Statement statement, Resource rdfContext) throws IOException, InterruptedException {
                            for (KeyValue keyValue: HalyardTableUtils.toKeyValues(statement.getSubject(), statement.getPredicate(), statement.getObject(), rdfContext)) {
                                context.write(new ImmutableBytesWritable(keyValue.getRowArray(), keyValue.getRowOffset(), keyValue.getRowLength()), keyValue);
                            }
                        }
                    });
                    context.setStatus(fistLine + " - " + newCounter.get() + "/" + counter.get());
                    LOG.log(Level.INFO, "Query finished with {0} new out of {1} statements", new Object[] {newCounter.get(), counter.get()});
                } finally {
                    rep.shutDown();
                }
            } catch (RepositoryException | MalformedQueryException | QueryEvaluationException | RDFHandlerException ex) {
                LOG.log(Level.SEVERE, null, ex);
                throw new IOException(ex);
            }

        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: bulkupdate [-D" + MRJobConfig.QUEUE_NAME + "=proofofconcepts] [-D" + DEFAULT_CONTEXT_PROPERTY + "=http://new_context] [-D" + OVERRIDE_CONTEXT_PROPERTY + "=true] <input_file_with_SPARQL_queries> <output_path> <table_name>");
            return -1;
        }
        TableMapReduceUtil.addDependencyJars(getConf(),
                NTriplesUtil.class,
                Rio.class,
                RDFFormat.class,
                RDFParser.class);
        HBaseConfiguration.addHbaseResources(getConf());
        if (SnappyCodec.isNativeCodeLoaded()) {
            getConf().setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, true);
            getConf().setClass(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, SnappyCodec.class, CompressionCodec.class);
        }
        getConf().setDouble(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 1.0);
        getConf().setLong(MRJobConfig.TASK_TIMEOUT, 3600000l);
        getConf().setInt(MRJobConfig.IO_SORT_FACTOR, 100);
        getConf().setInt(MRJobConfig.IO_SORT_MB, 1000);
        getConf().setInt(FileInputFormat.SPLIT_MAXSIZE, 1000000000);
        getConf().setInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, 2048);
        getConf().setStrings(TABLE_NAME_PROPERTY, args[2]);
        Job job = Job.getInstance(getConf(), "HalyardBulkUpdate -> " + args[1] + " -> " + args[2]);
        NLineInputFormat.setNumLinesPerSplit(job, 1);
        job.setJarByClass(HalyardBulkUpdate.class);
        job.setMapperClass(SPARQLMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        job.setInputFormatClass(NLineInputFormat.class);
        job.setSpeculativeExecution(false);
        job.setReduceSpeculativeExecution(false);
        try (HTable hTable = HalyardTableUtils.getTable(getConf(), args[2], false, 0)) {
            HFileOutputFormat2.configureIncrementalLoad(job, hTable.getTableDescriptor(), hTable.getRegionLocator());
            FileInputFormat.setInputPaths(job, args[0]);
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            TableMapReduceUtil.addDependencyJars(job);
            TableMapReduceUtil.initCredentials(job);
            if (job.waitForCompletion(true)) {
                new LoadIncrementalHFiles(getConf()).doBulkLoad(new Path(args[1]), hTable);
                LOG.info("Bulk Update Completed..");
                return 0;
            }
        }
        return -1;
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
     * Main of the HalyardBulkUpdate
     * @param args String command line arguments
     * @throws Exception throws Exception in case of any problem
     */
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new HalyardBulkUpdate(), args));
    }
}
