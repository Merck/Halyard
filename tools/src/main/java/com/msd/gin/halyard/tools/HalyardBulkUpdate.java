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
import static com.msd.gin.halyard.tools.HalyardBulkLoad.DEFAULT_TIMESTAMP_PROPERTY;
import com.msd.gin.halyard.tools.TimeAwareHBaseSail.TimestampCallbackBinding;
import com.yammer.metrics.core.Gauge;
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
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.htrace.Trace;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
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
 * Apache Hadoop MapReduce tool for performing SPARQL Graph construct queries and then bulk loading the results back into HBase. Essentially, batch process queries
 * and bulk load the results.
 * @author Adam Sotona (MSD)
 */
public class HalyardBulkUpdate implements Tool {

    /**
     * String name of a custom SPARQL function to decimate parallel evaluation based on Mapper index
     */
    public static final String DECIMATE_FUNCTION_NAME = "decimateBy";

    /**
     * Property defining optional ElasticSearch index URL
     */
    public static final String ELASTIC_INDEX_URL = "halyard.elastic.index.url";

    static final String TIMESTAMP_BINDING_NAME = "HALYARD_TIMESTAMP_SPECIAL_VARIABLE";
    static final String TIMESTAMP_CALLBACK_BINDING_NAME = "HALYARD_TIMESTAMP_SPECIAL_CALLBACK_BINDING";

    /**
     * Full URI of a custom SPARQL function to decimate parallel evaluation based on Mapper index
     */
    public static final String DECIMATE_FUNCTION_URI = HALYARD.NAMESPACE + DECIMATE_FUNCTION_NAME;
    private static final String TABLE_NAME_PROPERTY = "halyard.table.name";
    private static final Logger LOG = Logger.getLogger(HalyardBulkUpdate.class.getName());
    private Configuration conf;

    /**
     * Mapper class performing SPARQL Graph query evaluation and producing Halyard KeyValue pairs for HBase BulkLoad Reducers
     */
    public static class SPARQLMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

        private String tableName;
        private String elasticIndexURL;
        private long defaultTimestamp;

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
                    int hash = Arrays.hashCode(Arrays.copyOfRange(args, 2, args.length));
                    return valueFactory.createLiteral(Math.floorMod(hash, size) == index);
                }
            });
            Configuration conf = context.getConfiguration();
            tableName = conf.get(TABLE_NAME_PROPERTY);
            elasticIndexURL = conf.get(ELASTIC_INDEX_URL);
            defaultTimestamp = conf.getLong(DEFAULT_TIMESTAMP_PROPERTY, System.currentTimeMillis());
        }

        @Override
        protected void map(LongWritable key, Text value, final Context context) throws IOException, InterruptedException {
            String query = StringEscapeUtils.unescapeJava(value.toString());
            int i = query.indexOf('\n');
            final String fistLine = i > 0 ? query.substring(0, i) : query;
            context.setStatus("Execution of: " + fistLine);
            final AtomicLong added = new AtomicLong();
            final AtomicLong removed = new AtomicLong();
            try {
                final HBaseSail sail = new TimeAwareHBaseSail(context.getConfiguration(), tableName, false, 0, true, 0, elasticIndexURL, new HBaseSail.Ticker() {
                    @Override
                    public void tick() {
                        context.progress();
                    }
                }) {
                    @Override
                    protected void put(KeyValue kv) throws IOException {
                        try {
                            context.write(new ImmutableBytesWritable(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()), kv);
                        } catch (InterruptedException ex) {
                            throw new IOException(ex);
                        }
                        if (added.incrementAndGet() % 1000l == 0) {
                            context.setStatus(fistLine + " - " + added.get() + " added " + removed.get() + " removed");
                            LOG.log(Level.INFO, "{0} KeyValues added and {1} removed", new Object[] {added.get(), removed.get()});
                        }
                    }

                    @Override
                    protected void delete(KeyValue kv) throws IOException {
                        kv = new KeyValue(kv.getRowArray(), kv.getRowOffset(), (int) kv.getRowLength(),
                                kv.getFamilyArray(), kv.getFamilyOffset(), (int) kv.getFamilyLength(),
                                kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength(),
                                kv.getTimestamp(), KeyValue.Type.DeleteColumn, kv.getValueArray(), kv.getValueOffset(),
                                kv.getValueLength(), kv.getTagsArray(), kv.getTagsOffset(), kv.getTagsLength());
                        try {
                            context.write(new ImmutableBytesWritable(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()), kv);
                        } catch (InterruptedException ex) {
                            throw new IOException(ex);
                        }
                        if (added.incrementAndGet() % 1000l == 0) {
                            context.setStatus(fistLine + " - " + added.get() + " added " + removed.get() + " removed");
                            LOG.log(Level.INFO, "{0} KeyValues added and {1} removed", new Object[] {added.get(), removed.get()});
                        }
                    }

                    @Override
                    protected long getDefaultTimeStamp() {
                        return defaultTimestamp;
                    }

                    @Override
                    public void clear(Resource... contexts) throws SailException {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void removeStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
                        throw new UnsupportedOperationException();
                    }
                };
                SailRepository rep = new SailRepository(sail);
                try {
                    rep.initialize();
                    Update upd = rep.getConnection().prepareUpdate(QueryLanguage.SPARQL, query);
                    ((MapBindingSet)upd.getBindings()).addBinding(new TimestampCallbackBinding());
                    LOG.log(Level.INFO, "Execution of: {0}", query);
                    context.setStatus(fistLine);
                    upd.execute();
                    context.setStatus(fistLine + " - " + added.get() + " added " + removed.get() + " removed");
                    LOG.log(Level.INFO, "Query finished with {0} KeyValues added and {1} removed", new Object[] {added.get(), removed.get()});
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
            System.err.println("Usage: bulkupdate [-D" + MRJobConfig.QUEUE_NAME + "=proofofconcepts] <input_file_with_SPARQL_queries> <output_path> <table_name>");
            return -1;
        }
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
        getConf().setStrings(TABLE_NAME_PROPERTY, args[2]);
        getConf().setLong(DEFAULT_TIMESTAMP_PROPERTY, getConf().getLong(DEFAULT_TIMESTAMP_PROPERTY, System.currentTimeMillis()));
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
