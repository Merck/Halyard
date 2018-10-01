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
import static com.msd.gin.halyard.tools.HalyardBulkLoad.DEFAULT_CONTEXT_PROPERTY;
import static com.msd.gin.halyard.tools.HalyardBulkLoad.OVERRIDE_CONTEXT_PROPERTY;
import com.msd.gin.halyard.tools.HalyardBulkLoad.RioFileInputFormat;
import static com.msd.gin.halyard.tools.HalyardBulkLoad.SKIP_INVALID_PROPERTY;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import static com.msd.gin.halyard.tools.HalyardBulkLoad.DEFAULT_TIMESTAMP_PROPERTY;
import java.util.logging.Level;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * Apache Hadoop MapReduce Tool for calculating pre-splits of an HBase table before a large dataset bulk-load.
 * Splits are based on the keys of a sample of the data to be loaded.
 * @author Adam Sotona (MSD)
 */
public final class HalyardPreSplit extends AbstractHalyardTool {

    static final String TABLE_PROPERTY = "halyard.presplit.table";
    static final String SPLIT_LIMIT_PROPERTY = "halyard.presplit.limit";
    static final String DECIMATION_FACTOR_PROPERTY = "halyard.presplit.decimation";

    private static final long DEFAULT_SPLIT_LIMIT = 80000000l;
    private static final int DEFAULT_DECIMATION_FACTOR = 1000;

    /**
     * Mapper class transforming randomly selected sample of parsed Statement into set of HBase Keys and sizes
     */
    public final static class RDFDecimatingMapper extends Mapper<LongWritable, Statement, ImmutableBytesWritable, LongWritable> {

        private final Random random = new Random(0);
        private long counter = 0, next = 0;
        private int decimationFactor;
        private long timestamp;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            decimationFactor = conf.getInt(DECIMATION_FACTOR_PROPERTY, DEFAULT_DECIMATION_FACTOR);
            for (byte b = 1; b < 6; b++) {
                context.write(new ImmutableBytesWritable(new byte[] {b}), new LongWritable(1));
            }
            timestamp = conf.getLong(DEFAULT_TIMESTAMP_PROPERTY, System.currentTimeMillis());
        }

        @Override
        protected void map(LongWritable key, Statement value, final Context context) throws IOException, InterruptedException {
            if (counter++ == next) {
                next = counter + random.nextInt(decimationFactor);
                for (KeyValue keyValue: HalyardTableUtils.toKeyValues(value.getSubject(), value.getPredicate(), value.getObject(), value.getContext(), false, timestamp)) {
                    context.write(new ImmutableBytesWritable(keyValue.getRowArray(), keyValue.getRowOffset(), keyValue.getRowLength()), new LongWritable(keyValue.getLength()));
                }
            }
        }
    }

    static final class PreSplitReducer extends Reducer<ImmutableBytesWritable, LongWritable, NullWritable, NullWritable>  {

        private long size = 0, splitLimit;
        private byte lastRegion = 0;
        private final List<byte[]> splits = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            splitLimit = context.getConfiguration().getLong(SPLIT_LIMIT_PROPERTY, DEFAULT_SPLIT_LIMIT);
        }

        @Override
	public void reduce(ImmutableBytesWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            byte region = key.get()[key.getOffset()];
            if (lastRegion != region || size > splitLimit) {
                byte[] split = lastRegion != region ? new byte[]{region} : key.copyBytes();
                splits.add(split);
                context.setStatus("#" + splits.size() + " " + Arrays.toString(split));
                lastRegion = key.get()[key.getOffset()];
                size = 0;
            }
            for (LongWritable val : values) {
                    size += val.get();
            }
	}

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            HalyardTableUtils.getTable(conf, conf.get(TABLE_PROPERTY), true, splits.toArray(new byte[splits.size()][])).close();
        }
    }

    public HalyardPreSplit() {
        super(
            "presplit",
            "Halyard Presplit is a MapReduce application designed to estimate optimal HBase region splits for big datasets before the Bulk Load. "
                + "Halyard PreSplit creates an empty HBase table based on calculations from the dataset sources sampling. "
                + "For very large datasets it is wise to calculate the pre-splits before the HBase table is created to allow more efficient following Bulk Load process of the data. "
                + "Optional definition or override of the named graph should be specified exactly the same as for the following Bulk Load process so the region presplits estimations are precise.\n"
                + "Halyard PreSplit consumes the same RDF data sources as Halyard Bulk Load.",
            "Example: halyard presplit -s hdfs://my_RDF_files -t mydataset"
        );
        addOption("s", "source", "source_paths", "Source path(s) with RDF files, more paths can be delimited by comma, the paths are searched for the supported files recurrently", true, true);
        addOption("t", "target", "dataset_table", "Target HBase table with Halyard RDF store", true, true);
        addOption("i", "skip-invalid", null, "Optionally skip invalid source files and parsing errors", false, false);
        addOption("g", "default-named-graph", "named_graph", "Optionally specify default target named graph", false, true);
        addOption("o", "named-graph-override", null, "Optionally override named graph also for quads, named graph is stripped from quads if --default-named-graph option is not specified", false, false);
        addOption("d", "decimation-factor", "decimation_factor", "Optionally overide pre-split random decimation factor (default is 1000)", false, true);
        addOption("l", "split-limit-size", "size", "Optionally override calculated split size (default is 80000000)", false, true);
    }

    @Override
    protected int run(CommandLine cmd) throws Exception {
        String source = cmd.getOptionValue('s');
        String target = cmd.getOptionValue('t');
        try (Connection con = ConnectionFactory.createConnection(getConf())) {
            try (Admin admin = con.getAdmin()) {
                if (admin.tableExists(TableName.valueOf(target))) {
                    LOG.log(Level.WARNING, "Pre-split cannot modify already existing table {0}", target);
                    return -1;
                }
            }
        }
        getConf().setBoolean(SKIP_INVALID_PROPERTY, cmd.hasOption('i'));
        if (cmd.hasOption('g')) getConf().set(DEFAULT_CONTEXT_PROPERTY, cmd.getOptionValue('g'));
        getConf().setBoolean(OVERRIDE_CONTEXT_PROPERTY, cmd.hasOption('o'));
        TableMapReduceUtil.addDependencyJars(getConf(),
                NTriplesUtil.class,
                Rio.class,
                AbstractRDFHandler.class,
                RDFFormat.class,
                RDFParser.class);
        HBaseConfiguration.addHbaseResources(getConf());
        getConf().setLong(DEFAULT_TIMESTAMP_PROPERTY, getConf().getLong(DEFAULT_TIMESTAMP_PROPERTY, System.currentTimeMillis()));
        getConf().setInt(DECIMATION_FACTOR_PROPERTY, Integer.parseInt(cmd.getOptionValue('d', String.valueOf(DEFAULT_DECIMATION_FACTOR))));
        getConf().setLong(SPLIT_LIMIT_PROPERTY, Long.parseLong(cmd.getOptionValue('l', String.valueOf(DEFAULT_SPLIT_LIMIT))));
        Job job = Job.getInstance(getConf(), "HalyardPreSplit -> " + target);
         job.getConfiguration().set(TABLE_PROPERTY, target);
        job.setJarByClass(HalyardPreSplit.class);
        job.setMapperClass(RDFDecimatingMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setInputFormatClass(RioFileInputFormat.class);
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.setInputPaths(job, source);
        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.initCredentials(job);
        job.setReducerClass(PreSplitReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputFormatClass(NullOutputFormat.class);
        if (job.waitForCompletion(true)) {
            LOG.info("PreSplit Calculation Completed..");
            return 0;
        }
        return -1;
    }
}
