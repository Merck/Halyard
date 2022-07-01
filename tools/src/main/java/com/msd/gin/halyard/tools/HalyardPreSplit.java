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

import static com.msd.gin.halyard.tools.HalyardBulkLoad.*;

import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.StatementIndex;
import com.msd.gin.halyard.tools.HalyardBulkLoad.RioFileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.helpers.NTriplesUtil;

/**
 * Apache Hadoop MapReduce Tool for calculating pre-splits of an HBase table before a large dataset bulk-load.
 * Splits are based on the keys of a sample of the data to be loaded.
 * @author Adam Sotona (MSD)
 */
public final class HalyardPreSplit extends AbstractHalyardTool {
	private static final String TOOL_NAME = "presplit";
    private static final String TABLE_PROPERTY = confProperty(TOOL_NAME, "table");
    private static final String SPLIT_LIMIT_PROPERTY = confProperty(TOOL_NAME, "limit");
    private static final String DECIMATION_FACTOR_PROPERTY = confProperty(TOOL_NAME, "decimation-factor");
    private static final String OVERWRITE_PROPERTY = confProperty(TOOL_NAME, "overwrite");
    private static final String MAX_VERSIONS_PROPERTY = confProperty(TOOL_NAME, "max-versions");

    private static final long DEFAULT_SPLIT_LIMIT = 40000000000l;
    private static final int DEFAULT_DECIMATION_FACTOR = 1000;
    private static final int DEFAULT_MAX_VERSIONS = 1;

    enum Counters {
    	STATEMENTS_PROCESSED,
    	TOTAL_SPLITS
    }

    /**
     * Mapper class transforming randomly selected sample of parsed Statement into set of HBase Keys and sizes
     */
    public final static class RDFDecimatingMapper extends Mapper<LongWritable, Statement, ImmutableBytesWritable, LongWritable> {

        private final ImmutableBytesWritable rowKey = new ImmutableBytesWritable();
        private final LongWritable keyValueLength = new LongWritable();
        private final Random random = new Random(0);
        private RDFFactory rdfFactory;
        private long counter = 0, next = 0;
        private int maxIncrement;
        private long stmtCount = 0L;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            rdfFactory = RDFFactory.create(conf);
            int decimationFactor = conf.getInt(DECIMATION_FACTOR_PROPERTY, DEFAULT_DECIMATION_FACTOR);
            maxIncrement = 2*decimationFactor - 1;
            // prefix splits
            for (byte b = 1; b < StatementIndex.Name.values().length; b++) {
                context.write(new ImmutableBytesWritable(new byte[] {b}), new LongWritable(1));
            }
        }

        @Override
        protected void map(LongWritable key, Statement value, final Context context) throws IOException, InterruptedException {
            if (counter++ == next) {
                next = counter + random.nextInt(maxIncrement);
                for (KeyValue keyValue: HalyardTableUtils.insertKeyValues(value.getSubject(), value.getPredicate(), value.getObject(), value.getContext(), 0, rdfFactory)) {
                    rowKey.set(keyValue.getRowArray(), keyValue.getRowOffset(), keyValue.getRowLength());
                    keyValueLength.set(keyValue.getLength());
                    context.write(rowKey, keyValueLength);
                }
                stmtCount++;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	context.getCounter(Counters.STATEMENTS_PROCESSED).increment(stmtCount);
        }
    }

    static final class PreSplitReducer extends Reducer<ImmutableBytesWritable, LongWritable, NullWritable, NullWritable>  {

        private final List<byte[]> splits = new ArrayList<>();
        private long size = 0, splitLimit;
        private int decimationFactor;
        private byte lastRegion = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            splitLimit = context.getConfiguration().getLong(SPLIT_LIMIT_PROPERTY, DEFAULT_SPLIT_LIMIT);
            decimationFactor = context.getConfiguration().getInt(DECIMATION_FACTOR_PROPERTY, DEFAULT_DECIMATION_FACTOR);
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
                size += val.get() * decimationFactor;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	context.getCounter(Counters.TOTAL_SPLITS).setValue(splits.size());
            Configuration conf = context.getConfiguration();
            TableName tableName = TableName.valueOf(conf.get(TABLE_PROPERTY));
            int maxVersions = conf.getInt(MAX_VERSIONS_PROPERTY, DEFAULT_MAX_VERSIONS);
            boolean overwrite = conf.getBoolean(OVERWRITE_PROPERTY, false);
            try (Connection conn = HalyardTableUtils.getConnection(conf)) {
            	if (overwrite) {
		    		try (Admin admin = conn.getAdmin()) {
		    			if (admin.tableExists(tableName)) {
		    				admin.disableTable(tableName);
		    				admin.deleteTable(tableName);
		    			}
		    		}
            	}
	            HalyardTableUtils.createTable(conn, tableName, splits.toArray(new byte[splits.size()][]), maxVersions).close();
            }
        }
    }

    public HalyardPreSplit() {
        super(
            TOOL_NAME,
            "Halyard Presplit is a MapReduce application designed to estimate optimal HBase region splits for big datasets before the Bulk Load. "
                + "Halyard PreSplit creates an empty HBase table based on calculations from the dataset sources sampling. "
                + "For very large datasets it is wise to calculate the pre-splits before the HBase table is created to allow more efficient following Bulk Load process of the data. "
                + "Optional definition or override of the named graph should be specified exactly the same as for the following Bulk Load process so the region presplits estimations are precise.\n"
                + "Halyard PreSplit consumes the same RDF data sources as Halyard Bulk Load.",
            "Example: halyard presplit -s hdfs://my_RDF_files -t mydataset"
        );
        addOption("s", "source", "source_paths", SOURCE_PATHS_PROPERTY, "Source path(s) with RDF files, more paths can be delimited by comma, the paths are recursively searched for the supported files", true, true);
        addOption("t", "target", "dataset_table", "Target HBase table with Halyard RDF store, optional HBase namespace of the target table must already exist", true, true);
        addOption("i", "allow-invalid", null, SKIP_INVALID_PROPERTY, "Optionally allow invalid IRI values (less overhead)", false, false);
        addOption("g", "default-named-graph", "named_graph", DEFAULT_CONTEXT_PROPERTY, "Optionally specify default target named graph", false, true);
        addOption("o", "named-graph-override", null, OVERRIDE_CONTEXT_PROPERTY, "Optionally override named graph also for quads, named graph is stripped from quads if --default-named-graph option is not specified", false, false);
        addOption("d", "decimation-factor", "decimation_factor", DECIMATION_FACTOR_PROPERTY, String.format("Optionally overide pre-split random decimation factor (default is %d)", DEFAULT_DECIMATION_FACTOR), false, true);
        addOption("l", "split-limit-size", "size", SPLIT_LIMIT_PROPERTY, String.format("Optionally override calculated split size (default is %d)", DEFAULT_SPLIT_LIMIT), false, true);
        addOption("f", "force", null, OVERWRITE_PROPERTY, "Overwrite existing table", false, false);
        addOption("n", "max-versions", "versions", MAX_VERSIONS_PROPERTY, String.format("Optionally set the maximum number of versions for the table (default is %d)", DEFAULT_MAX_VERSIONS), false, true);
    }

    @Override
    protected int run(CommandLine cmd) throws Exception {
    	configureString(cmd, 's', null);
        String target = cmd.getOptionValue('t');
        configureBoolean(cmd, 'f');
        if (!getConf().getBoolean(OVERWRITE_PROPERTY, false)) {
	        try (Connection con = ConnectionFactory.createConnection(getConf())) {
	            try (Admin admin = con.getAdmin()) {
	                if (admin.tableExists(TableName.valueOf(target))) {
	                    LOG.warn("Pre-split cannot modify already existing table {}", target);
	                    return -1;
	                }
	            }
	        }
        }
        configureBoolean(cmd, 'i');
        configureString(cmd, 'g', null);
        configureBoolean(cmd, 'o');
        configureInt(cmd, 'd', DEFAULT_DECIMATION_FACTOR);
        configureLong(cmd, 'l', DEFAULT_SPLIT_LIMIT);
        String sourcePaths = getConf().get(SOURCE_PATHS_PROPERTY);
        TableMapReduceUtil.addDependencyJarsForClasses(getConf(),
                NTriplesUtil.class,
                Rio.class,
                AbstractRDFHandler.class,
                RDFFormat.class,
                RDFParser.class);
        HBaseConfiguration.addHbaseResources(getConf());
        Job job = Job.getInstance(getConf(), "HalyardPreSplit -> " + target);
        job.getConfiguration().set(TABLE_PROPERTY, target);
        job.setJarByClass(HalyardPreSplit.class);
        job.setMapperClass(RDFDecimatingMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setInputFormatClass(RioFileInputFormat.class);
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.setInputPaths(job, sourcePaths);
        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.initCredentials(job);
        job.setReducerClass(PreSplitReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputFormatClass(NullOutputFormat.class);
        if (job.waitForCompletion(true)) {
            LOG.info("PreSplit Calculation completed.");
            return 0;
        } else {
    		LOG.error("PreSplit failed to complete.");
            return -1;
        }
    }
}
