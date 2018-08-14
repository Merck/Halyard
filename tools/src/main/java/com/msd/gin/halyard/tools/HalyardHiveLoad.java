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
import static com.msd.gin.halyard.tools.AbstractHalyardTool.LOG;
import java.io.IOException;
import java.io.StringReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import java.util.logging.Level;
import org.apache.commons.cli.CommandLine;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;

/**
 * MapReduce tool for bulk loading of RDF data (in any standard RDF form) from given Hive table and column
 * @author Adam Sotona (MSD)
 */
public final class HalyardHiveLoad extends AbstractHalyardTool {

    private static final String HIVE_DATA_COLUMN_INDEX_PROPERTY = "halyard.hive.data.column.index";

    /**
     * Base URI property used for parsed data
     */
    public static final String BASE_URI_PROPERTY = "halyard.base.uri";
    private static final String RDF_MIME_TYPE_PROPERTY = "halyard.rdf.mime.type";

    /**
     * HiveMapper reads specified Hive table and column data and produces Halyard KeyValue pairs for HBase Reducers
     */
    public static final class HiveMapper extends Mapper<WritableComparable<Object>, HCatRecord, ImmutableBytesWritable, KeyValue> {

        private IRI defaultRdfContext;
        private boolean overrideRdfContext, skipInvalid, verifyDataTypeValues;
        private int dataColumnIndex;
        private RDFFormat rdfFormat;
        private String baseUri;
        private long timestamp;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            overrideRdfContext = conf.getBoolean(HalyardBulkLoad.OVERRIDE_CONTEXT_PROPERTY, false);
            String defCtx = conf.get(HalyardBulkLoad.DEFAULT_CONTEXT_PROPERTY);
            defaultRdfContext = defCtx == null ? null : SimpleValueFactory.getInstance().createIRI(defCtx);
            dataColumnIndex = conf.getInt(HIVE_DATA_COLUMN_INDEX_PROPERTY, 0);
            rdfFormat = Rio.getParserFormatForMIMEType(conf.get(RDF_MIME_TYPE_PROPERTY)).get();
            baseUri = conf.get(BASE_URI_PROPERTY);
            timestamp = conf.getLong(HalyardBulkLoad.DEFAULT_TIMESTAMP_PROPERTY, System.currentTimeMillis());
            skipInvalid = conf.getBoolean(HalyardBulkLoad.SKIP_INVALID_PROPERTY, false);
            verifyDataTypeValues = conf.getBoolean(HalyardBulkLoad.VERIFY_DATATYPE_VALUES_PROPERTY, false);
        }

        @Override
        protected void map(WritableComparable<Object> key, HCatRecord value, final Context context) throws IOException, InterruptedException {
            String text = (String)value.get(dataColumnIndex);
            RDFParser parser = Rio.createParser(rdfFormat);
            parser.setStopAtFirstError(!skipInvalid);
            parser.set(BasicParserSettings.VERIFY_DATATYPE_VALUES, verifyDataTypeValues);
            parser.setRDFHandler(new AbstractRDFHandler() {
                @Override
                public void handleStatement(Statement st) throws RDFHandlerException {
                    Resource rdfContext;
                    if (overrideRdfContext || (rdfContext = st.getContext()) == null) {
                        rdfContext = defaultRdfContext;
                    }
                    for (KeyValue keyValue: HalyardTableUtils.toKeyValues(st.getSubject(), st.getPredicate(), st.getObject(), rdfContext, false, timestamp)) try {
                        context.write(new ImmutableBytesWritable(keyValue.getRowArray(), keyValue.getRowOffset(), keyValue.getRowLength()), keyValue);
                    } catch (IOException | InterruptedException e) {
                        throw new RDFHandlerException(e);
                    }
                }
            });
            try {
                parser.parse(new StringReader(text), baseUri);
            } catch (Exception e) {
                if (skipInvalid) {
                    LOG.log(Level.WARNING, "Exception while parsing RDF", e);
                } else {
                    throw e;
                }
            }
        }
    }

    public HalyardHiveLoad() {
        super("hiveload", "loads a bulk of RDF fragments from Hive table using MapReduce framework", "Example: hiveload -s myHiveTable -c 3 -m 'application/ld+json' -u 'http://my_base_uri/' -w hdfs:///my_tmp_workdir -t mydataset");
        addOption("s", "source", "hive_table", "Source Hive table with RDF fragments", true, true);
        addOption("c", "column-index", "column_index", "Index of column with RDF fragments within the source Hive table", true, true);
        addOption("m", "mime-type", "mime_type", "MIME-Type of the RDF fragments", true, true);
        addOption("u", "base-uri", "base_uri", "Base URI for the RDF fragments", true, true);
        addOption("w", "work-dir", "shared_folder", "Unique non-existent folder within shared filesystem to server as a working directory for the job", true, true);
        addOption("t", "target", "dataset_table", "Target HBase table with Halyard RDF store", true, true);
        addOption("i", "skip-invalid", null, "Optionally skip invalid source files and parsing errors", false, false);
        addOption("d", "verify-data-types", null, "Optionally verify RDF data type values while parsing", false, false);
        addOption("r", "truncate-target", null, "Optionally truncate target table just before the loading the new data", false, false);
        addOption("b", "pre-split-bits", "bits", "Optionally specify bit depth of region pre-splits for a case when target table does not exist (default is 3)", false, true);
        addOption("g", "graph-context", "graph_context", "Optionally specify default target named graph context", false, true);
        addOption("o", "graph-context-override", null, "Optionally override named graph context also for loaded quads", false, false);
        addOption("e", "target-timestamp", "timestamp", "Optionally specify timestamp of all loaded records (defaul is actual time of the operation)", false, true);
    }

    @Override
    protected int run(CommandLine cmd) throws Exception {
        String source = cmd.getOptionValue('s');
        getConf().set(HIVE_DATA_COLUMN_INDEX_PROPERTY, cmd.getOptionValue('c'));
        getConf().set(RDF_MIME_TYPE_PROPERTY, cmd.getOptionValue('m'));
        getConf().set(BASE_URI_PROPERTY, cmd.getOptionValue('u'));
        String workdir = cmd.getOptionValue('w');
        String target = cmd.getOptionValue('t');
        getConf().setBoolean(HalyardBulkLoad.SKIP_INVALID_PROPERTY, cmd.hasOption('i'));
        getConf().setBoolean(HalyardBulkLoad.VERIFY_DATATYPE_VALUES_PROPERTY, cmd.hasOption('d'));
        getConf().setBoolean(HalyardBulkLoad.TRUNCATE_PROPERTY, cmd.hasOption('r'));
        getConf().setInt(HalyardBulkLoad.SPLIT_BITS_PROPERTY, Integer.parseInt(cmd.getOptionValue('b', "3")));
        if (cmd.hasOption('g')) getConf().set(HalyardBulkLoad.DEFAULT_CONTEXT_PROPERTY, cmd.getOptionValue('g'));
        getConf().setBoolean(HalyardBulkLoad.OVERRIDE_CONTEXT_PROPERTY, cmd.hasOption('o'));
        getConf().setLong(HalyardBulkLoad.DEFAULT_TIMESTAMP_PROPERTY, Long.parseLong(cmd.getOptionValue('e', String.valueOf(System.currentTimeMillis()))));
        TableMapReduceUtil.addDependencyJars(getConf(),
                NTriplesUtil.class,
                Rio.class,
                AbstractRDFHandler.class,
                RDFFormat.class,
                RDFParser.class);
        HBaseConfiguration.addHbaseResources(getConf());
        Job job = Job.getInstance(getConf(), "HalyardHiveLoad " + source + " -> " + workdir + " -> " + target);
        int i = source.indexOf('.');
        HCatInputFormat.setInput(job, i > 0 ? source.substring(0, i) : null, source.substring(i + 1));
        job.setJarByClass(HalyardHiveLoad.class);
        job.setMapperClass(HiveMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        job.setInputFormatClass(HCatInputFormat.class);
        job.setSpeculativeExecution(false);
        job.setReduceSpeculativeExecution(false);
        try (HTable hTable = HalyardTableUtils.getTable(getConf(), target, true, getConf().getInt(HalyardBulkLoad.SPLIT_BITS_PROPERTY, 3))) {
            HFileOutputFormat2.configureIncrementalLoad(job, hTable.getTableDescriptor(), hTable.getRegionLocator());
            FileOutputFormat.setOutputPath(job, new Path(workdir));
            TableMapReduceUtil.addDependencyJars(job);
            TableMapReduceUtil.initCredentials(job);
            if (job.waitForCompletion(true)) {
                if (getConf().getBoolean(HalyardBulkLoad.TRUNCATE_PROPERTY, false)) {
                    HalyardTableUtils.truncateTable(hTable).close();
                }
                new LoadIncrementalHFiles(getConf()).doBulkLoad(new Path(workdir), hTable);
                LOG.info("Hive Load Completed..");
                return 0;
            }
        }
        return -1;
    }

    /**
     * Main of the HalyardHiveLoad
     * @param args String command line arguments
     * @throws Exception throws Exception in case of any problem
     */
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new HalyardHiveLoad(), args));
    }
}
