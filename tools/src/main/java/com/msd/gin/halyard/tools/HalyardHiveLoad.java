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
import java.io.IOException;
import java.io.StringReader;
import java.util.logging.Logger;
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
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import static com.msd.gin.halyard.tools.HalyardBulkLoad.DEFAULT_TIMESTAMP_PROPERTY;

/**
 * MapReduce tool for bulk loading of RDF data (in any standard RDF form) from given Hive table and column
 * @author Adam Sotona (MSD)
 */
public class HalyardHiveLoad implements Tool {

    private static final String HIVE_DATA_COLUMN_INDEX_PROPERTY = "halyard.hive.data.column.index";

    /**
     * Base URI property used for parsed data
     */
    public static final String BASE_URI_PROPERTY = "halyard.base.uri";
    private static final String RDF_MIME_TYPE_PROPERTY = "halyard.rdf.mime.type";
    private static final Logger LOG = Logger.getLogger(HalyardHiveLoad.class.getName());

    private Configuration conf;

    /**
     * HiveMapper reads specified Hive table and column data and produces Halyard KeyValue pairs for HBase Reducers
     */
    public static class HiveMapper extends Mapper<WritableComparable<Object>, HCatRecord, ImmutableBytesWritable, KeyValue> {

        private IRI defaultRdfContext;
        private boolean overrideRdfContext;
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
            timestamp = conf.getLong(DEFAULT_TIMESTAMP_PROPERTY, System.currentTimeMillis());
        }

        @Override
        protected void map(WritableComparable<Object> key, HCatRecord value, final Context context) throws IOException, InterruptedException {
            String text = (String)value.get(dataColumnIndex);
            RDFParser parser = Rio.createParser(rdfFormat);
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
            } catch (RDFParseException | RDFHandlerException e) {
                throw new IOException(e);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: hiveload -D" + RDF_MIME_TYPE_PROPERTY + "='application/ld+json' [-D" + MRJobConfig.QUEUE_NAME + "=proofofconcepts] [-D" + HIVE_DATA_COLUMN_INDEX_PROPERTY + "=3] [-D" + BASE_URI_PROPERTY + "='http://my_base_uri/'] [-D" + HalyardBulkLoad.SPLIT_BITS_PROPERTY + "=8] [-D" + HalyardBulkLoad.DEFAULT_CONTEXT_PROPERTY + "=http://new_context] [-D" + HalyardBulkLoad.OVERRIDE_CONTEXT_PROPERTY + "=true] <hive_table_name> <output_path> <hbase_table_name>");
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
        Job job = Job.getInstance(getConf(), "HalyardHiveLoad -> " + args[1] + " -> " + args[2]);
        int i = args[0].indexOf('.');
        HCatInputFormat.setInput(job, i > 0 ? args[0].substring(0, i) : null, args[0].substring(i + 1));
        job.setJarByClass(HalyardHiveLoad.class);
        job.setMapperClass(HiveMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        job.setInputFormatClass(HCatInputFormat.class);
        job.setSpeculativeExecution(false);
        job.setReduceSpeculativeExecution(false);
        try (HTable hTable = HalyardTableUtils.getTable(getConf(), args[2], true, getConf().getInt(HalyardBulkLoad.SPLIT_BITS_PROPERTY, 3))) {
            HFileOutputFormat2.configureIncrementalLoad(job, hTable.getTableDescriptor(), hTable.getRegionLocator());
            FileInputFormat.setInputDirRecursive(job, true);
            FileInputFormat.setInputPaths(job, args[0]);
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            TableMapReduceUtil.addDependencyJars(job);
            TableMapReduceUtil.initCredentials(job);
            if (job.waitForCompletion(true)) {
                new LoadIncrementalHFiles(getConf()).doBulkLoad(new Path(args[1]), hTable);
                LOG.info("Bulk Load Completed..");
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
     * Main of the HalyardHiveLoad
     * @param args String command line arguments
     * @throws Exception throws Exception in case of any problem
     */
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new HalyardHiveLoad(), args));
    }
}
