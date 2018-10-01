/*
 * Copyright 2018 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
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
import com.yammer.metrics.core.Gauge;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.htrace.Trace;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;

/**
 *
 * @author Adam Sotona (MSD)
 */
public final class HalyardBulkDelete extends AbstractHalyardTool {

    private static final String SUBJECT = "halyard.delete.subject";
    private static final String PREDICATE = "halyard.delete.predicate";
    private static final String OBJECT = "halyard.delete.object";
    private static final String CONTEXTS = "halyard.delete.contexts";

    static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    static final class DeleteMapper extends TableMapper<ImmutableBytesWritable, KeyValue> {

        long total = 0, deleted = 0;
        Resource subj;
        IRI pred;
        Value obj;
        List<Resource> ctx;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            SimpleValueFactory vf = SimpleValueFactory.getInstance();
            Configuration conf = context.getConfiguration();
            String s = conf.get(SUBJECT);
            if (s!= null) {
                subj = NTriplesUtil.parseResource(s, vf);
            }
            String p = conf.get(PREDICATE);
            if (p!= null) {
                pred = NTriplesUtil.parseURI(p, vf);
            }
            String o = conf.get(OBJECT);
            if (o!= null) {
                obj = NTriplesUtil.parseValue(o, vf);
            }
            String cs[] = conf.getStrings(CONTEXTS);
            if (cs != null) {
                ctx = new ArrayList<>();
                for (String c : cs) {
                    if ("NONE".equals(c)) {
                        ctx.add(null);
                    } else {
                        ctx.add(NTriplesUtil.parseResource(c, vf));
                    }
                }
            }
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context output) throws IOException, InterruptedException {
            for (Cell c : value.rawCells()) {
                Statement st = HalyardTableUtils.parseStatement(c);
                if ((subj == null || subj.equals(st.getSubject())) && (pred == null || pred.equals(st.getPredicate())) && (obj == null || obj.equals(st.getObject())) && (ctx == null || ctx.contains(st.getContext()))) {
                    KeyValue kv = new KeyValue(c.getRowArray(), c.getRowOffset(), (int) c.getRowLength(),
                        c.getFamilyArray(), c.getFamilyOffset(), (int) c.getFamilyLength(),
                        c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength(),
                        c.getTimestamp(), KeyValue.Type.DeleteColumn, c.getValueArray(), c.getValueOffset(),
                        c.getValueLength(), c.getTagsArray(), c.getTagsOffset(), c.getTagsLength());
                    output.write(new ImmutableBytesWritable(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()), kv);
                    deleted++;
                } else {
                    output.progress();
                }
                if (total++ % 10000l == 0) {
                    String msg = MessageFormat.format("{0} / {1} cells deleted", deleted, total);
                    output.setStatus(msg);
                    LOG.log(Level.INFO, msg);
                }
            }

        }

    }

    public HalyardBulkDelete() {
        super(
            "bulkdelete",
            "Halyard Bulk Delete is a MapReduce application that effectively deletes large set of triples or whole named graphs, based on specified statement pattern and/or named graph(s).",
            "Example: halyard bulkdelete -t my_data -f bulkdelete_temp1 -s <http://whatever/mysubj> -g <http://whatever/mygraph1> -g <http://whatever/mygraph2>"
        );
        addOption("t", "target-dataset", "dataset_table", "HBase table with Halyard RDF store", true, true);
        addOption("f", "temp-folder", "temporary_folder", "Temporary folder for HBase files", true, true);
        addOption("s", "subject", "subject", "Optional subject to delete", false, true);
        addOption("p", "predicate", "predicate", "Optional predicate to delete", false, true);
        addOption("o", "object", "object", "Optional object to delete", false, true);
        addOption("g", "named-graph", "named_graph", "Optional named graph(s) to delete, NONE represents triples outside of any named graph", false, false);
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        String source = cmd.getOptionValue('t');
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
        Job job = Job.getInstance(getConf(), "HalyardDelete " + source);
        if (cmd.hasOption('s')) {
            job.getConfiguration().set(SUBJECT, cmd.getOptionValue('s'));
        }
        if (cmd.hasOption('p')) {
            job.getConfiguration().set(PREDICATE, cmd.getOptionValue('p'));
        }
        if (cmd.hasOption('o')) {
            job.getConfiguration().set(OBJECT, cmd.getOptionValue('o'));
        }
        if (cmd.hasOption('g')) {
            job.getConfiguration().setStrings(CONTEXTS, cmd.getOptionValues('g'));
        }
        job.setJarByClass(HalyardBulkDelete.class);
        TableMapReduceUtil.initCredentials(job);

        Scan scan = new Scan();
        scan.addFamily("e".getBytes(StandardCharsets.UTF_8));
        scan.setMaxVersions(1);
        scan.setBatch(10);
        scan.setAllowPartialResults(true);

        TableMapReduceUtil.initTableMapperJob(source,
            scan,
            DeleteMapper.class,
            ImmutableBytesWritable.class,
            LongWritable.class,
            job);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        job.setSpeculativeExecution(false);
        job.setMapSpeculativeExecution(false);
        job.setReduceSpeculativeExecution(false);
        try (HTable hTable = HalyardTableUtils.getTable(getConf(), source, false, 0)) {
            HFileOutputFormat2.configureIncrementalLoad(job, hTable.getTableDescriptor(), hTable.getRegionLocator());
            FileOutputFormat.setOutputPath(job, new Path(cmd.getOptionValue('f')));
            TableMapReduceUtil.addDependencyJars(job);
            if (job.waitForCompletion(true)) {
                new LoadIncrementalHFiles(getConf()).doBulkLoad(new Path(cmd.getOptionValue('f')), hTable);
                LOG.info("Bulk Delete Completed..");
                return 0;
            }
        }
        return -1;
    }
}
