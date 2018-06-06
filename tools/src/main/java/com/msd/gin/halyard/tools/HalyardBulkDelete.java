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
import com.msd.gin.halyard.tools.HalyardExport.ExportException;
import com.yammer.metrics.core.Gauge;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
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
public class HalyardBulkDelete implements Tool {

    private static final String SUBJECT = "halyard.delete.subject";
    private static final String PREDICATE = "halyard.delete.predicate";
    private static final String OBJECT = "halyard.delete.object";
    private static final String CONTEXTS = "halyard.delete.contexts";

    private static final Logger LOG = Logger.getLogger(HalyardBulkDelete.class.getName());

    static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    private Configuration conf;

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

    private static Option newOption(String opt, String argName, String description) {
        Option o = new Option(opt, null, argName != null, description);
        o.setArgName(argName);
        return o;
    }

    private static void printHelp(Options options) {
//        new HelpFormatter().printHelp(100, "stats", "Updates or exports statistics about Halyard dataset.", options, "Example: stats [-D" + MRJobConfig.QUEUE_NAME + "=proofofconcepts] [-D" + GRAPH_CONTEXT + "='http://whatever/mystats'] -s my_dataset [-t hdfs:/my_folder/my_stats.trig]", true);
    }

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(newOption("h", null, "Prints this help"));
        options.addOption(newOption("v", null, "Prints version"));
        options.addOption(newOption("t", "dataset_table", "HBase table with Halyard RDF store"));
        options.addOption(newOption("f", "temporary_folder", "Temporary folder for HBase files"));
        options.addOption(newOption("s", "subject", "Optional subject to delete"));
        options.addOption(newOption("p", "predicate", "Optional predicate to delete"));
        options.addOption(newOption("o", "object", "Optional object to delete"));
        options.addOption(newOption("c", "context", "Optional graph context to delete"));
        try {
            CommandLine cmd = new PosixParser().parse(options, args);
            if (args.length == 0 || cmd.hasOption('h')) {
                printHelp(options);
                return -1;
            }
            if (cmd.hasOption('v')) {
                Properties p = new Properties();
                try (InputStream in = HalyardBulkDelete.class.getResourceAsStream("/META-INF/maven/com.msd.gin.halyard/halyard-tools/pom.properties")) {
                    if (in != null) {
                        p.load(in);
                    }
                }
                System.out.println("Halyard Stats version " + p.getProperty("version", "unknown"));
                return 0;
            }
            if (!cmd.getArgList().isEmpty()) {
                throw new ExportException("Unknown arguments: " + cmd.getArgList().toString());
            }
            for (char c : "tf".toCharArray()) {
                if (!cmd.hasOption(c)) {
                    throw new ExportException("Missing mandatory option: " + c);
                }
            }
            for (char c : "tspo".toCharArray()) {
                String s[] = cmd.getOptionValues(c);
                if (s != null && s.length > 1) {
                    throw new ExportException("Multiple values for option: " + c);
                }
            }
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
            if (cmd.hasOption('c')) {
                job.getConfiguration().setStrings(CONTEXTS, cmd.getOptionValues('c'));
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
        } catch (RuntimeException exp) {
            System.out.println(exp.getMessage());
            printHelp(options);
            throw exp;
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
     * Main of the HalyardStats
     *
     * @param args String command line arguments
     * @throws Exception throws Exception in case of any problem
     */
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new HalyardBulkDelete(), args));
    }
}
