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

import com.msd.gin.halyard.sail.HALYARD;
import com.msd.gin.halyard.tools.HalyardExport.ExportException;
import com.yammer.metrics.core.Gauge;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.htrace.Trace;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;

/**
 * Map-only MapReduce tool providing the ability to export the results of a SPARQL query. A parallel version of {@link HalyardExport}
 * @author Adam Sotona (MSD)
 */
public class HalyardParallelExport implements Tool {

    /**
     * String name of the custom SPARQL parallel split filter function
     */
    public static final String PARALLEL_SPLIT_FUNCTION_NAME = "parallelSplitBy";

    /**
     * String full URI of the custom SPARQL parallel split filter function
     */
    public static final String PARALLEL_SPLIT_FUNCTION_URI = HALYARD.NAMESPACE + PARALLEL_SPLIT_FUNCTION_NAME;
    private static final String SOURCE = "halyard.parallelexport.source";
    private static final String QUERY = "halyard.parallelexport.query";
    private static final String TARGET = "halyard.parallelexport.target";
    private static final String JDBC_DRIVER = "halyard.parallelexport.jdbc.driver";
    private static final String JDBC_CLASSPATH = "halyard.parallelexport.jdbc.classpath";
    private static final String JDBC_PROPERTIES = "halyard.parallelexport.jdbc.properties";

    private static final Logger LOG = Logger.getLogger(HalyardParallelExport.class.getName());
    private static final Charset UTF8 = Charset.forName("UTF-8");


    private Configuration conf;

    static class IndexedInputSplit extends InputSplit implements Writable {

        public static IndexedInputSplit read(DataInput in) throws IOException {
            IndexedInputSplit iis = new IndexedInputSplit();
            iis.readFields(in);
            return iis;
        }

        public int index, size;

        public IndexedInputSplit() {
        }

        public IndexedInputSplit(int index, int size) {
            this.index = index;
            this.size = size;
        }

        public int getIndex() {
            return index;
        }

        public int getSize() {
            return size;
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return 0l;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[0];
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(index);
            out.writeInt(size);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            index = in.readInt();
            size = in.readInt();
        }

    }

    static class ParallelExportMapper extends Mapper<NullWritable, Void, NullWritable, Void> {

        @Override
        public void run(final Context context) throws IOException, InterruptedException {
            final IndexedInputSplit iis = (IndexedInputSplit)context.getInputSplit();
            if (iis.size == 0) throw new IllegalArgumentException("Invalid IndexedInputSplit instance.");
            FunctionRegistry.getInstance().add(new Function() {
                @Override
                public String getURI() {
                    return PARALLEL_SPLIT_FUNCTION_URI;
                }

                @Override
                public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
                    if (args == null || args.length == 0) {
                        throw new ValueExprEvaluationException("paralelSplitBy function has at least one mandatory argument");
                    }
                    for (Value v : args) {
                        if (v == null) {
                            throw new ValueExprEvaluationException("paralelSplitBy function does not allow null values");
                        }
                    }
                    return valueFactory.createLiteral(Math.floorMod(Arrays.hashCode(args), iis.size) == iis.index);
                }
            });
            try {
                HalyardExport.StatusLog log = new HalyardExport.StatusLog() {
                    @Override
                    public void tick() {
                        context.progress();
                    }
                    @Override
                    public void logStatus(String status) {
                        context.setStatus(status);
                    }
                };
                Configuration cfg = context.getConfiguration();
                String[] cp = cfg.getStrings(JDBC_CLASSPATH);
                URL[] drCp = null;
                if (cp != null) {
                    drCp = new URL[cp.length];
                    for (int i=0; i<cp.length; i++) {
                        drCp[i] = HalyardParallelExport.class.getResource(cp[i]);
                    }
                }
                String[] props = cfg.getStrings(JDBC_PROPERTIES);
                if (props != null) {
                    for (int i=0; i<props.length; i++) {
                        props[i] = new String(Base64.decodeBase64(props[i]), UTF8);
                    }
                }
                HalyardExport.export(cfg, log, cfg.get(SOURCE), cfg.get(QUERY), MessageFormat.format(cfg.get(TARGET), iis.index), cfg.get(JDBC_DRIVER), drCp, props, false, cfg.get(HalyardBulkUpdate.ELASTIC_INDEX_URL));
            } catch (Exception e) {
                throw new IOException(e);
            }

        }
    }

    static class IndexedInputFormat extends InputFormat<NullWritable, Void> {

        @Override
        public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
            int maps = context.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);
            ArrayList<InputSplit> splits = new ArrayList<>(maps);
            for (int i = 0; i < maps; i++) {
                splits.add(new IndexedInputSplit(i, maps));
            }
            return splits;
        }

        @Override
        public RecordReader<NullWritable, Void> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            return new RecordReader<NullWritable, Void>() {
                @Override
                public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
                }

                @Override
                public boolean nextKeyValue() throws IOException, InterruptedException {
                    return false;
                }

                @Override
                public NullWritable getCurrentKey() throws IOException, InterruptedException {
                    return null;
                }

                @Override
                public Void getCurrentValue() throws IOException, InterruptedException {
                    return null;
                }

                @Override
                public float getProgress() throws IOException, InterruptedException {
                    return 0;
                }

                @Override
                public void close() throws IOException {
                }
            };
        }

    }

    private static Option newOption(String opt, String argName, String description) {
        Option o = new Option(opt, null, argName != null, description);
        o.setArgName(argName);
        return o;
    }

    private static void printHelp(Options options) {
        new HelpFormatter().printHelp(100, "pexport", "Exports graph or table data from Halyard RDF store, using parallalel SPARQL query", options, "Example: pexport [-D" + MRJobConfig.NUM_MAPS + "=10] [-D" + MRJobConfig.QUEUE_NAME + "=proofofconcepts] -s my_dataset -q '\nPREFIX halyard: <" + HALYARD.NAMESPACE + ">\nselect * where {?s ?p ?o .\nFILTER (halyard:" + PARALLEL_SPLIT_FUNCTION_NAME + " (?s))}' -t hdfs:/my_folder/my_data{0}.csv.gz", true);
    }

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(newOption("h", null, "Prints this help"));
        options.addOption(newOption("v", null, "Prints version"));
        options.addOption(newOption("s", "source_htable", "Source HBase table with Halyard RDF store"));
        options.addOption(newOption("q", "sparql_query", "SPARQL tuple or graph query with use of '" + PARALLEL_SPLIT_FUNCTION_URI + "' function"));
        options.addOption(newOption("t", "target_url", "file://<path>/<file_name>{0}.<ext> or hdfs://<path>/<file_name>{0}.<ext> or jdbc:<jdbc_connection>/<table_name>"));
        options.addOption(newOption("p", "property=value", "JDBC connection properties"));
        options.addOption(newOption("l", "driver_classpath", "JDBC driver classpath delimited by ':'"));
        options.addOption(newOption("c", "driver_class", "JDBC driver class name"));
        try {
            CommandLine cmd = new PosixParser().parse(options, args);
            if (args.length == 0 || cmd.hasOption('h')) {
                printHelp(options);
                return -1;
            }
            if (cmd.hasOption('v')) {
                Properties p = new Properties();
                try (InputStream in = HalyardExport.class.getResourceAsStream("/META-INF/maven/com.msd.gin.halyard/halyard-tools/pom.properties")) {
                    if (in != null) p.load(in);
                }
                System.out.println("Halyard Parallel Export version " + p.getProperty("version", "unknown"));
                return 0;
            }
            if (!cmd.getArgList().isEmpty()) throw new ExportException("Unknown arguments: " + cmd.getArgList().toString());
            for (char c : "sqt".toCharArray()) {
                if (!cmd.hasOption(c))  throw new ExportException("Missing mandatory option: " + c);
            }
            for (char c : "sqtlc".toCharArray()) {
                String s[] = cmd.getOptionValues(c);
                if (s != null && s.length > 1)  throw new ExportException("Multiple values for option: " + c);
            }
            String source = cmd.getOptionValue('s');
            String query = cmd.getOptionValue('q');
            if (!query.contains(PARALLEL_SPLIT_FUNCTION_NAME)) {
                throw new ExportException("Parallel export SPARQL query must contain '" + PARALLEL_SPLIT_FUNCTION_URI + "' function.");
            }
            String target = cmd.getOptionValue('t');
            if ((target.startsWith("file:") || target.startsWith("hdfs:")) && !target.contains("{0}")) {
                throw new ExportException("Parallel export file target must contain '{0}' counter in the file path or name.");
            }
            getConf().set(SOURCE, source);
            getConf().set(QUERY, query);
            getConf().set(TARGET, target);
            String driver = cmd.getOptionValue('c');
            if (driver != null) {
                getConf().set(JDBC_DRIVER, driver);
            }
            String props[] = cmd.getOptionValues('p');
            if (props != null) {
                for (int i=0; i<props.length; i++) {
                    props[i] = Base64.encodeBase64String(props[i].getBytes(UTF8));
                }
                getConf().setStrings(JDBC_PROPERTIES, props);
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
            Job job = Job.getInstance(getConf(), "HalyardParallelExport " + source + " -> " + target);
            String cp = cmd.getOptionValue('l');
            if (cp != null) {
                String jars[] = cp.split(":");
                for (int i=0; i<jars.length; i++) {
                    Path p = new Path(jars[i]);
                    job.addFileToClassPath(p);
                    jars[i] = p.getName();
                }
                job.getConfiguration().setStrings(JDBC_CLASSPATH, jars);
            }
            job.setJarByClass(HalyardParallelExport.class);
            job.setMaxMapAttempts(1);
            job.setMapperClass(ParallelExportMapper.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(Void.class);
            job.setNumReduceTasks(0);
            job.setInputFormatClass(IndexedInputFormat.class);
            job.setOutputFormatClass(NullOutputFormat.class);
            TableMapReduceUtil.initCredentials(job);
            if (job.waitForCompletion(true)) {
                LOG.info("Parallel Export Completed..");
                return 0;
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
     * Main of the HalyardParallelExport
     * @param args String command line arguments
     * @throws Exception throws Exception in case of any problem
     */
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new HalyardParallelExport(), args));
    }
}
