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

import static com.msd.gin.halyard.tools.HalyardBulkUpdate.DECIMATE_FUNCTION_URI;
import com.yammer.metrics.core.Gauge;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.htrace.Trace;
import org.eclipse.rdf4j.model.Literal;
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
 * Apache Hadoop MapReduce tool for batch exporting of SPARQL queries.
 * @author Adam Sotona (MSD)
 */
public class HalyardBulkExport implements Tool {

    private static final Logger LOG = Logger.getLogger(HalyardBulkExport.class.getName());
    private static final String SOURCE = "halyard.bulkexport.source";
    private static final String TARGET = "halyard.bulkexport.target";
    private static final String JDBC_DRIVER = "halyard.bulkexport.jdbc.driver";
    private static final String JDBC_CLASSPATH = "halyard.bulkexport.jdbc.classpath";
    private static final String JDBC_PROPERTIES = "halyard.bulkexport.jdbc.properties";
    private Configuration conf;

    /**
     * Mapper class performing SPARQL Graph query evaluation and producing Halyard KeyValue pairs for HBase BulkLoad Reducers
     */
    public static class BulkExportMapper extends Mapper<NullWritable, Text, NullWritable, Void> {

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
        }

        @Override
        protected void map(NullWritable key, Text value, final Context context) throws IOException, InterruptedException {
            final String query = StringEscapeUtils.unescapeJava(value.toString());
            final String name = ((FileSplit)context.getInputSplit()).getPath().getName();
            int dot = name.indexOf('.');
            final String bName = dot > 0 ? name.substring(0, dot) : name;
            context.setStatus("Execution of: " + name);
            LOG.log(Level.INFO, "Execution of {0}:\n{1}", new Object[]{name, query});
            try {
                HalyardExport.StatusLog log = new HalyardExport.StatusLog() {
                    @Override
                    public void tick() {
                        context.progress();
                    }
                    @Override
                    public void logStatus(String status) {
                        context.setStatus(name + ": " + status);
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
                        props[i] = new String(Base64.decodeBase64(props[i]), StandardCharsets.UTF_8);
                    }
                }
                HalyardExport.export(cfg, log, cfg.get(SOURCE), query, MessageFormat.format(cfg.get(TARGET), bName), cfg.get(JDBC_DRIVER), drCp, props, false, cfg.get(HalyardBulkUpdate.ELASTIC_INDEX_URL));
            } catch (Exception e) {
                throw new IOException(e);
            }

        }
    }


    private static Option newOption(String opt, String argName, String description) {
        Option o = new Option(opt, null, argName != null, description);
        o.setArgName(argName);
        return o;
    }

    private static void printHelp(Options options) {
        new HelpFormatter().printHelp(100, "bulkexport", "Exports graph or table data from Halyard RDF store, using batch of SPARQL queries", options, "Example: bulkexport [-D" + MRJobConfig.NUM_MAPS + "=10] -s my_dataset -q hdfs:///myqueries/*.sparql -t hdfs:/my_folder/{0}.csv.gz", true);
    }

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(newOption("h", null, "Prints this help"));
        options.addOption(newOption("v", null, "Prints version"));
        options.addOption(newOption("s", "source_htable", "Source HBase table with Halyard RDF store"));
        options.addOption(newOption("q", "sparql_queries", "folder or path pattern with SPARQL tuple or graph queries"));
        options.addOption(newOption("t", "target_url", "file://<path>/{0}.<ext> or hdfs://<path>/{0}.<ext> or jdbc:<jdbc_connection>/{0}"));
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
                System.out.println("Halyard Bulk Export version " + p.getProperty("version", "unknown"));
                return 0;
            }
            if (!cmd.getArgList().isEmpty()) throw new HalyardExport.ExportException("Unknown arguments: " + cmd.getArgList().toString());
            for (char c : "sqt".toCharArray()) {
                if (!cmd.hasOption(c))  throw new HalyardExport.ExportException("Missing mandatory option: " + c);
            }
            for (char c : "sqtlc".toCharArray()) {
                String s[] = cmd.getOptionValues(c);
                if (s != null && s.length > 1)  throw new HalyardExport.ExportException("Multiple values for option: " + c);
            }
            String source = cmd.getOptionValue('s');
            String queryFiles = cmd.getOptionValue('q');


            String target = cmd.getOptionValue('t');
            if (!target.contains("{0}")) {
                throw new HalyardExport.ExportException("Bulk export target must contain '{0}' to be replaced by stripped filename of the actual SPARQL query.");
            }
            getConf().set(SOURCE, source);
            getConf().set(TARGET, target);
            String driver = cmd.getOptionValue('c');
            if (driver != null) {
                getConf().set(JDBC_DRIVER, driver);
            }
            String props[] = cmd.getOptionValues('p');
            if (props != null) {
                for (int i=0; i<props.length; i++) {
                    props[i] = Base64.encodeBase64String(props[i].getBytes(StandardCharsets.UTF_8));
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
            Job job = Job.getInstance(getConf(), "HalyardBulkExport " + source + " -> " + target);
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
            job.setJarByClass(HalyardBulkExport.class);
            job.setMaxMapAttempts(1);
            job.setMapperClass(BulkExportMapper.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(Void.class);
            job.setNumReduceTasks(0);
            job.setInputFormatClass(WholeFileTextInputFormat.class);
            FileInputFormat.setInputDirRecursive(job, true);
            FileInputFormat.setInputPaths(job, queryFiles);
            job.setOutputFormatClass(NullOutputFormat.class);
            TableMapReduceUtil.initCredentials(job);
            if (job.waitForCompletion(true)) {
                LOG.info("Bulk Export Completed..");
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
     * Main of the HalyardBulkUpdate
     * @param args String command line arguments
     * @throws Exception throws Exception in case of any problem
     */
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new HalyardBulkExport(), args));
    }

}
