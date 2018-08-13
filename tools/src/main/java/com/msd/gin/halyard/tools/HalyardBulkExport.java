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
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.logging.Level;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
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
public final class HalyardBulkExport extends AbstractHalyardTool {

    private static final String SOURCE = "halyard.bulkexport.source";
    private static final String TARGET = "halyard.bulkexport.target";
    private static final String JDBC_DRIVER = "halyard.bulkexport.jdbc.driver";
    private static final String JDBC_CLASSPATH = "halyard.bulkexport.jdbc.classpath";
    private static final String JDBC_PROPERTIES = "halyard.bulkexport.jdbc.properties";

    /**
     * Mapper class performing SPARQL Graph query evaluation and producing Halyard KeyValue pairs for HBase BulkLoad Reducers
     */
    public static final class BulkExportMapper extends Mapper<NullWritable, Text, NullWritable, Void> {

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
                String[] props = cfg.getStrings(JDBC_PROPERTIES);
                if (props != null) {
                    for (int i=0; i<props.length; i++) {
                        props[i] = new String(Base64.decodeBase64(props[i]), StandardCharsets.UTF_8);
                    }
                }
                HalyardExport.export(cfg, log, cfg.get(SOURCE), query, MessageFormat.format(cfg.get(TARGET), bName), cfg.get(JDBC_DRIVER), cfg.get(JDBC_CLASSPATH), props, false, cfg.get(HalyardBulkUpdate.ELASTIC_INDEX_URL));
            } catch (Exception e) {
                throw new IOException(e);
            }

        }
    }

    public HalyardBulkExport() {
        super(
            "bulkexport",
            "Exports graph or table data from Halyard RDF store, using batch of SPARQL queries",
            "Example: bulkexport -s my_dataset -q hdfs:///myqueries/*.sparql -t hdfs:/my_folder/{0}.csv.gz"
        );
        addOption("s", "source-dataset", "dataset_table", "Source HBase table with Halyard RDF store", true, true);
        addOption("q", "queries", "sparql_queries", "folder or path pattern with SPARQL tuple or graph queries", true, true);
        addOption("t", "target-url", "target_url", "file://<path>/{0}.<ext> or hdfs://<path>/{0}.<ext> or jdbc:<jdbc_connection>/{0}", true, true);
        addOption("p", "jdbc-property", "property=value", "JDBC connection property", false, false);
        addOption("l", "jdbc-driver-classpath", "driver_classpath", "JDBC driver classpath delimited by ':'", false, true);
        addOption("c", "jdbc-driver-class", "driver_class", "JDBC driver class name", false, true);
    }

    @Override
    protected int run(CommandLine cmd) throws Exception {
        if (!cmd.getArgList().isEmpty()) throw new HalyardExport.ExportException("Unknown arguments: " + cmd.getArgList().toString());
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
        String cp = cmd.getOptionValue('l');
        if (cp != null) {
            String jars[] = cp.split(":");
            StringBuilder newCp = new StringBuilder();
            for (int i=0; i<jars.length; i++) {
                if (i > 0) newCp.append(':');
                newCp.append(addTmpFile(jars[i])); //append clappspath entris to tmpfiles and trim paths from the classpath
            }
            getConf().set(JDBC_CLASSPATH, newCp.toString());
        }
        Job job = Job.getInstance(getConf(), "HalyardBulkExport " + source + " -> " + target);
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
    }

     /**
     * Main of the HalyardBulkUpdate
     * @param args String command line arguments
     * @throws Exception throws Exception in case of any problem
     */
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new HalyardBulkExport(), args));
    }

}
