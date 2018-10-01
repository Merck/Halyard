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

import com.yammer.metrics.core.Gauge;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.logging.Level;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.htrace.Trace;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import static com.msd.gin.halyard.sail.HALYARD.PARALLEL_SPLIT_FUNCTION;

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
    public static final class BulkExportMapper extends Mapper<NullWritable, Void, NullWritable, Void> {

        @Override
        public void run(Context context) throws IOException, InterruptedException {
            final QueryInputFormat.QueryInputSplit qis = (QueryInputFormat.QueryInputSplit)context.getInputSplit();
            final String query = qis.getQuery();
            final String name = qis.getQueryName();
            int dot = name.indexOf('.');
            final String bName = dot > 0 ? name.substring(0, dot) : name;
            context.setStatus("Execution of: " + name);
            LOG.log(Level.INFO, "Execution of {0}:\n{1}", new Object[]{name, query});
            Function fn = new ParallelSplitFunction(qis.getRepeatIndex());
            FunctionRegistry.getInstance().add(fn);
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
                HalyardExport.export(cfg, log, cfg.get(SOURCE), query, MessageFormat.format(cfg.get(TARGET), bName, qis.getRepeatIndex()), cfg.get(JDBC_DRIVER), cfg.get(JDBC_CLASSPATH), props, false, cfg.get(HalyardBulkUpdate.ELASTIC_INDEX_URL));
            } catch (Exception e) {
                throw new IOException(e);
            } finally {
                FunctionRegistry.getInstance().remove(fn);
            }
        }
    }

    public HalyardBulkExport() {
        super("bulkexport",
            "Halyard Bulk Export is a MapReduce application that executes multiple Halyard Exports in MapReduce framework. "
                + "Query file name (without extension) can be used in the target URL pattern. Order of queries execution is not guaranteed. "
                + "Another internal level of parallelisation is done using a custom SPARQL function halyard:" + PARALLEL_SPLIT_FUNCTION.toString() + "(<constant_number_of_forks>, ?a_binding, ...). "
                + "The function takes one or more bindings as its arguments and these bindings are used as keys to randomly distribute the query evaluation across the executed parallel forks of the same query.",
            "Example: halyard bulkexport -s my_dataset -q hdfs:///myqueries/*.sparql -t hdfs:/my_folder/{0}-{1}.csv.gz"
        );
        addOption("s", "source-dataset", "dataset_table", "Source HBase table with Halyard RDF store", true, true);
        addOption("q", "queries", "sparql_queries", "folder or path pattern with SPARQL tuple or graph queries", true, true);
        addOption("t", "target-url", "target_url", "file://<path>/{0}-{1}.<ext> or hdfs://<path>/{0}-{1}.<ext> or jdbc:<jdbc_connection>/{0}, where {0} is replaced query filename (without extension) and {1} is replaced with parallel fork index (when " + PARALLEL_SPLIT_FUNCTION.toString() + " function is used in the particular query)", true, true);
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
        job.setInputFormatClass(QueryInputFormat.class);
        QueryInputFormat.setQueriesFromDirRecursive(job.getConfiguration(), queryFiles, false, 0);
        job.setOutputFormatClass(NullOutputFormat.class);
        TableMapReduceUtil.initCredentials(job);
        if (job.waitForCompletion(true)) {
            LOG.info("Bulk Export Completed..");
            return 0;
        }
        return -1;
    }

    private String addTmpFile(String file) throws IOException {
        String tmpFiles = getConf().get("tmpfiles");
        Path path = new Path(new File(file).toURI());
        getConf().set("tmpfiles", tmpFiles == null ? path.toString() : tmpFiles + "," + path.toString());
        return path.getName();
    }
}
