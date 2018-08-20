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

import com.msd.gin.halyard.sail.HBaseSail;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.RDFWriterRegistry;
import org.eclipse.rdf4j.rio.Rio;

/**
 * Command line tool to run SPARQL queries and export the results into various target systems. This class could be extended or modified to add new types of
 * export targets.
 * @author Adam Sotona (MSD)
 */
public final class HalyardExport extends AbstractHalyardTool {

    /**
     * A generic exception during export
     */
    public static final class ExportException extends Exception {
        private static final long serialVersionUID = 2946182537302463011L;

        /**
         * ExportException constructor
         * @param message String exception message
         */
        public ExportException(String message) {
            super(message);
        }

        /**
         * ExportException constructor
         * @param cause Throwable exception cause
         */
        public ExportException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * StatusLog is a simple service interface that is notified when some data are processed or status is changed.
     * It's purpose is to notify caller (for example MapReduce task) that the execution is still alive and about update of the status.
     */
    public interface StatusLog {

        /**
         * This method is called to notify that the process is still alive
         */
        public void tick();

        /**
         * This method is called whenever the status has changed
         * @param status String new status
         */
        public void logStatus(String status);
    }

    private static abstract class QueryResultWriter implements AutoCloseable {
        private final AtomicLong counter = new AtomicLong();
        private final StatusLog log;
        private long startTime;

        public QueryResultWriter(StatusLog log) {
            this.log = log;
        }

        public final void initTimer() {
            startTime = System.currentTimeMillis();
        }

        protected final long tick() {
            log.tick();
            long count = counter.incrementAndGet();
            if ((count % 10000l) == 0) {
                long time = System.currentTimeMillis();
                log.logStatus(MessageFormat.format("Exported {0} records/triples in average speed {1}/s", count, (1000 * count)/(time - startTime)));
            }
            return count;
        }

        public abstract void writeTupleQueryResult(TupleQueryResult queryResult) throws ExportException;
        public abstract void writeGraphQueryResult(GraphQueryResult queryResult) throws ExportException;
        @Override
        public final void close() throws ExportException {
            long time = System.currentTimeMillis()+1;
            long count = counter.get();
            log.logStatus(MessageFormat.format("Export finished with {0} records/triples in average speed {1}/s", count, (1000 * count)/(time - startTime)));
            closeWriter();
        }
        protected abstract void closeWriter() throws ExportException;
    }

    private static class CSVResultWriter extends QueryResultWriter {

        private static final char[] HEX_DIGIT = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};

        private static String escapeAndQuoteField(String field) {
            char fch[] = field.toCharArray();
            boolean quoted = fch.length == 0;
            StringBuilder sb = new StringBuilder();
            for (char c : fch) {
                if (c == '"') {
                    sb.append("\"\"");
                    quoted = true;
                } else if (c == '\n') {
                    sb.append("\\n");
                } else if (c == '\r') {
                    sb.append("\\r");
                } else if (c == '\\'){
                    sb.append("\\\\");
                } else if (c == ',') {
                    sb.append(',');
                    quoted = true;
                } else if (c < 32 || c > 126) {
                    sb.append("\\u");
                    sb.append(HEX_DIGIT[(c >> 12) & 0xF]);
                    sb.append(HEX_DIGIT[(c >>  8) & 0xF]);
                    sb.append(HEX_DIGIT[(c >>  4) & 0xF]);
                    sb.append(HEX_DIGIT[ c        & 0xF]);
                } else {
                    sb.append(c);
                }
            }
            if (quoted) {
                return "\"" + sb.toString() + "\"";
            } else {
                return sb.toString();
            }
        }

        private final Writer writer;

        public CSVResultWriter(StatusLog log, OutputStream out) {
            super(log);
            this.writer = new OutputStreamWriter(out, Charset.forName("UTF-8"));
        }

        @Override
        public void writeTupleQueryResult(TupleQueryResult queryResult) throws ExportException {
            try {
                List<String> bns = queryResult.getBindingNames();
                boolean first = true;
                for (String bn : bns) {
                    if (first) {
                        first = false;
                    } else {
                        writer.write(',');
                    }
                    writer.write(escapeAndQuoteField(bn));
                }
                writer.write('\n');
                while (queryResult.hasNext()) {
                    BindingSet bs = queryResult.next();
                    first = true;
                    for (String bn : bns) {
                        if (first) {
                            first = false;
                        } else {
                            writer.write(',');
                        }
                        Value v = bs.getValue(bn);
                        if (v != null) {
                            writer.write(escapeAndQuoteField(v.stringValue()));
                        }
                    }
                    writer.write('\n');
                    tick();
                }
            } catch (QueryEvaluationException | IOException e) {
                throw new ExportException(e);
            }
        }

        @Override
        public void writeGraphQueryResult(GraphQueryResult queryResult) throws ExportException {
            throw new ExportException("Graph query results cannot be written to CSV file.");
        }

        @Override
        public void closeWriter() throws ExportException {
            try {
                writer.close();
            } catch (IOException e) {
                throw new ExportException(e);
            }
        }
    }

    private static class RIOResultWriter extends QueryResultWriter {

        private final OutputStream out;
        private final RDFWriter writer;

        public RIOResultWriter(StatusLog log, RDFFormat rdfFormat, OutputStream out) {
            super(log);
            this.out = out;
            this.writer = Rio.createWriter(rdfFormat, out);
        }

        @Override
        public void writeTupleQueryResult(TupleQueryResult queryResult) throws ExportException {
            throw new ExportException("Tuple query results could not be written in RDF file.");
        }

        @Override
        public void writeGraphQueryResult(GraphQueryResult queryResult) throws ExportException {
            try {
                writer.startRDF();
                for (Map.Entry<String, String> me : queryResult.getNamespaces().entrySet()) {
                    writer.handleNamespace(me.getKey(), me.getValue());
                }
                while (queryResult.hasNext()) {
                    writer.handleStatement(queryResult.next());
                    tick();
                }
                writer.endRDF();
            } catch (QueryEvaluationException | RDFHandlerException e) {
                throw new ExportException(e);
            }
        }

        @Override
        public void closeWriter() throws ExportException {
            try {
                out.close();
            } catch (IOException e) {
                throw new ExportException(e);
            }
        }
    }

    private static class JDBCResultWriter extends QueryResultWriter {

        private static final Pattern TABLE_NAME_PATTERN = Pattern.compile("^[a-zA-Z_0-9\\.]+$");
        private static final Collection<Integer> DATE_TIME_TYPES = Arrays.asList(Types.DATE, Types.TIME, Types.TIMESTAMP);

        private final Connection con;
        private final String tableName;
        private final boolean trimTable;

        public JDBCResultWriter(StatusLog log, String dbUrl, String tableName, String[] connProps, final String driverClass, URL[] driverClasspath, boolean trimTable) throws ExportException {
            super(log);
            this.trimTable = trimTable;
            try {
                this.tableName = tableName;
                if (!TABLE_NAME_PATTERN.matcher(tableName).matches()) {
                    throw new ExportException("Illegal character(s) in table name: " + tableName);
                }
                final ArrayList<URL> urls = new ArrayList<>();
                if (driverClasspath != null) {
                    urls.addAll(Arrays.asList(driverClasspath));
                }
                Driver driver = AccessController.doPrivileged(new PrivilegedExceptionAction<Driver>() {
                    @Override
                    public Driver run() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
                        return (Driver)Class.forName(driverClass, true, new URLClassLoader(urls.toArray(new URL[urls.size()]))).newInstance();
                    }
                });
                Properties props = new Properties();
                if (connProps != null) {
                    for (String p : connProps) {
                        int i = p.indexOf('=');
                        if (i < 0) {
                            props.put(p, "true");
                        } else {
                            props.put(p.substring(0, i), p.substring(i + 1));
                        }
                    }
                }
                this.con = driver.connect(dbUrl, props);
            } catch (SQLException | PrivilegedActionException e) {
                throw new ExportException(e);
            }
        }

        @Override
        public void writeTupleQueryResult(TupleQueryResult queryResult) throws ExportException {
            try {
                List<String> bns = queryResult.getBindingNames();
                if (bns.size() < 1) return;
                con.setAutoCommit(false);
                if (trimTable) try (Statement s = con.createStatement()) {
                    s.execute("delete from " + tableName);
                }
                StringBuilder sb = new StringBuilder("select ").append(bns.get(0));
                for (int i = 1; i < bns.size(); i++) {
                    sb.append(',').append(bns.get(i));
                }
                sb.append(" from ").append(tableName);
                int columnTypes[] = new int[bns.size()];
                try (Statement s = con.createStatement()) {
                    try (ResultSet rs = s.executeQuery(sb.toString())) {
                        ResultSetMetaData meta = rs.getMetaData();
                        for (int i=0; i<meta.getColumnCount(); i++) {
                            columnTypes[i] = meta.getColumnType(i+1);
                        }
                    }
                }
                sb = new StringBuilder("insert into ").append(tableName).append(" (").append(bns.get(0));
                for (int i = 1; i < bns.size(); i++) {
                    sb.append(',').append(bns.get(i));
                }
                sb.append(") values (?");
                for (int i = 1; i < bns.size(); i++) {
                    sb.append(",?");
                }
                sb.append(')');
                try (PreparedStatement ps = con.prepareStatement(sb.toString())) {
                    while (queryResult.hasNext()) {
                        BindingSet bs = queryResult.next();
                        for (int i=0; i < bns.size(); i++) {
                            String bn = bns.get(i);
                            Value v = bs.getValue(bn);
                            if (v instanceof Literal && DATE_TIME_TYPES.contains(columnTypes[i])) {
                                ps.setTimestamp(i+1, new Timestamp(((Literal)v).calendarValue().toGregorianCalendar().getTimeInMillis()));
                            } else if (v instanceof Literal && columnTypes[i] == Types.FLOAT) {
                                ps.setFloat(i+1, ((Literal)v).floatValue());
                            } else if (v instanceof Literal && columnTypes[i] == Types.DOUBLE) {
                                ps.setDouble(i+1, ((Literal)v).doubleValue());
                            } else {
                                ps.setObject(i+1, v == null ? null : v.stringValue(), columnTypes[i]);
                            }
                        }
                        ps.addBatch();
                        if (tick() % 1000 == 0) {
                            for (int i : ps.executeBatch()) {
                                if (i != 1) throw new SQLException("Row has not been inserted for uknown reason");
                            }
                        }
                    }
                    for (int i : ps.executeBatch()) {
                        if (i != 1) throw new SQLException("Row has not been inserted for uknown reason");
                    }
                }
                con.commit();
            } catch (SQLException | QueryEvaluationException e) {
                throw new ExportException(e);
            }
        }

        @Override
        public void writeGraphQueryResult(GraphQueryResult queryResult) throws ExportException {
            throw new ExportException("Graph query results could not be written to JDBC table.");
        }

        @Override
        public void closeWriter() throws ExportException {
            try {
                con.close();
            } catch (SQLException e) {
                throw new ExportException(e);
            }
        }
    }

    private static class NullResultWriter extends QueryResultWriter {

        public NullResultWriter(StatusLog log) {
            super(log);
        }

        @Override
        public void writeTupleQueryResult(TupleQueryResult queryResult) throws ExportException {
            try {
                while (queryResult.hasNext()) {
                    queryResult.next();
                    tick();
                }
            } catch (QueryEvaluationException e) {
                throw new ExportException(e);
            }
        }

        @Override
        public void writeGraphQueryResult(GraphQueryResult queryResult) throws ExportException {
            try {
                while (queryResult.hasNext()) {
                    queryResult.next();
                    tick();
                }
            } catch (QueryEvaluationException e) {
                throw new ExportException(e);
            }
        }

        @Override
        protected void closeWriter() throws ExportException {
        }
    }

    /**
     * Export function is called for the export execution with given arguments.
     * @param conf Hadoop Configuration instance
     * @param log StatusLog notification service implementation for back-calls
     * @param source String source HTable name
     * @param query String SPARQL Graph query
     * @param targetUrl String URL of the target system (+folder or schema, +table or file name)
     * @param driverClass String JDBC Driver class name (for JDBC export only)
     * @param driverClasspath String with JDBC Driver classpath delimited by : (for DB export only)
     * @param jdbcProperties Arrays of String JDBC connection properties (for DB export only)
     * @param trimTable boolean option to trim target JDBC table before export (for DB export only)
     * @throws ExportException in case of an export problem
     */
    public static void export(Configuration conf, StatusLog log, String source, String query, String targetUrl, String driverClass, String driverClasspath, String[] jdbcProperties, boolean trimTable, String elasticIndexURL) throws ExportException {
        try {
            QueryResultWriter writer = null;
            if (targetUrl.startsWith("null:")) {
                writer = new NullResultWriter(log);
            } else if (targetUrl.startsWith("jdbc:")) {
                int i = targetUrl.lastIndexOf('/');
                if (i < 0) throw new ExportException("Taret URL does not end with /<table_name>");
                if (driverClass == null) throw new ExportException("Missing mandatory JDBC driver class name argument -c <driver_class>");
                URL driverCP[] = null;
                if (driverClasspath != null) {
                    String jars[] = driverClasspath.split(":");
                    driverCP = new URL[jars.length];
                    for (int j=0; j<jars.length; j++) {
                        File f = new File(jars[j]);
                        if (!f.isFile()) throw new ExportException("Invalid JDBC driver classpath element: " + jars[j]);
                        driverCP[j] = f.toURI().toURL();
                    }
                }
                writer = new JDBCResultWriter(log, targetUrl.substring(0, i), targetUrl.substring(i+1), jdbcProperties, driverClass, driverCP, trimTable);
            } else {
                OutputStream out = FileSystem.get(URI.create(targetUrl), conf).create(new Path(targetUrl));
                try {
                    if (targetUrl.endsWith(".bz2")) {
                        out = new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, out);
                        targetUrl = targetUrl.substring(0, targetUrl.length() - 4);
                    } else if (targetUrl.endsWith(".gz")) {
                        out = new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.GZIP, out);
                        targetUrl = targetUrl.substring(0, targetUrl.length() - 3);
                    }
                } catch (CompressorException e) {
                    IOUtils.closeQuietly(out);
                    throw new ExportException(e);
                }
                if (targetUrl.endsWith(".csv")) {
                    writer = new CSVResultWriter(log, out);
                } else {
                    Optional<RDFFormat> form = Rio.getWriterFormatForFileName(targetUrl);
                    if (!form.isPresent()) throw new ExportException("Unsupported target file format extension: " + targetUrl);
                    writer = new RIOResultWriter(log, form.get(), out);
                }
            }
            try {
                SailRepository rep = new SailRepository(new HBaseSail(conf, source, false, 0, true, 0, elasticIndexURL, null));
                rep.initialize();
                try {
                    writer.initTimer();
                    log.logStatus("Query execution started");
                    Query q = rep.getConnection().prepareQuery(QueryLanguage.SPARQL, query);
                    if (q instanceof TupleQuery) {
                        writer.writeTupleQueryResult(((TupleQuery)q).evaluate());
                    } else if (q instanceof GraphQuery) {
                        writer.writeGraphQueryResult(((GraphQuery)q).evaluate());
                    } else {
                        throw new ExportException("Only SPARQL Tuple and Graph query types are supported.");
                    }
                    log.logStatus("Export finished");
                } finally {
                    rep.shutDown();
                }
            } finally {
                writer.close();
            }
        } catch (RepositoryException | MalformedQueryException | QueryEvaluationException | IOException e) {
            throw new ExportException(e);
        }
    }

    private static String listRDFOut() {
        StringBuilder sb = new StringBuilder();
        for (RDFFormat fmt : RDFWriterRegistry.getInstance().getKeys()) {
            sb.append("* ").append(fmt.getName()).append(" (");
            boolean first = true;
            for (String ext : fmt.getFileExtensions()) {
                if (first) {
                    first = false;
                } else {
                    sb.append(", ");
                }
                sb.append('.').append(ext);
            }
            sb.append(")\n");
        }
        return sb.toString();
    }

    public HalyardExport() {
        super(
            "export",
            "Halyard Export is a command-line application designed to export data from HBase (a Halyard dataset) into various targets and formats.",
            "The exported data is determined by a SPARQL query. It can be either a SELECT query that produces a set of tuples (a table) or a CONSTRUCT/DESCRIBE query that produces a set of triples (a graph). "
                + "The supported target systems, query types, formats, and compressions are listed in the following table:\n"
                + "+-----------+----------+-------------------------------+---------------------------------------+\n"
                + "| Target    | Protocol | SELECT query                  | CONSTRUCT/DESCRIBE query              |\n"
                + "+-----------+----------+-------------------------------+---------------------------------------+\n"
                + "| Local FS  | file:    | .csv + compression            | RDF4J supported formats + compression |\n"
                + "| Hadoop FS | hdfs:    | .csv + compression            | RDF4J supported formats + compression |\n"
                + "| Database  | jdbc:    | direct mapping to tab.columns | not supported                         |\n"
                + "| Dry run   | null:    | .csv + compression            | RDF4J supported formats + compression |\n"
                + "+-----------+----------+-------------------------------+---------------------------------------+\n"
                + "Other Hadoop standard and optional filesystems (like s3:, s3n:, file:, ftp:, webhdfs:) may work according to the actual cluster configuration, however they have not been tested.\n"
                + "Optional compressions are:\n"
                + "* Bzip2 (.bz2)\n"
                + "* Gzip (.gz)\n"
                + "The RDF4J supported RDF formats are:\n"
                + listRDFOut()
                + "Example: halyard export -s my_dataset -q 'select * where {?subjet ?predicate ?object}' -t hdfs:/my_folder/my_data.csv.gz"
        );
        addOption("s", "source-dataset", "dataset_table", "Source HBase table with Halyard RDF store", true, true);
        addOption("q", "query", "sparql_query", "SPARQL tuple or graph query executed to export the data", true, true);
        addOption("t", "target-url", "target_url", "file://<path>/<file_name>.<ext> or hdfs://<path>/<file_name>.<ext> or jdbc:<jdbc_connection>/<table_name>", true, true);
        addOption("p", "jdbc-property", "property=value", "JDBC connection property, the most frequent JDBC connection properties are -p user=<jdbc_connection_username> and -p password=<jdbc_connection_password>`", false, false);
        addOption("l", "jdbc-driver-classpath", "driver_classpath", "JDBC driver classpath delimited by ':'", false, true);
        addOption("c", "jdbc-driver-class", "driver_class", "JDBC driver class name, mandatory for JDBC export", false, true);
        addOption("r", "trim", null, "Trim target table before export (apply for JDBC only)", false, false);
        addOption("e", "elastic-index", "elastic_index_url", "Optional ElasticSearch index URL", false, true);
    }

    @Override
    protected int run(CommandLine cmd) throws Exception {
        export(getConf(), new StatusLog() {
            @Override
            public void tick() {}

            @Override
            public void logStatus(String status) {
                LOG.info(status);
            }
        }, cmd.getOptionValue('s'), cmd.getOptionValue('q'), cmd.getOptionValue('t'), cmd.getOptionValue('c'), cmd.getOptionValue('l'), cmd.getOptionValues('p'), cmd.hasOption('r'), cmd.getOptionValue('e'));
        return 0;
    }
}
