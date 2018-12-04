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
import com.msd.gin.halyard.sail.HALYARD;
import com.msd.gin.halyard.sail.HBaseSail;
import com.yammer.metrics.core.Gauge;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.htrace.Trace;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.eclipse.rdf4j.sail.SailException;

/**
 * MapReduce tool providing summary of a Halyard dataset.
 * @author Adam Sotona (MSD)
 */
public final class HalyardSummary extends AbstractHalyardTool {

    private static final String NAMESPACE = "http://merck.github.io/Halyard/summary#";
    private static final String FILTER_NAMESPACE_PREFIX = "http://merck.github.io/Halyard/";
    private static final String SOURCE = "halyard.summary.source";
    private static final String TARGET = "halyard.summary.target";
    private static final String TARGET_GRAPH = "halyard.summary.target.graph";

    private static final Charset UTF8 = Charset.forName("UTF-8");

    static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    enum ReportType {
        ClassCardinality, PCardinality, PDomain, PRange, PDomainAndRange, PRangeLiteralType, PDomainAndRangeLiteralType, ClassesOverlap;
    }

    static final class SummaryMapper extends TableMapper<ImmutableBytesWritable, LongWritable>  {

        final SimpleValueFactory ssf = SimpleValueFactory.getInstance();

        Table table;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.mapContext = context;
            if (table == null) {
                Configuration conf = context.getConfiguration();
                table = HalyardTableUtils.getTable(conf, conf.get(SOURCE), false, 0);
            }
        }

        private Set<Resource> queryForClasses(Value instance) throws IOException {
            if (instance instanceof Resource) {
                Set<Resource> res = new HashSet<>();
                Scan scan = HalyardTableUtils.scan((Resource)instance, RDF.TYPE, null, null);
                scan.setSmall(true);
                try (ResultScanner scanner = table.getScanner(scan)) {
                    for (Result r : scanner) {
                        Statement st = HalyardTableUtils.parseStatement(r.rawCells()[0]);
                        if (st.getSubject().equals(instance) && st.getPredicate().equals(RDF.TYPE) && (st.getObject() instanceof Resource)) {
                            res.add((Resource)st.getObject());
                        }
                    }
                }
                return res;
            }
            return Collections.emptySet();
        }

        Statement oldStatement = null;
        long predicateCardinality = 0;
        long classCardinality = 0;
        Set<Resource> rangeClasses = Collections.emptySet();
        Context mapContext = null;

        private void reportClassCardinality(Resource clazz, long cardinality) throws IOException, InterruptedException {
            report(ReportType.ClassCardinality, clazz, cardinality);
        }

        private void reportPredicateCardinality(IRI predicate, long cardinality) throws IOException, InterruptedException {
            report(ReportType.PCardinality, predicate, cardinality);
        }

        private void reportPredicateDomain(IRI predicate, Resource domainClass) throws IOException, InterruptedException {
            report(ReportType.PDomain, predicate, 1l, domainClass);
        }

        private void reportPredicateRange(IRI predicate, Resource rangeClass) throws IOException, InterruptedException {
            report(ReportType.PRange, predicate, 1l, rangeClass);
        }

        private void reportPredicateDomainAndRange(IRI predicate, Resource domainClass, Resource rangeClass) throws IOException, InterruptedException {
            report(ReportType.PDomainAndRange, predicate, 1l, domainClass, rangeClass);
        }

        private void reportPredicateRangeLiteralType(IRI predicate, IRI rangeLiteralDataType) throws IOException, InterruptedException {
            report(ReportType.PRangeLiteralType, predicate, 1l, rangeLiteralDataType == null ? XMLSchema.STRING : rangeLiteralDataType);
        }

        private void reportPredicateDomainAndRangeLiteralType(IRI predicate, Resource domainClass, IRI rangeLiteralDataType) throws IOException, InterruptedException {
            report(ReportType.PDomainAndRangeLiteralType, predicate, 1l, domainClass, rangeLiteralDataType == null ? XMLSchema.STRING : rangeLiteralDataType);
        }

        private void reportClassesOverlap(Resource class1, Resource class2) throws IOException, InterruptedException {
            report(ReportType.ClassesOverlap, class1, 1l, class2);
        }

        private final ByteArrayOutputStream baos = new ByteArrayOutputStream(100000);
        private void report(ReportType type, Resource firstKey, long cardinality, Value ... otherKeys) throws IOException, InterruptedException {
            baos.reset();
            try (DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeUTF(NTriplesUtil.toNTriplesString(firstKey));
                dos.writeByte(type.ordinal());
                for (Value key : otherKeys) {
                    dos.writeUTF(NTriplesUtil.toNTriplesString(key));
                }
            }
            mapContext.write(new ImmutableBytesWritable(baos.toByteArray()), new LongWritable(cardinality));
        }

        private void statementChange(Statement newStatement) throws IOException, InterruptedException {
            if (oldStatement != null && !HALYARD.STATS_GRAPH_CONTEXT.equals(oldStatement.getContext())) {
                boolean predicateChange = newStatement == null || !oldStatement.getPredicate().equals(newStatement.getPredicate());
                boolean objectChange = predicateChange || !oldStatement.getObject().equals(newStatement.getObject());
                boolean subjectChange = objectChange || !oldStatement.getSubject().equals(newStatement.getSubject());
                if (RDF.TYPE.equals(oldStatement.getPredicate())) {
                    if (subjectChange) {
                        //subject change
                        classCardinality++;
                        for (Resource subjClass : queryForClasses(oldStatement.getSubject())) {
                            if ((oldStatement.getObject() instanceof Resource) && subjClass.stringValue().compareTo(oldStatement.getObject().stringValue()) < 0) {
                                reportClassesOverlap(subjClass, (Resource)oldStatement.getObject());
                            }
                        }
                    }
                    if (objectChange) {
                        //object change
                        if (oldStatement.getObject() instanceof Resource) {
                            reportClassCardinality((Resource)oldStatement.getObject(), classCardinality);
                        }
                        classCardinality = 0;
                    }
                } else if (!oldStatement.getPredicate().stringValue().startsWith(FILTER_NAMESPACE_PREFIX)) {
                    if (objectChange) {
                        //init of object change
                        rangeClasses = queryForClasses(oldStatement.getObject());
                        for (Resource objClass : rangeClasses) {
                            reportPredicateRange(oldStatement.getPredicate(), objClass);
                        }
                        if (oldStatement.getObject() instanceof Literal) {
                            reportPredicateRangeLiteralType(oldStatement.getPredicate(), ((Literal)oldStatement.getObject()).getDatatype());
                        }
                    }
                    if (subjectChange) {
                        //subject change
                        predicateCardinality++;
                        for (Resource domainClass : queryForClasses(oldStatement.getSubject())) {
                            reportPredicateDomain(oldStatement.getPredicate(), domainClass);
                            for (Resource rangeClass : rangeClasses) {
                                reportPredicateDomainAndRange(oldStatement.getPredicate(), domainClass, rangeClass);
                            }
                            if (oldStatement.getObject() instanceof Literal) {
                                reportPredicateDomainAndRangeLiteralType(oldStatement.getPredicate(), domainClass, ((Literal)oldStatement.getObject()).getDatatype());
                            }
                        }
                    }
                    if (predicateChange) {
                        //predicate change
                        reportPredicateCardinality(oldStatement.getPredicate(), predicateCardinality);
                        predicateCardinality = 0;
                    }
                }
            }
            oldStatement = newStatement;
        }

        long counter = 0;

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context output) throws IOException, InterruptedException {
            statementChange(HalyardTableUtils.parseStatement(value.rawCells()[0]));
            if (++counter % 1000 == 0) {
                output.setStatus(Long.toString(counter));
            }
        }

        @Override
        protected void cleanup(Context output) throws IOException, InterruptedException {
            statementChange(null);
            if (table != null) {
                table.close();;
                table = null;
            }
        }

    }

    static final class SummaryCombiner extends Reducer<ImmutableBytesWritable, LongWritable, ImmutableBytesWritable, LongWritable>  {
        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long cardinality = 0;
            for (LongWritable lw : values) {
                cardinality += lw.get();
            }
            if (cardinality > 0) {
                context.write(key, new LongWritable(cardinality));
            }
        }
    }

    static final class SummaryReducer extends Reducer<ImmutableBytesWritable, LongWritable, NullWritable, NullWritable>  {

        OutputStream out;
        RDFWriter writer;
        HBaseSail sail;
        IRI namedGraph;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String targetUrl = conf.get(TARGET);
            String ng = conf.get(TARGET_GRAPH);
            namedGraph = ng ==null ? null : SVF.createIRI(ng);

            sail = new HBaseSail(conf, conf.get(SOURCE), false, 0, true, 0, null, null);
            sail.initialize();
            targetUrl = MessageFormat.format(targetUrl, context.getTaskAttemptID().getTaskID().getId());
            out = FileSystem.get(URI.create(targetUrl), conf).create(new Path(targetUrl));
            try {
                if (targetUrl.endsWith(".bz2")) {
                    out = new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, out);
                    targetUrl = targetUrl.substring(0, targetUrl.length() - 4);
                } else if (targetUrl.endsWith(".gz")) {
                    out = new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.GZIP, out);
                    targetUrl = targetUrl.substring(0, targetUrl.length() - 3);
                }
            } catch (CompressorException ce) {
                throw new IOException(ce);
            }
            Optional<RDFFormat> form = Rio.getWriterFormatForFileName(targetUrl);
            if (!form.isPresent()) throw new IOException("Unsupported target file format extension: " + targetUrl);
            writer = Rio.createWriter(form.get(), out);
            writer.handleNamespace("", NAMESPACE);
            writer.handleNamespace(XMLSchema.PREFIX, XMLSchema.NAMESPACE);
            writer.handleNamespace(RDF.PREFIX, RDF.NAMESPACE);
            try (CloseableIteration<? extends Namespace, SailException> iter = sail.getNamespaces()) {
                while (iter.hasNext()) {
                    Namespace ns = iter.next();
                    writer.handleNamespace(ns.getPrefix(), ns.getName());
                }
            }
            writer.startRDF();
        }


        private void write(Resource subj, String halyardPredicate, String resource) {
            write(subj, halyardPredicate, NTriplesUtil.parseResource(resource, SVF));
        }

        private void write(Resource subj, String halyardPredicate, long value) {
            write(subj, halyardPredicate, SVF.createLiteral(value));
        }

        private void write(Resource subj, String halyardPredicate, Value value) {
            writer.handleStatement(SVF.createStatement(subj, SVF.createIRI(NAMESPACE, halyardPredicate), value, namedGraph));
        }

        private void copyDescription(Resource subject) {
            Statement dedup = null;
            try (CloseableIteration<? extends Statement, SailException> it = sail.getStatements(subject, null, null, true)) {
                while (it.hasNext()) {
                    Statement st = it.next();
                    if (!st.getPredicate().stringValue().startsWith(FILTER_NAMESPACE_PREFIX)) {
                        if (st.getContext() != null) {
                            st = SVF.createStatement(st.getSubject(), st.getPredicate(), st.getObject());
                        }
                        if (!st.equals(dedup)) {
                            writer.handleStatement(st);
                        }
                        dedup = st;
                    }
                }
            }
        }

        @Override
	public void reduce(ImmutableBytesWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long cardinality = 0;
            for (LongWritable lw : values) {
                cardinality += lw.get();
            }
            if (cardinality > 0) try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(key.get(), key.getOffset(), key.getLength()))) {
                Resource firstKey = NTriplesUtil.parseResource(dis.readUTF(), SVF);
                IRI generatedRoot = SVF.createIRI(NAMESPACE, HalyardTableUtils.encode(HalyardTableUtils.hashKey(key.get())));
                switch (ReportType.values()[dis.readByte()]) {
                    case ClassCardinality:
                        write(firstKey, "classCardinality", cardinality);
                        copyDescription(firstKey);
                        break;
                    case PCardinality:
                        write(firstKey, "predicateCardinality", cardinality);
                        copyDescription(firstKey);
                        break;
                    case PDomain:
                        write(generatedRoot, "predicate", firstKey);
                        write(generatedRoot, "domainCardinality", cardinality);
                        write(generatedRoot, "domain", dis.readUTF());
                        break;
                    case PRange:
                        write(generatedRoot, "predicate", firstKey);
                        write(generatedRoot, "rangeCardinality", cardinality);
                        write(generatedRoot, "range", dis.readUTF());
                        break;
                    case PDomainAndRange:
                        write(generatedRoot, "predicate", firstKey);
                        write(generatedRoot, "domainAndRangeCardinality", cardinality);
                        write(generatedRoot, "domain", dis.readUTF());
                        write(generatedRoot, "range", dis.readUTF());
                        break;
                    case PRangeLiteralType:
                        write(generatedRoot, "predicate", firstKey);
                        write(generatedRoot, "rangeTypeCardinality", cardinality);
                        write(generatedRoot, "rangeType", dis.readUTF());
                        break;
                    case PDomainAndRangeLiteralType:
                        write(generatedRoot, "predicate", firstKey);
                        write(generatedRoot, "domainAndRangeTypeCardinality", cardinality);
                        write(generatedRoot, "domain", dis.readUTF());
                        write(generatedRoot, "rangeType", dis.readUTF());
                        break;
                    case ClassesOverlap:
                        write(generatedRoot, "class", firstKey);
                        write(generatedRoot, "classesOverlapCardinality", cardinality);
                        write(generatedRoot, "class", dis.readUTF());
                }
            }

	}

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            writer.endRDF();
            out.close();
            sail.close();
        }
    }

    public HalyardSummary() {
        super(
            "summary",
            "Halyard Summary is a MapReduce application that calculates dataset summary and exports it into a file.",
            "Example: halyard summary -s my_dataset -n 10 -g http://my_dataset_summary -t hdfs:/my_folder/my_dataset_summary.nq.gz");
        addOption("s", "source-dataset", "dataset_table", "Source HBase table with Halyard RDF store", true, true);
        addOption("t", "target-file", "target_url", "Target file to export the statistics (instead of update) hdfs://<path>/<file_name>.<RDF_ext>[.<compression>]", true, true);
        addOption("g", "summary-named-graph", "target_graph", "Optional target named graph of the exported graph summary", false, true);
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        String source = cmd.getOptionValue('s');
        String target = cmd.getOptionValue('t');
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
        Job job = Job.getInstance(getConf(), "HalyardSummary " + source + (target == null ? " update" : " -> " + target));
        job.getConfiguration().set(SOURCE, source);
        if (target != null) job.getConfiguration().set(TARGET, target);
        if (cmd.hasOption('g')) job.getConfiguration().set(TARGET_GRAPH, cmd.getOptionValue('g'));
        job.setJarByClass(HalyardSummary.class);
        TableMapReduceUtil.initCredentials(job);

        Scan scan = new Scan(new byte[]{HalyardTableUtils.POS_PREFIX}, new byte[]{HalyardTableUtils.POS_PREFIX + 1});
        scan.addFamily("e".getBytes(UTF8));
        scan.setMaxVersions(1);
        scan.setBatch(100);
        scan.setAllowPartialResults(true);
        TableMapReduceUtil.initTableMapperJob(source,
                scan,
                SummaryMapper.class,
                ImmutableBytesWritable.class,
                LongWritable.class,
                job);
        job.setCombinerClass(SummaryCombiner.class);
        job.setReducerClass(SummaryReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputFormatClass(NullOutputFormat.class);
        if (job.waitForCompletion(true)) {
            LOG.info("Summary Generation Completed..");
            return 0;
        }
        return -1;
    }
}
