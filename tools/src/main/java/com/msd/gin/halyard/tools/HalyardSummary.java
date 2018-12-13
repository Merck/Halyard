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
import java.util.Random;
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
import org.apache.hadoop.mapreduce.Partitioner;
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


    enum ReportType {
        ClassCardinality, PredicateCardinality, DomainCardinality, RangeCardinality, DomainAndRangeCardinality, RangeTypeCardinality, DomainAndRangeTypeCardinality, ClassesOverlapCardinality;

        final IRI IRI = SVF.createIRI(NAMESPACE, Character.toLowerCase(name().charAt(0)) + name().substring(1));
    }

    static final String NAMESPACE = "http://merck.github.io/Halyard/summary#";
    static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();
    static final IRI PREDICATE = SVF.createIRI(NAMESPACE, "predicate");
    static final IRI CLASS = SVF.createIRI(NAMESPACE, "class");
    static final IRI DOMAIN = SVF.createIRI(NAMESPACE, "domain");
    static final IRI RANGE = SVF.createIRI(NAMESPACE, "range");
    static final IRI RANGE_TYPE = SVF.createIRI(NAMESPACE, "rangeType");

    private static final String FILTER_NAMESPACE_PREFIX = "http://merck.github.io/Halyard/";
    private static final String SOURCE = "halyard.summary.source";
    private static final String TARGET = "halyard.summary.target";
    private static final String TARGET_GRAPH = "halyard.summary.target.graph";
    private static final String DECIMATION_FACTOR = "halyard.summary.decimation";
    private static final int DEFAULT_DECIMATION_FACTOR = 100;

    private static final Charset UTF8 = Charset.forName("UTF-8");


    static final class SummaryMapper extends TableMapper<ImmutableBytesWritable, LongWritable>  {

        private int decimationFactor;
        private final Random random = new Random(0);
        private Table table;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.mapContext = context;
            Configuration conf = context.getConfiguration();
            this.decimationFactor = conf.getInt(DECIMATION_FACTOR, DEFAULT_DECIMATION_FACTOR);
            if (table == null) {
                table = HalyardTableUtils.getTable(conf, conf.get(SOURCE), false, 0);
            }
        }

        private Set<Resource> queryForClasses(Value instance) throws IOException {
            if (instance instanceof Resource) {
                Set<Resource> res = new HashSet<>();
                Scan scan = HalyardTableUtils.scan((Resource)instance, RDF.TYPE, null, null);
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
        long objectCardinality = 0;
        long classCardinality = 0;
        Set<Resource> rangeClasses = null;
        Context mapContext = null;

        long counter = 0, ccCounter = 0, pcCounter = 0, pdCounter = 0, prCounter = 0, pdrCounter = 0, prltCounter = 0, pdrltCounter = 0, coCounter = 0;

        private void reportClassCardinality(Resource clazz, long cardinality) throws IOException, InterruptedException {
            report(ReportType.ClassCardinality, clazz, cardinality);
            ccCounter+=cardinality;
        }

        private void reportPredicateCardinality(IRI predicate, long cardinality) throws IOException, InterruptedException {
            report(ReportType.PredicateCardinality, predicate, cardinality);
            pcCounter+=cardinality;
        }

        private void reportPredicateDomain(IRI predicate, Resource domainClass) throws IOException, InterruptedException {
            report(ReportType.DomainCardinality, predicate, 1l, domainClass);
            pdCounter++;
        }

        private void reportPredicateRange(IRI predicate, Resource rangeClass, long cardinality) throws IOException, InterruptedException {
            report(ReportType.RangeCardinality, predicate, cardinality, rangeClass);
            prCounter += cardinality;
        }

        private void reportPredicateDomainAndRange(IRI predicate, Resource domainClass, Resource rangeClass) throws IOException, InterruptedException {
            report(ReportType.DomainAndRangeCardinality, predicate, 1l, domainClass, rangeClass);
            pdrCounter++;
        }

        private void reportPredicateRangeLiteralType(IRI predicate, IRI rangeLiteralDataType, long cardinality) throws IOException, InterruptedException {
            report(ReportType.RangeTypeCardinality, predicate, cardinality, rangeLiteralDataType == null ? XMLSchema.STRING : rangeLiteralDataType);
            prltCounter += cardinality;
        }

        private void reportPredicateDomainAndRangeLiteralType(IRI predicate, Resource domainClass, IRI rangeLiteralDataType) throws IOException, InterruptedException {
            report(ReportType.DomainAndRangeTypeCardinality, predicate, 1l, domainClass, rangeLiteralDataType == null ? XMLSchema.STRING : rangeLiteralDataType);
            pdrltCounter++;
        }

        private void reportClassesOverlap(Resource class1, Resource class2) throws IOException, InterruptedException {
            report(ReportType.ClassesOverlapCardinality, class1, 1l, class2);
            coCounter++;
        }

        private final ByteArrayOutputStream baos = new ByteArrayOutputStream(100000);
        private void report(ReportType type, Resource firstKey, long cardinality, Value ... otherKeys) throws IOException, InterruptedException {
            baos.reset();
            try (DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeByte(type.ordinal());
                dos.writeUTF(NTriplesUtil.toNTriplesString(firstKey));
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
                if (objectChange || !oldStatement.getSubject().equals(newStatement.getSubject())) {
                    if (RDF.TYPE.equals(oldStatement.getPredicate())) {
                        //subject change
                        classCardinality++;
                        for (Resource subjClass : queryForClasses(oldStatement.getSubject())) {
                            if ((oldStatement.getObject() instanceof Resource) && subjClass.stringValue().compareTo(oldStatement.getObject().stringValue()) < 0) {
                                reportClassesOverlap(subjClass, (Resource)oldStatement.getObject());
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
                        //subject change
                        objectCardinality++;
                        predicateCardinality++;
                        if (rangeClasses == null) {
                            //init after object change
                            rangeClasses = queryForClasses(oldStatement.getObject());
                        }
                        for (Resource domainClass : queryForClasses(oldStatement.getSubject())) {
                            reportPredicateDomain(oldStatement.getPredicate(), domainClass);
                            for (Resource rangeClass : rangeClasses) {
                                reportPredicateDomainAndRange(oldStatement.getPredicate(), domainClass, rangeClass);
                            }
                            if (oldStatement.getObject() instanceof Literal) {
                                reportPredicateDomainAndRangeLiteralType(oldStatement.getPredicate(), domainClass, ((Literal)oldStatement.getObject()).getDatatype());
                            }
                        }
                        if (objectChange) {
                            //object change
                            for (Resource objClass : rangeClasses) {
                                reportPredicateRange(oldStatement.getPredicate(), objClass, objectCardinality);
                            }
                            if (oldStatement.getObject() instanceof Literal) {
                                reportPredicateRangeLiteralType(oldStatement.getPredicate(), ((Literal)oldStatement.getObject()).getDatatype(), objectCardinality);
                            }
                            objectCardinality = 0;
                            rangeClasses = null;
                        }
                        if (predicateChange) {
                            //predicate change
                            reportPredicateCardinality(oldStatement.getPredicate(), predicateCardinality);
                            predicateCardinality = 0;
                        }
                    }
                }
            }
            oldStatement = newStatement;
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context output) throws IOException, InterruptedException {
            if (random.nextInt(decimationFactor) == 0) {
                statementChange(HalyardTableUtils.parseStatement(value.rawCells()[0]));
            }
            if (++counter % 10000 == 0) {
                output.setStatus(MessageFormat.format("{0} cc:{1} co:{2} pc:{3} pd:{4} pr:{5} pdr:{6} prlt:{7} pdrlt:{8}", counter, ccCounter, coCounter, pcCounter, pdCounter, prCounter, pdrCounter, prltCounter, pdrltCounter));
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

    static final class SummaryPartitioner extends Partitioner<ImmutableBytesWritable, LongWritable> {
        @Override
        public int getPartition(ImmutableBytesWritable key, LongWritable value, int numPartitions) {
            return key.get()[key.getOffset()] % numPartitions;
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

        private OutputStream out;
        private RDFWriter writer;
        private HBaseSail sail;
        private IRI namedGraph;
        private int decimationFactor;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String targetUrl = conf.get(TARGET);
            String ng = conf.get(TARGET_GRAPH);
            this.namedGraph = ng == null ? null : SVF.createIRI(ng);
            this.decimationFactor = conf.getInt(DECIMATION_FACTOR, DEFAULT_DECIMATION_FACTOR);
            sail = new HBaseSail(conf, conf.get(SOURCE), false, 0, true, 0, null, null);
            sail.initialize();
            targetUrl = MessageFormat.format(targetUrl, ReportType.values()[context.getTaskAttemptID().getTaskID().getId() % ReportType.values().length].name());
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


        private void write(Resource subj, IRI predicate, String resource) {
            write(subj, predicate, NTriplesUtil.parseResource(resource, SVF));
        }

        private void write(Resource subj, ReportType reportType, long count) {
            write(subj, reportType.IRI, SVF.createLiteral(63 - Long.numberOfLeadingZeros(count)));
        }

        private void write(Resource subj, IRI predicate, Value value) {
            writer.handleStatement(SVF.createStatement(subj, predicate, value, namedGraph));
        }

        private void copyDescription(Resource subject) {
            Statement dedup = null;
            try (CloseableIteration<? extends Statement, SailException> it = sail.getStatements(subject, null, null, true)) {
                while (it.hasNext()) {
                    Statement st = it.next();
                    if (!st.getPredicate().stringValue().startsWith(FILTER_NAMESPACE_PREFIX)) {
                        st = SVF.createStatement(st.getSubject(), st.getPredicate(), st.getObject(), namedGraph);
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
            long count = 0;
            for (LongWritable lw : values) {
                count += lw.get();
            }
            if (count > 0) try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(key.get(), key.getOffset(), key.getLength()))) {
                count *= decimationFactor;
                ReportType reportType = ReportType.values()[dis.readByte()];
                Resource firstKey = NTriplesUtil.parseResource(dis.readUTF(), SVF);
                IRI generatedRoot = SVF.createIRI(NAMESPACE, HalyardTableUtils.encode(HalyardTableUtils.hashKey(key.get())));
                switch (reportType) {
                    case ClassCardinality:
                        write(firstKey, reportType, count);
                        copyDescription(firstKey);
                        break;
                    case PredicateCardinality:
                        write(firstKey, reportType, count);
                        copyDescription(firstKey);
                        break;
                    case DomainCardinality:
                        write(generatedRoot, PREDICATE, firstKey);
                        write(generatedRoot, reportType, count);
                        write(generatedRoot, DOMAIN, dis.readUTF());
                        break;
                    case RangeCardinality:
                        write(generatedRoot, PREDICATE, firstKey);
                        write(generatedRoot, reportType, count);
                        write(generatedRoot, RANGE, dis.readUTF());
                        break;
                    case DomainAndRangeCardinality:
                        write(generatedRoot, PREDICATE, firstKey);
                        write(generatedRoot, reportType, count);
                        write(generatedRoot, DOMAIN, dis.readUTF());
                        write(generatedRoot, RANGE, dis.readUTF());
                        break;
                    case RangeTypeCardinality:
                        write(generatedRoot, PREDICATE, firstKey);
                        write(generatedRoot, reportType, count);
                        write(generatedRoot, RANGE_TYPE, dis.readUTF());
                        break;
                    case DomainAndRangeTypeCardinality:
                        write(generatedRoot, PREDICATE, firstKey);
                        write(generatedRoot, reportType, count);
                        write(generatedRoot, DOMAIN, dis.readUTF());
                        write(generatedRoot, RANGE_TYPE, dis.readUTF());
                        break;
                    case ClassesOverlapCardinality:
                        write(generatedRoot, CLASS, firstKey);
                        write(generatedRoot, reportType, count);
                        write(generatedRoot, CLASS, dis.readUTF());
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
            "Example: halyard summary -s my_dataset -g http://my_dataset_summary -t hdfs:/my_folder/my_dataset_summary-{0}.nq.gz");
        addOption("s", "source-dataset", "dataset_table", "Source HBase table with Halyard RDF store", true, true);
        addOption("t", "target-file", "target_url", "Target file to export the summary (instead of update) hdfs://<path>/<file_name>{0}.<RDF_ext>[.<compression>], usage of {0} pattern is optional and it will split target file into multiple summarization categories.", true, true);
        addOption("g", "summary-named-graph", "target_graph", "Optional target named graph of the exported graph summary", false, true);
        addOption("d", "decimation-factor", "decimation_factor", "Optionally overide summary random decimation factor (default is " + DEFAULT_DECIMATION_FACTOR + ")", false, true);
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
        if (cmd.hasOption('d')) job.getConfiguration().setInt(DECIMATION_FACTOR, Integer.parseInt(cmd.getOptionValue('d')));
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
        if (target.contains("{0}")) {
            job.setPartitionerClass(SummaryPartitioner.class);
            job.setNumReduceTasks(ReportType.values().length);
        } else {
            job.setNumReduceTasks(1);
        }
        job.setCombinerClass(SummaryCombiner.class);
        job.setReducerClass(SummaryReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        if (job.waitForCompletion(true)) {
            LOG.info("Summary Generation Completed..");
            return 0;
        }
        return -1;
    }
}
