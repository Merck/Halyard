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
import com.msd.gin.halyard.common.StatementIndex;
import com.msd.gin.halyard.common.ValueIO;
import com.msd.gin.halyard.common.HalyardTableUtils.TableTripleReader;
import com.msd.gin.halyard.common.Hashes;
import com.msd.gin.halyard.common.RDFPredicate;
import com.msd.gin.halyard.common.RDFSubject;
import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.vocab.HALYARD;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.URI;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;

/**
 * MapReduce tool providing summary of a Halyard dataset.
 * @author Adam Sotona (MSD)
 */
public final class HalyardSummary extends AbstractHalyardTool {


    enum SummaryType {
        ClassSummary, PredicateSummary, DomainSummary, RangeSummary, DomainAndRangeSummary;
    }

    static final String NAMESPACE = "http://merck.github.io/Halyard/summary#";
    static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();
    static final IRI CARDINALITY = SVF.createIRI(NAMESPACE, "cardinality");

    private static final String FILTER_NAMESPACE_PREFIX = "http://merck.github.io/Halyard/";
    private static final String SOURCE = "halyard.summary.source";
    private static final String TARGET = "halyard.summary.target";
    private static final String TARGET_GRAPH = "halyard.summary.target.graph";
    private static final String DECIMATION_FACTOR = "halyard.summary.decimation";
    private static final int DEFAULT_DECIMATION_FACTOR = 100;

    static int toCardinality(long count) {
        return (63 - Long.numberOfLeadingZeros(count));
    }

    static IRI cardinalityIRI(String type, int cardinality) {
        return SVF.createIRI(NAMESPACE, type + cardinality);
    }

    static final class SummaryMapper extends TableMapper<ImmutableBytesWritable, LongWritable>  {

        private final ImmutableBytesWritable outputKey = new ImmutableBytesWritable();
        private final LongWritable outputValue = new LongWritable();
        private final Random random = new Random(0);
        private int decimationFactor;
        private Table table;
        private ValueIO.Reader valueReader;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.decimationFactor = conf.getInt(DECIMATION_FACTOR, DEFAULT_DECIMATION_FACTOR);
            this.table = HalyardTableUtils.getTable(conf, conf.get(SOURCE), false, 0);
            this.valueReader = new ValueIO.Reader(SVF, new TableTripleReader(table));
        }

        private Set<IRI> queryForClasses(Value instance) throws IOException {
            if (instance instanceof Resource) {
                Set<IRI> res = new HashSet<>();
                RDFSubject s = RDFSubject.create((Resource)instance);
                RDFPredicate p = RDFPredicate.create(RDF.TYPE);
                Scan scan = HalyardTableUtils.scan(s, p, null, null);
                try (ResultScanner scanner = table.getScanner(scan)) {
                    for (Result r : scanner) {
                        for (Statement st : HalyardTableUtils.parseStatements(s, p, null, null, r, valueReader)) {
	                        if (st.getSubject().equals(instance) && st.getPredicate().equals(RDF.TYPE) && (st.getObject() instanceof IRI)) {
	                            res.add((IRI)st.getObject());
	                        }
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
        Set<IRI> rangeClasses = null;

        long counter = 0, ccCounter = 0, pcCounter = 0, pdCounter = 0, prCounter = 0, pdrCounter = 0;

        private void reportClassCardinality(Context output, IRI clazz, long cardinality) throws IOException, InterruptedException {
            report(output, SummaryType.ClassSummary, clazz, cardinality);
            ccCounter+=cardinality;
        }

        private void reportPredicateCardinality(Context output, IRI predicate, long cardinality) throws IOException, InterruptedException {
            report(output, SummaryType.PredicateSummary, predicate, cardinality);
            pcCounter+=cardinality;
        }

        private void reportPredicateDomain(Context output, IRI predicate, IRI domainClass) throws IOException, InterruptedException {
            report(output, SummaryType.DomainSummary, predicate, 1l, domainClass);
            pdCounter++;
        }

        private void reportPredicateRange(Context output, IRI predicate, IRI rangeClass, long cardinality) throws IOException, InterruptedException {
            report(output, SummaryType.RangeSummary, predicate, cardinality, rangeClass);
            prCounter += cardinality;
        }

        private void reportPredicateDomainAndRange(Context output, IRI predicate, IRI domainClass, IRI rangeClass) throws IOException, InterruptedException {
            report(output, SummaryType.DomainAndRangeSummary, predicate, 1l, domainClass, rangeClass);
            pdrCounter++;
        }

        private final ByteArrayOutputStream baos = new ByteArrayOutputStream(100000);
        private void report(Context output, SummaryType type, IRI firstKey, long cardinality, IRI ... otherKeys) throws IOException, InterruptedException {
            baos.reset();
            try (DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeByte(type.ordinal());
                dos.writeUTF(firstKey.stringValue());
                for (Value key : otherKeys) {
                    dos.writeUTF(key.stringValue());
                }
            }
            outputKey.set(baos.toByteArray());
            outputValue.set(cardinality);
            output.write(outputKey, outputValue);
        }

        private void statementChange(Context output, Statement newStatement) throws IOException, InterruptedException {
            if (oldStatement != null && !HALYARD.STATS_GRAPH_CONTEXT.equals(oldStatement.getContext())) {
                boolean predicateChange = newStatement == null || !oldStatement.getPredicate().equals(newStatement.getPredicate());
                boolean objectChange = predicateChange || !oldStatement.getObject().equals(newStatement.getObject());
                if (objectChange || !oldStatement.getSubject().equals(newStatement.getSubject())) {
                    if (RDF.TYPE.equals(oldStatement.getPredicate())) {
                        //subject change
                        classCardinality++;
                        if (objectChange) {
                            //object change
                            if (oldStatement.getObject() instanceof IRI) {
                                reportClassCardinality(output, (IRI)oldStatement.getObject(), classCardinality);
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
                        for (IRI domainClass : queryForClasses(oldStatement.getSubject())) {
                            reportPredicateDomain(output, oldStatement.getPredicate(), domainClass);
                            for (IRI rangeClass : rangeClasses) {
                                reportPredicateDomainAndRange(output, oldStatement.getPredicate(), domainClass, rangeClass);
                            }
                            if (oldStatement.getObject() instanceof Literal) {
                                IRI lt = ((Literal)oldStatement.getObject()).getDatatype();
                                reportPredicateDomainAndRange(output, oldStatement.getPredicate(), domainClass, lt == null ? XSD.STRING : lt);
                            }
                        }
                        if (objectChange) {
                            //object change
                            for (IRI objClass : rangeClasses) {
                                reportPredicateRange(output, oldStatement.getPredicate(), objClass, objectCardinality);
                            }
                            if (oldStatement.getObject() instanceof Literal) {
                                IRI lt = ((Literal)oldStatement.getObject()).getDatatype();
                                reportPredicateRange(output, oldStatement.getPredicate(), lt == null ? XSD.STRING : lt, objectCardinality);
                            }
                            objectCardinality = 0;
                            rangeClasses = null;
                        }
                        if (predicateChange) {
                            //predicate change
                            reportPredicateCardinality(output, oldStatement.getPredicate(), predicateCardinality);
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
                statementChange(output, HalyardTableUtils.parseStatement(null, null, null, null, value.rawCells()[0], valueReader));
            }
            if (++counter % 10000 == 0) {
                output.setStatus(MessageFormat.format("{0} cc:{1} pc:{2} pd:{3} pr:{4} pdr:{5}", counter, ccCounter, pcCounter, pdCounter, prCounter, pdrCounter));
            }
        }

        @Override
        protected void cleanup(Context output) throws IOException, InterruptedException {
            statementChange(output, null);
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

        private Configuration conf;
        private OutputStream out = null;
        private FSDataOutputStream fsOut = null;
        private RDFWriter writer = null;
        private boolean splitOutput;
        private long outputLimit;
        private int outputCounter = 0;
        private HBaseSail sail;
		private SailConnection conn;
        private IRI namedGraph;
        private int decimationFactor;
        private final BitSet ccSet = new BitSet(64), pcSet = new BitSet(64), dcSet = new BitSet(64), rcSet = new BitSet(64), drcSet = new BitSet(64);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.conf = context.getConfiguration();
            this.splitOutput = conf.get(TARGET).contains("{0}");
            this.outputLimit = conf.getLong("mapreduce.input.fileinputformat.split.maxsize", Long.MAX_VALUE);
            String ng = conf.get(TARGET_GRAPH);
            this.namedGraph = ng == null ? null : SVF.createIRI(ng);
            this.decimationFactor = conf.getInt(DECIMATION_FACTOR, DEFAULT_DECIMATION_FACTOR);
            sail = new HBaseSail(conf, conf.get(SOURCE), false, 0, true, 0, null, null);
            sail.initialize();
			conn = sail.getConnection();
            setupOutput();
            write(CARDINALITY, RDF.TYPE, RDF.PROPERTY);
            write(CARDINALITY, RDFS.LABEL, SVF.createLiteral("cardinality"));
            write(CARDINALITY, RDFS.COMMENT, SVF.createLiteral("cardinality is positive integer [0..63], it is a binary logarithm of the pattern occurences (2^63 is the maximum allowed number of occurences)"));
            write(CARDINALITY, RDFS.RANGE, XSD.INTEGER);
        }

        private void setupOutput() throws IOException {
            String targetUrl = conf.get(TARGET);
            if (splitOutput || out == null) {
                if (out != null) {
                    writer.endRDF();
                    out.close();
                }
                targetUrl = MessageFormat.format(targetUrl, outputCounter++);
                fsOut = FileSystem.get(URI.create(targetUrl), conf).create(new Path(targetUrl));
                out = fsOut;
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
                writer.startRDF();
                writer.handleNamespace("", NAMESPACE);
                writer.handleNamespace(XSD.PREFIX, XSD.NAMESPACE);
                writer.handleNamespace(RDF.PREFIX, RDF.NAMESPACE);
                writer.handleNamespace(RDFS.PREFIX, RDFS.NAMESPACE);
				try (CloseableIteration<? extends Namespace, SailException> iter = conn.getNamespaces()) {
                    while (iter.hasNext()) {
                        Namespace ns = iter.next();
                        writer.handleNamespace(ns.getPrefix(), ns.getName());
                    }
                }
            }
        }

        private void write(IRI subj, IRI predicate, Value value) throws IOException {
            writer.handleStatement(SVF.createStatement(subj, predicate, value, namedGraph));
            if (splitOutput && fsOut.getPos() > outputLimit) {
                setupOutput();
            }
        }

        private void copyDescription(Resource subject) throws IOException {
            Statement dedup = null;
			try (CloseableIteration<? extends Statement, SailException> it = conn.getStatements(subject, null, null, true)) {
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
            if (splitOutput && fsOut.getPos() > outputLimit) {
                setupOutput();
            }
        }

        @Override
		public void reduce(ImmutableBytesWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable lw : values) {
                count += lw.get();
            }
            if (count > 0) {
            	try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(key.get(), key.getOffset(), key.getLength()))) {
	                int cardinality = toCardinality(count * decimationFactor);
	                SummaryType reportType = SummaryType.values()[dis.readByte()];
	                IRI firstKey = SVF.createIRI(dis.readUTF());
	                switch (reportType) {
	                    case ClassSummary:
	                        IRI ccClass = cardinalityIRI("Class", cardinality);
	                        if (!ccSet.get(cardinality)) {
	                            ccSet.set(cardinality);
	                            write(ccClass, RDFS.SUBCLASSOF, RDFS.CLASS);
	                            write(ccClass, RDFS.LABEL, SVF.createLiteral("rdfs:Class with cardinality " + cardinality));
	                            write(ccClass, CARDINALITY, SVF.createLiteral(BigInteger.valueOf(cardinality)));
	                        }
	                        write(firstKey, RDF.TYPE, ccClass);
	                        copyDescription(firstKey);
	                        break;
	                    case PredicateSummary:
	                        IRI pcPred = cardinalityIRI("Property", cardinality);
	                        if (!pcSet.get(cardinality)) {
	                            pcSet.set(cardinality);
	                            write(pcPred, RDFS.SUBCLASSOF, RDF.PROPERTY);
	                            write(pcPred, RDFS.LABEL, SVF.createLiteral("rdf:Property with cardinality " + cardinality));
	                            write(pcPred, CARDINALITY, SVF.createLiteral(BigInteger.valueOf(cardinality)));
	                        }
	                        write(firstKey, RDF.TYPE, pcPred);
	                        copyDescription(firstKey);
	                        break;
	                    case DomainSummary:
	                        IRI dcPred = cardinalityIRI("domain", cardinality);
	                        if (!dcSet.get(cardinality)) {
	                            dcSet.set(cardinality);
	                            write(dcPred, RDFS.SUBPROPERTYOF, RDFS.DOMAIN);
	                            write(dcPred, RDFS.LABEL, SVF.createLiteral("rdfs:domain with cardinality " + cardinality));
	                            write(dcPred, CARDINALITY, SVF.createLiteral(BigInteger.valueOf(cardinality)));
	                        }
	                        write(firstKey, dcPred, SVF.createIRI(dis.readUTF()));
	                        break;
	                    case RangeSummary:
	                        IRI rcPred = cardinalityIRI("range", cardinality);
	                        if (!rcSet.get(cardinality)) {
	                            rcSet.set(cardinality);
	                            write(rcPred, RDFS.SUBPROPERTYOF, RDFS.RANGE);
	                            write(rcPred, RDFS.LABEL, SVF.createLiteral("rdfs:range with cardinality " + cardinality));
	                            write(rcPred, CARDINALITY, SVF.createLiteral(BigInteger.valueOf(cardinality)));
	                        }
	                        write(firstKey, cardinalityIRI("range", cardinality), SVF.createIRI(dis.readUTF()));
	                        break;
	                    case DomainAndRangeSummary:
	                        IRI slicePPred = cardinalityIRI("sliceSubProperty", cardinality);
	                        IRI sliceDPred = cardinalityIRI("sliceDomain", cardinality);
	                        IRI sliceRPred = cardinalityIRI("sliceRange", cardinality);
	                        if (!drcSet.get(cardinality)) {
	                            drcSet.set(cardinality);
	                            write(slicePPred, RDFS.SUBPROPERTYOF, RDFS.SUBPROPERTYOF);
	                            write(slicePPred, RDFS.LABEL, SVF.createLiteral("slice rdfs:subPropertyOf with cardinality " + cardinality));
	                            write(slicePPred, CARDINALITY, SVF.createLiteral(BigInteger.valueOf(cardinality)));
	                            write(sliceDPred, RDFS.SUBPROPERTYOF, RDFS.DOMAIN);
	                            write(sliceDPred, RDFS.LABEL, SVF.createLiteral("slice rdfs:domain with cardinality " + cardinality));
	                            write(sliceDPred, CARDINALITY, SVF.createLiteral(BigInteger.valueOf(cardinality)));
	                            write(sliceRPred, RDFS.SUBPROPERTYOF, RDFS.RANGE);
	                            write(sliceRPred, RDFS.LABEL, SVF.createLiteral("slice rdfs:range with cardinality " + cardinality));
	                            write(sliceRPred, CARDINALITY, SVF.createLiteral(BigInteger.valueOf(cardinality)));
	                        }
	                        IRI generatedRoot = SVF.createIRI(NAMESPACE, Hashes.encode(Hashes.hashUnique(ByteBuffer.wrap(key.get(), key.getOffset(), key.getLength()))));
	                        write(generatedRoot, slicePPred, firstKey);
	                        write(generatedRoot, sliceDPred, SVF.createIRI(dis.readUTF()));
	                        write(generatedRoot, sliceRPred, SVF.createIRI(dis.readUTF()));
	                }
	            }
	        }

		}

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            writer.endRDF();
            out.close();
			conn.close();
			sail.shutDown();
        }
    }

    public HalyardSummary() {
        super(
            "summary",
            "Halyard Summary is an experimental MapReduce application that calculates dataset approximate summary in a form of self-described synthetic RDF schema and exports it into a file.",
            "Example: halyard summary -s my_dataset -g http://my_dataset_summary -t hdfs:/my_folder/my_dataset_summary-{0}.nq.gz");
        addOption("s", "source-dataset", "dataset_table", "Source HBase table with Halyard RDF store", true, true);
        addOption("t", "target-file", "target_url", "Target file to export the summary (instead of update) hdfs://<path>/<file_name>{0}.<RDF_ext>[.<compression>], usage of {0} pattern is optional and it will split output into multiple files for large summaries.", true, true);
        addOption("g", "summary-named-graph", "target_graph", "Optional target named graph of the exported graph summary", false, true);
        addOption("d", "decimation-factor", "decimation_factor", "Optionally overide summary random decimation factor (default is " + DEFAULT_DECIMATION_FACTOR + ")", false, true);
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        String source = cmd.getOptionValue('s');
        String target = cmd.getOptionValue('t');
        TableMapReduceUtil.addDependencyJarsForClasses(getConf(),
               Rio.class,
               AbstractRDFHandler.class,
               RDFFormat.class,
               RDFParser.class,
               Table.class,
               HBaseConfiguration.class,
               AuthenticationProtos.class);
        HBaseConfiguration.addHbaseResources(getConf());
        Job job = Job.getInstance(getConf(), "HalyardSummary " + source + (target == null ? " update" : " -> " + target));
        job.getConfiguration().set(SOURCE, source);
        if (target != null) job.getConfiguration().set(TARGET, target);
        if (cmd.hasOption('g')) job.getConfiguration().set(TARGET_GRAPH, cmd.getOptionValue('g'));
        if (cmd.hasOption('d')) job.getConfiguration().setInt(DECIMATION_FACTOR, Integer.parseInt(cmd.getOptionValue('d')));
        job.setJarByClass(HalyardSummary.class);
        TableMapReduceUtil.initCredentials(job);

        Scan scan = StatementIndex.POS.scan();

        TableMapReduceUtil.initTableMapperJob(source,
                scan,
                SummaryMapper.class,
                ImmutableBytesWritable.class,
                LongWritable.class,
                job);
        job.setNumReduceTasks(1);
        job.setCombinerClass(SummaryCombiner.class);
        job.setReducerClass(SummaryReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        if (job.waitForCompletion(true)) {
            LOG.info("Summary Generation completed.");
            return 0;
        } else {
    		LOG.error("Summary Generation failed to complete.");
            return -1;
        }
    }
}
