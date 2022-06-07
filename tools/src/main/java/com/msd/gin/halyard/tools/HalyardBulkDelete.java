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

import static com.msd.gin.halyard.tools.HalyardBulkLoad.*;

import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.common.Keyspace;
import com.msd.gin.halyard.common.KeyspaceConnection;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.StatementIndex;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.MissingOptionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.helpers.NTriplesUtil;

/**
 *
 * @author Adam Sotona (MSD)
 */
public final class HalyardBulkDelete extends AbstractHalyardTool {
	static final String DEFAULT_GRAPH_KEYWORD = "DEFAULT";
    private static final String SOURCE = "halyard.delete.source";
    private static final String SNAPSHOT_PATH = "halyard.delete.snapshot";
    private static final String SUBJECT = "halyard.delete.subject";
    private static final String PREDICATE = "halyard.delete.predicate";
    private static final String OBJECT = "halyard.delete.object";
    private static final String CONTEXTS = "halyard.delete.contexts";

    enum Counters {
		REMOVED_KVS,
		TOTAL_KVS
	}

    static final class DeleteMapper extends RdfTableMapper<ImmutableBytesWritable, KeyValue> {

        final ImmutableBytesWritable rowKey = new ImmutableBytesWritable();
        long totalKvs = 0, deletedKvs = 0;
        long htimestamp;
        Resource subj;
        IRI pred;
        Value obj;
        List<Resource> ctx;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            openKeyspace(conf, conf.get(SOURCE), conf.get(SNAPSHOT_PATH));
            htimestamp = HalyardTableUtils.toHalyardTimestamp(conf.getLong(TIMESTAMP_PROPERTY, System.currentTimeMillis()), false);
            ValueFactory vf = rdfFactory.getIdValueFactory();
            String s = conf.get(SUBJECT);
            if (s != null) {
                subj = NTriplesUtil.parseResource(s, vf);
            }
            String p = conf.get(PREDICATE);
            if (p != null) {
                pred = NTriplesUtil.parseURI(p, vf);
            }
            String o = conf.get(OBJECT);
            if (o != null) {
                obj = NTriplesUtil.parseValue(o, vf);
            }
            String cs[] = conf.getStrings(CONTEXTS);
            if (cs != null) {
                ctx = new ArrayList<>();
                for (String c : cs) {
                    if (DEFAULT_GRAPH_KEYWORD.equals(c)) {
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
                Statement st = HalyardTableUtils.parseStatement(null, null, null, null, c, valueReader, rdfFactory);
                if ((subj == null || subj.equals(st.getSubject())) && (pred == null || pred.equals(st.getPredicate())) && (obj == null || obj.equals(st.getObject())) && (ctx == null || ctx.contains(st.getContext()))) {
                    KeyValue kv = new KeyValue(c.getRowArray(), c.getRowOffset(), (int) c.getRowLength(),
                        c.getFamilyArray(), c.getFamilyOffset(), (int) c.getFamilyLength(),
                        c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength(),
                        htimestamp, KeyValue.Type.DeleteColumn, c.getValueArray(), c.getValueOffset(),
                        c.getValueLength());
                    rowKey.set(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength());
                    output.write(rowKey, kv);
                    deletedKvs++;
                } else {
                    output.progress();
                }
                if (totalKvs++ % 10000l == 0) {
                    String msg = MessageFormat.format("{0} / {1} cells deleted", deletedKvs, totalKvs);
                    output.setStatus(msg);
                    LOG.info(msg);
                }
            }
        }

        @Override
        protected void cleanup(Context output) throws IOException {
        	output.getCounter(Counters.REMOVED_KVS).increment(deletedKvs);
        	output.getCounter(Counters.TOTAL_KVS).increment(totalKvs);
        	closeKeyspace();
        }
    }

    public HalyardBulkDelete() {
        super(
            "bulkdelete",
            "Halyard Bulk Delete is a MapReduce application that effectively deletes large set of triples or whole named graphs, based on specified statement pattern and/or named graph(s).",
            "Example: halyard bulkdelete -t my_data -w bulkdelete_temp1 -s <http://whatever/mysubj> -g <http://whatever/mygraph1> -g <http://whatever/mygraph2>"
        );
        addOption("t", "target-dataset", "dataset_table", "HBase table with Halyard RDF store to update", true, true);
        addOption("w", "work-dir", "shared_folder", "Temporary folder for HBase files", true, true);
        addOption("s", "subject", "subject", "Optional subject to delete", false, true);
        addOption("p", "predicate", "predicate", "Optional predicate to delete", false, true);
        addOption("o", "object", "object", "Optional object to delete", false, true);
        addOption("g", "named-graph", "named_graph", "Optional named graph(s) to delete, "+DEFAULT_GRAPH_KEYWORD+" represents triples outside of any named graph", false, false);
        addOption("e", "target-timestamp", "timestamp", "Optionally specify timestamp of all deleted records (default is actual time of the operation)", false, true);
        addOption("n", "snapshot-name", "snapshot_name", "Snapshot to read from. If specified then data is read from the snapshot instead of the table specified by -t and the results are written to the table. Requires -u.", false, true);
        addOption("u", "restore-dir", "restore_folder", "The snapshot restore folder on HDFS. Requires -n.", false, true);
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
    	if ((cmd.hasOption('n') && !cmd.hasOption('u')) || (!cmd.hasOption('n') && cmd.hasOption('u'))) {
    		throw new MissingOptionException("Both -n and -u must be specified to read from a snapshot");
    	}
        String target = cmd.getOptionValue('t');
    	boolean useSnapshot = cmd.hasOption('n') && cmd.hasOption('u');
        String source = useSnapshot ? cmd.getOptionValue('n') : target;
        getConf().set(SOURCE, source);
        getConf().setLong(TIMESTAMP_PROPERTY, cmd.hasOption('e') ? Long.parseLong(cmd.getOptionValue('e')) : System.currentTimeMillis());
        if (cmd.hasOption('u')) {
			FileSystem fs = CommonFSUtils.getRootDirFileSystem(getConf());
        	if (fs.exists(new Path(cmd.getOptionValue('u')))) {
        		throw new IOException("Snapshot restore directory already exists");
        	}
        	getConf().set(SNAPSHOT_PATH, cmd.getOptionValue('u'));
        }
        if (cmd.hasOption('s')) {
        	getConf().set(SUBJECT, cmd.getOptionValue('s'));
        }
        if (cmd.hasOption('p')) {
        	getConf().set(PREDICATE, cmd.getOptionValue('p'));
        }
        if (cmd.hasOption('o')) {
        	getConf().set(OBJECT, cmd.getOptionValue('o'));
        }
        if (cmd.hasOption('g')) {
        	getConf().setStrings(CONTEXTS, cmd.getOptionValues('g'));
        }
        TableMapReduceUtil.addDependencyJarsForClasses(getConf(),
            NTriplesUtil.class,
            Rio.class,
            AbstractRDFHandler.class,
            RDFFormat.class,
            RDFParser.class,
            Table.class,
            HBaseConfiguration.class,
            AuthenticationProtos.class);
        HBaseConfiguration.addHbaseResources(getConf());
        Job job = Job.getInstance(getConf(), "HalyardDelete " + source);
        job.setJarByClass(HalyardBulkDelete.class);
        TableMapReduceUtil.initCredentials(job);

        RDFFactory rdfFactory;
        Keyspace keyspace = HalyardTableUtils.getKeyspace(getConf(), source, cmd.getOptionValue('u'));
        try {
        	try (KeyspaceConnection kc = keyspace.getConnection()) {
        		rdfFactory = RDFFactory.create(kc);
        	}
		} finally {
			keyspace.close();
		}
        Scan scan = StatementIndex.scanAll(rdfFactory);
        keyspace.initMapperJob(
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
        TableName hTableName;
		try (Connection conn = HalyardTableUtils.getConnection(getConf())) {
			try (Table hTable = HalyardTableUtils.getTable(conn, target, false, 0)) {
				hTableName = hTable.getName();
				RegionLocator regionLocator = conn.getRegionLocator(hTableName);
				HFileOutputFormat2.configureIncrementalLoad(job, hTable.getDescriptor(), regionLocator);
			}
		}
		Path workDir = new Path(cmd.getOptionValue('w'));
        FileOutputFormat.setOutputPath(job, workDir);
        TableMapReduceUtil.addDependencyJars(job);
        try {
            if (job.waitForCompletion(true)) {
				BulkLoadHFiles.create(getConf()).bulkLoad(hTableName, workDir);
                LOG.info("Bulk Delete completed.");
                return 0;
            } else {
        		LOG.error("Bulk Delete failed to complete.");
                return -1;
            }
        } finally {
        	keyspace.destroy();
        }
    }
}
