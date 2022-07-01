package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.common.Keyspace;
import com.msd.gin.halyard.common.KeyspaceConnection;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.StatementIndex;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public final class HalyardKeys extends AbstractHalyardTool {
	private static final String TOOL_NAME = "keys";
	private static final String TARGET_PROPERTY = confProperty(TOOL_NAME, "target");
	private static final String DECIMATION_FACTOR_PROPERTY = confProperty(TOOL_NAME, "decimation-factor");
	
	private static final int DEFAULT_DECIMATION_FACTOR = 0;

	public HalyardKeys() {
		super(
			TOOL_NAME,
			"Halyard Keys is a MapReduce application that calculates key distribution statistics.",
			"Example: halyard keys -s my_dataset -t key_stats.csv"
		);
        addOption("s", "source-dataset", "dataset_table", SOURCE_NAME_PROPERTY, "Source HBase table with Halyard RDF store", true, true);
        addOption("t", "target-file", "target_url", TARGET_PROPERTY, "Target file to export the statistics to.", true, true);
		addOption("d", "decimation-factor", "decimation_factor", DECIMATION_FACTOR_PROPERTY, "Optionally overide random decimation factor (default is 0)", false, true);
        addOption("u", "restore-dir", "restore_folder", SNAPSHOT_PATH_PROPERTY, "If specified then -s is a snapshot name and this is the folder to restore to on HDFS", false, true);
	}

	static final class KeyColumnMapper extends TableMapper<ImmutableBytesWritable, LongWritable> {
		private final ImmutableBytesWritable outputKey = new ImmutableBytesWritable();
		private final LongWritable outputValue = new LongWritable();
		private final Random random = new Random(0);
		private int decimationFactor;
		private long counter = 0;

		@Override
		protected void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			decimationFactor = conf.getInt(DECIMATION_FACTOR_PROPERTY, DEFAULT_DECIMATION_FACTOR);
		}

		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context output) throws IOException, InterruptedException {
			if (decimationFactor == 0 || random.nextInt(decimationFactor) == 0) {
				report(output, key, value.size());
			}
			if (++counter % 10000 == 0) {
				output.setStatus(MessageFormat.format("{0}", counter));
			}
		}

		private void report(Context output, ImmutableBytesWritable key, int colCount) throws IOException, InterruptedException {
			byte[] keyBytes = new byte[5];
			byte prefix = key.get()[key.getOffset()];
			keyBytes[0] = prefix;
			Bytes.putInt(keyBytes, 1, colCount);
			outputKey.set(keyBytes);
			outputValue.set(1);
			output.write(outputKey, outputValue);
		}
	}

	static abstract class AbstractStatsReducer<OUTK,OUTV> extends Reducer<ImmutableBytesWritable, LongWritable, OUTK, OUTV> {
		@Override
		protected final void reduce(ImmutableBytesWritable key, Iterable<LongWritable> freqs, Context context) throws IOException, InterruptedException {
			long colCountFreq = 0;
			for (LongWritable freq : freqs) {
				colCountFreq += freq.get();
			}
			report(context, key, colCountFreq);
		}

		protected abstract void report(Context context, ImmutableBytesWritable key, long colCountFreq)  throws IOException, InterruptedException;
	}

	static final class StatsCombiner extends AbstractStatsReducer<ImmutableBytesWritable, LongWritable> {
		private final LongWritable outputValue = new LongWritable();

		@Override
		protected void report(Context context, ImmutableBytesWritable key, long colCountFreq) throws IOException, InterruptedException {
			outputValue.set(colCountFreq);
			context.write(key, outputValue);
		}
	}

	static final class StatsReducer extends AbstractStatsReducer<NullWritable, NullWritable> {
		private final Map<StatementIndex.Name,Map<Integer,Long>> statsByPrefix = new HashMap<>();

		@Override
        protected void setup(Context context) throws IOException, InterruptedException {
        }

        @Override
		protected void report(Context context, ImmutableBytesWritable key, long colCountFreq) throws IOException, InterruptedException {
        	byte[] keyBytes = key.get();
        	byte prefix = keyBytes[0];
        	int colCount = Bytes.toInt(keyBytes, 1);
        	StatementIndex.Name index = StatementIndex.Name.values()[prefix];
        	statsByPrefix.compute(index, (k,v) -> {
        		if (v == null) {
        			v = new HashMap<>();
        		}
        		v.put(colCount, colCountFreq);
        		return v;
        	});
		}

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String targetUrl = conf.get(TARGET_PROPERTY);
            FSDataOutputStream fsOut = FileSystem.get(URI.create(targetUrl), conf).create(new Path(targetUrl));
            fsOut.write("Index, Keys, Cols, Min cols/key, Max cols/key, Mean cols/key, Col freq\n".getBytes(StandardCharsets.US_ASCII));
            for (StatementIndex.Name index : StatementIndex.Name.values()) {
            	Map<Integer,Long> combinedColFreqs = statsByPrefix.get(index);
            	if (combinedColFreqs != null) {
		        	int min = Integer.MAX_VALUE;
		        	int max = Integer.MIN_VALUE;
		        	long totalCols = 0;
		        	long totalKeys = 0;
		        	List<Map.Entry<Integer,Long>> entries = new ArrayList<>(combinedColFreqs.size());
					for (Map.Entry<Integer,Long> entry : combinedColFreqs.entrySet()) {
						entries.add(entry);
						int colCount = entry.getKey();
						long freq = entry.getValue();
						min = Math.min(colCount, min);
						max = Math.max(colCount, max);
						totalCols += colCount*freq;
						totalKeys += freq;
					}
		        	StringBuilder buf = new StringBuilder();
		        	buf.append(index);
		        	buf.append(", ").append(Long.toString(totalKeys));
		        	buf.append(", ").append(Long.toString(totalCols));
		        	buf.append(", ").append(Integer.toString(min));
		        	buf.append(", ").append(Integer.toString(max));
		        	buf.append(", ").append(Long.toString(totalCols/totalKeys));
		        	buf.append(", ");
		        	entries.sort(new Comparator<Map.Entry<Integer,?>>() {
						@Override
						public int compare(Entry<Integer, ?> o1, Entry<Integer, ?> o2) {
							return o1.getKey() - o2.getKey();
						}
		        	});
					String freqSep = "";
					for (Map.Entry<Integer,Long> entry : entries) {
						buf.append(freqSep).append(entry.getKey()).append(":").append(entry.getValue());
						freqSep = "|";
					}
		        	buf.append("\n");
		        	fsOut.write(buf.toString().getBytes(StandardCharsets.US_ASCII));
            	}
            }
        	fsOut.close();
        }
	}

	@Override
	public int run(CommandLine cmd) throws Exception {
    	configureString(cmd, 's', null);
    	configureString(cmd, 't', null);
    	configureString(cmd, 'u', null);
        configureInt(cmd, 'd', DEFAULT_DECIMATION_FACTOR);
        String source = getConf().get(SOURCE_NAME_PROPERTY);
        String snapshotPath = getConf().get(SNAPSHOT_PATH_PROPERTY);
        if (snapshotPath != null) {
			FileSystem fs = CommonFSUtils.getRootDirFileSystem(getConf());
        	if (fs.exists(new Path(snapshotPath))) {
        		throw new IOException("Snapshot restore directory already exists");
        	}
        }
		HBaseConfiguration.addHbaseResources(getConf());
		Job job = Job.getInstance(getConf(), "HalyardKeys " + source);
		TableMapReduceUtil.addDependencyJars(job);
		TableMapReduceUtil.initCredentials(job);

        RDFFactory rdfFactory;
        Keyspace keyspace = HalyardTableUtils.getKeyspace(getConf(), source, snapshotPath);
        try {
        	try (KeyspaceConnection kc = keyspace.getConnection()) {
        		rdfFactory = RDFFactory.create(kc);
        	}
		} finally {
			keyspace.close();
		}
        Scan scan = StatementIndex.scanAll(rdfFactory);
        scan.setAllowPartialResults(false);
        scan.setBatch(-1);
        keyspace.initMapperJob(
            scan,
            KeyColumnMapper.class,
            ImmutableBytesWritable.class,
            LongWritable.class,
            job);
        job.setNumReduceTasks(1);
		job.setReducerClass(StatsReducer.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		try {
			if (job.waitForCompletion(true)) {
				LOG.info("Key stats completed.");
				return 0;
			} else {
				LOG.error("Key stats failed to complete.");
				return -1;
			}
		} finally {
			keyspace.destroy();
		}
	}
}
