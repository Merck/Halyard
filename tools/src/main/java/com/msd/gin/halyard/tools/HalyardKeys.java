package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.common.HalyardTableUtils;
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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public final class HalyardKeys extends AbstractHalyardTool {
	private static final String SOURCE_PROPERTY = "halyard.keys.source";
	private static final String TARGET_PROPERTY = "halyard.keys.target";
	private static final String DECIMATION_FACTOR_PROPERTY = "halyard.keys.decimation";
	
	private static final int DEFAULT_DECIMATION_FACTOR = 0;

	public HalyardKeys() {
		super(
			"keys",
			"Halyard Keys is a MapReduce application that calculates key distribution statistics.",
			"Example: halyard keys -s my_dataset -t key_stats.csv"
		);
        addOption("s", "source-dataset", "dataset_table", "Source HBase table with Halyard RDF store", true, true);
        addOption("t", "target-file", "target_url", "Target file to export the statistics to.", true, true);
		addOption("d", "decimation-factor", "decimation_factor", "Optionally overide random decimation factor (default is 0)", false, true);
	}

	static final class KeyColumnMapper extends TableMapper<ByteWritable, MapWritable> {
		private final ByteWritable outputKey = new ByteWritable();
		private final MapWritable outputValue = new MapWritable();
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
				Cell[] cells = value.rawCells();
				report(output, key, cells.length);
			}
			if (++counter % 10000 == 0) {
				output.setStatus(MessageFormat.format("{0}", counter));
			}
		}

		private void report(Context output, ImmutableBytesWritable key, int colCount) throws IOException, InterruptedException {
			byte prefix = key.get()[key.getOffset()];
			outputKey.set(prefix);
			outputValue.put(new IntWritable(colCount), new LongWritable(1));
			output.write(outputKey, outputValue);
		}
	}

	static abstract class AbstractStatsReducer<OUTK,OUTV> extends Reducer<ByteWritable, MapWritable, OUTK, OUTV> {
		@Override
		protected final void reduce(ByteWritable key, Iterable<MapWritable> colFreqs, Context context) throws IOException, InterruptedException {
			Map<Integer,Long> combinedColFreqs = new HashMap<>();
			for (MapWritable colFreq : colFreqs) {
				for (Map.Entry<Writable, Writable> entry : colFreq.entrySet()) {
					int colCount = ((IntWritable)entry.getKey()).get();
					long freq = ((LongWritable)entry.getValue()).get();
					combinedColFreqs.merge(colCount, freq, Long::sum);
				}
			}
			report(context, key, combinedColFreqs);
		}

		protected abstract void report(Context context, ByteWritable key, Map<Integer,Long> combinedColFreqs)  throws IOException, InterruptedException;
	}

	static final class StatsCombiner extends AbstractStatsReducer<ByteWritable, MapWritable> {
		private final MapWritable outputValue = new MapWritable();

		@Override
		protected void report(Context context, ByteWritable key, Map<Integer,Long> combinedColFreqs) throws IOException, InterruptedException {
			for (Map.Entry<Integer,Long> entry : combinedColFreqs.entrySet()) {
				outputValue.put(new IntWritable(entry.getKey()), new LongWritable(entry.getValue()));
			}
			context.write(key, outputValue);
		}
	}

	static final class StatsReducer extends AbstractStatsReducer<NullWritable, NullWritable> {
		private final Map<StatementIndex.Name,Map<Integer,Long>> statsByPrefix = new HashMap<>();

		@Override
        protected void setup(Context context) throws IOException, InterruptedException {
        }

        @Override
		protected void report(Context context, ByteWritable key, Map<Integer,Long> combinedColFreqs) throws IOException, InterruptedException {
        	StatementIndex.Name index = StatementIndex.Name.values()[key.get()];
        	statsByPrefix.put(index, combinedColFreqs);
		}

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String targetUrl = conf.get(TARGET_PROPERTY);
            FSDataOutputStream fsOut = FileSystem.get(URI.create(targetUrl), conf).create(new Path(targetUrl));
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
		        	buf.append(", ").append(Integer.toString(min));
		        	buf.append(", ").append(Integer.toString(max));
		        	buf.append(", ").append(Long.toString(totalCols/totalKeys));
		        	entries.sort(new Comparator<Map.Entry<Integer,?>>() {
						@Override
						public int compare(Entry<Integer, ?> o1, Entry<Integer, ?> o2) {
							return o1.getKey() - o2.getKey();
						}
		        	});
					for (Map.Entry<Integer,Long> entry : entries) {
			        	buf.append(", ").append(entry.getKey()).append(":").append(entry.getValue());
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
		String source = cmd.getOptionValue('s');
		getConf().set(SOURCE_PROPERTY, source);
		getConf().set(TARGET_PROPERTY, cmd.getOptionValue('t'));
		getConf().setInt(DECIMATION_FACTOR_PROPERTY, Integer.parseInt(cmd.getOptionValue('d', String.valueOf(DEFAULT_DECIMATION_FACTOR))));
		HBaseConfiguration.addHbaseResources(getConf());
		Job job = Job.getInstance(getConf(), "HalyardKeys " + source);
		TableMapReduceUtil.addDependencyJars(job);
		TableMapReduceUtil.initCredentials(job);

		RDFFactory rdfFactory;
        try (Table table = HalyardTableUtils.getTable(getConf(), source, false, 0)) {
            rdfFactory = RDFFactory.create(table);
        }
        Scan scan = StatementIndex.scanAll(rdfFactory);
        TableMapReduceUtil.initTableMapperJob(source,
                scan,
                KeyColumnMapper.class,
                ByteWritable.class,
                MapWritable.class,
                job);
        job.setNumReduceTasks(1);
		job.setReducerClass(StatsReducer.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		if (job.waitForCompletion(true)) {
			LOG.info("Key stats completed.");
			return 0;
		} else {
			LOG.error("Key stats failed to complete.");
			return -1;
		}
	}
}
