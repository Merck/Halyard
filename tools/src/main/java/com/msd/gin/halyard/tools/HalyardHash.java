package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.common.Hashes;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.ValueIO;
import com.msd.gin.halyard.tools.HalyardBulkLoad.RioFileInputFormat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.helpers.NTriplesUtil;

public final class HalyardHash extends AbstractHalyardTool {
	private static final String SOURCE_PROPERTY = "halyard.hash.source";
	private static final String DECIMATION_FACTOR_PROPERTY = "halyard.hash.decimation";
	
	private static final int DEFAULT_DECIMATION_FACTOR = 0;

	enum Counters {
		ID_COLLISIONS
	}

	public HalyardHash() {
		super(
			"hash",
			"Halyard Hash is a MapReduce application that checks the uniqueness of ID hashes.",
			"Example: halyard hash -s hdfs://my_RDF_files"
		);
        addOption("s", "source", "source_paths", "Source path(s) with RDF files, more paths can be delimited by comma, the paths are recursively searched for the supported files", true, true);
		addOption("d", "decimation-factor", "decimation_factor", "Optionally overide random decimation factor (default is 0)", false, true);
	}

	static final class HashMapper extends Mapper<LongWritable, Statement, ImmutableBytesWritable, ImmutableBytesWritable> {
		private final ImmutableBytesWritable outputKey = new ImmutableBytesWritable();
		private final ImmutableBytesWritable outputValue = new ImmutableBytesWritable();
		private final Random random = new Random(0);
		private ByteBuffer kbb;
		private ByteBuffer vbb;
		private int decimationFactor;
		private RDFFactory rdfFactory;
		private long counter = 0;

		@Override
		protected void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			decimationFactor = conf.getInt(DECIMATION_FACTOR_PROPERTY, DEFAULT_DECIMATION_FACTOR);
			rdfFactory = RDFFactory.create(conf);
			kbb = ByteBuffer.allocate(rdfFactory.getValueIO().getIdSize());
			vbb = ByteBuffer.allocate(ValueIO.DEFAULT_BUFFER_SIZE);
		}

		@Override
		protected void map(LongWritable key, Statement stmt, Context output) throws IOException, InterruptedException {
			if (decimationFactor == 0 || random.nextInt(decimationFactor) == 0) {
				report(output, stmt.getSubject());
				report(output, stmt.getPredicate());
				report(output, stmt.getObject());
				if (stmt.getContext() != null) {
					report(output, stmt.getContext());
				}
			}
			if (++counter % 10000 == 0) {
				output.setStatus(MessageFormat.format("{0}", counter));
			}
		}

		private void report(Context output, Value v) throws IOException, InterruptedException {
			kbb.clear();
			kbb = rdfFactory.getValueIO().id(v).writeTo(kbb);
			kbb.flip();

			vbb.clear();
			vbb = rdfFactory.getValueIO().STREAM_WRITER.writeTo(v, vbb);
			vbb.flip();

			outputKey.set(kbb.array(), kbb.arrayOffset(), kbb.limit());
			outputValue.set(vbb.array(), vbb.arrayOffset(), vbb.limit());
			output.write(outputKey, outputValue);
		}
	}

	static abstract class AbstractHashReducer<OUTK,OUTV> extends Reducer<ImmutableBytesWritable, ImmutableBytesWritable, OUTK, OUTV> {
		protected RDFFactory rdfFactory;

		@Override
		protected void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			rdfFactory = RDFFactory.create(conf);
		}

		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {
			Set<Value> rdfTerms = new HashSet<>();
			for (ImmutableBytesWritable value : values) {
				Value v = rdfFactory.getValueIO().STREAM_READER.readValue(ByteBuffer.wrap(value.get(), value.getOffset(), value.getLength()));
				rdfTerms.add(v);
			}

			if (rdfTerms.size() > 1) {
				context.getCounter(Counters.ID_COLLISIONS).increment(1);
				String msg = MessageFormat.format("Hash collision! {0} all have the ID {1}", rdfTerms, Hashes.encode(key.copyBytes()));
				context.setStatus(msg);
				LOG.error(msg);
			} else {
				report(context, key, rdfTerms.iterator().next());
			}
		}

		protected abstract void report(Context context, ImmutableBytesWritable key, Value rdfTerm) throws IOException, InterruptedException;
	}

	static final class HashCombiner extends AbstractHashReducer<ImmutableBytesWritable, ImmutableBytesWritable> {
		private final ImmutableBytesWritable outputValue = new ImmutableBytesWritable();

		@Override
		protected void report(Context context, ImmutableBytesWritable key, Value rdfTerm) throws IOException, InterruptedException {
			ByteBuffer vbb = ByteBuffer.allocate(ValueIO.DEFAULT_BUFFER_SIZE);
			vbb = rdfFactory.getValueIO().STREAM_WRITER.writeTo(rdfTerm, vbb);
			vbb.flip();
			outputValue.set(vbb.array(), vbb.arrayOffset(), vbb.limit());
			context.write(key, outputValue);
		}
	}

	static final class HashReducer extends AbstractHashReducer<NullWritable, NullWritable> {
		@Override
		protected void report(Context context, ImmutableBytesWritable key, Value rdfTerm) {
			// do nothing
		}
	}

	@Override
	public int run(CommandLine cmd) throws Exception {
		String source = cmd.getOptionValue('s');
		getConf().set(SOURCE_PROPERTY, source);
		getConf().setInt(DECIMATION_FACTOR_PROPERTY, Integer.parseInt(cmd.getOptionValue('d', String.valueOf(DEFAULT_DECIMATION_FACTOR))));
        TableMapReduceUtil.addDependencyJarsForClasses(getConf(),
                NTriplesUtil.class,
                Rio.class,
                AbstractRDFHandler.class,
                RDFFormat.class,
                RDFParser.class);
		HBaseConfiguration.addHbaseResources(getConf());
		Job job = Job.getInstance(getConf(), "HalyardHash " + source);
		TableMapReduceUtil.addDependencyJars(job);
		TableMapReduceUtil.initCredentials(job);
		job.setJarByClass(HalyardHash.class);
		job.setMapperClass(HashMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(ImmutableBytesWritable.class);
		job.setInputFormatClass(RioFileInputFormat.class);
		FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.setInputPaths(job, source);
		job.setCombinerClass(HashCombiner.class);
		job.setReducerClass(HashReducer.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		if (job.waitForCompletion(true)) {
			long idCollisions = job.getCounters().findCounter(Counters.ID_COLLISIONS).getValue();
			LOG.info("Hashing completed ({} collisions).", idCollisions);
			return (int) idCollisions;
		} else {
			LOG.error("Hashing failed to complete.");
			return -1;
		}
	}
}
