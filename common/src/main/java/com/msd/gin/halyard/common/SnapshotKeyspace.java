package com.msd.gin.halyard.common;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableSnapshotScanner;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.mapreduce.Job;

final class SnapshotKeyspace implements Keyspace {
	private final Configuration conf;
	private final String snapshotName;
	private final Path rootDir;
	private final Path restoreDir;
	private final boolean isOwner;

	public SnapshotKeyspace(Configuration conf, String snapshotName, Path restoreDir) throws IOException {
		this.conf = conf;
		this.snapshotName = snapshotName;
		this.rootDir = CommonFSUtils.getRootDir(conf);
		this.restoreDir = restoreDir;
		FileSystem fs = rootDir.getFileSystem(conf);
		this.isOwner = !fs.exists(restoreDir);
		if (this.isOwner) {
			RestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshotName);
		}
	}

	@Override
	public KeyspaceConnection getConnection() {
		return new SnapshotKeyspaceConnection();
	}

	@Override
	public void initMapperJob(Scan scan, Class<? extends TableMapper<?,?>> mapper, Class<?> outputKeyClass, Class<?> outputValueClass, Job job) throws IOException {
		TableMapReduceUtil.initTableSnapshotMapperJob(
			snapshotName,
			scan,
			mapper,
			outputKeyClass,
			outputValueClass,
			job,
			true,
			restoreDir);
	}

	@Override
	public void initMapperJob(List<Scan> scans, Class<? extends TableMapper<?,?>> mapper, Class<?> outputKeyClass, Class<?> outputValueClass, Job job) throws IOException {
        TableMapReduceUtil.initMultiTableSnapshotMapperJob(
            Collections.singletonMap(snapshotName, (Collection<Scan>) scans),
			mapper,
			outputKeyClass,
			outputValueClass,
			job,
			true,
			restoreDir);
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public void destroy() throws IOException {
		if (isOwner) {
			if (!restoreDir.getFileSystem(conf).delete(restoreDir, true)) {
				throw new IOException(String.format("Failed to delete restore directory for snapshot: %s", restoreDir));
			}
		}
	}


	final class SnapshotKeyspaceConnection implements KeyspaceConnection {
		@Override
		public Result get(Get get) throws IOException {
			try (ResultScanner scanner = getScanner(new Scan(get))) {
				return scanner.next();
			}
		}

		@Override
		public ResultScanner getScanner(Scan scan) throws IOException {
			return new TableSnapshotScanner(conf, rootDir, restoreDir, snapshotName, scan, true);
		}

		@Override
		public void close() throws IOException {
		}
	}
}
