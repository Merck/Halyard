package com.msd.gin.halyard.tools;

import com.google.common.collect.Sets;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RDFLoadStager implements Callable<Void> {
	private static final Logger LOGGER = LoggerFactory.getLogger(RDFLoadStager.class);

	private static final Set<String> MERGEABLE_COMPRESSION_FORMATS = Sets.newHashSet(".gz", ".bz2");
	private static final Set<String> MERGEABLE_RDF_FORMATS = Sets.newHashSet(".nt", ".nq", ".ttl", ".ttls", ".trig", ".trigs");

	private static final long MB = 1024*1024;
	private static final long GB = 1024*MB;
	private static final long MIN_BLOCK_SIZE = 64*MB;
	private static final int MAX_SMALL_PARTITIONS = 4;

	public static void main(String[] args) throws Exception {
		Path rdfData = Paths.get(args[0]);
		org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(args[1]);
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
		Configuration conf = new Configuration();
		if (args.length > 2) {
			org.apache.hadoop.fs.Path confFile = new org.apache.hadoop.fs.Path(args[2]);
			conf.addResource(confFile);
		}
		new RDFLoadStager(conf, rdfData, hdfsPath).call();
	}

	private final Configuration conf;
	private final Path rdfData;
	private final org.apache.hadoop.fs.Path hdfsPath;
	private final int bufferSize;
	private final short replication;
	private final long defaultBlockSize;

	RDFLoadStager(Configuration conf, Path rdfData, org.apache.hadoop.fs.Path hdfsPath) {
		this.conf = conf;
		this.rdfData = rdfData;
		this.hdfsPath = hdfsPath;
		this.bufferSize = Integer.getInteger(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY, conf.getInt(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY, CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT));
		this.replication = Integer.getInteger(HdfsClientConfigKeys.DFS_REPLICATION_KEY, conf.getInt(HdfsClientConfigKeys.DFS_REPLICATION_KEY, HdfsClientConfigKeys.DFS_REPLICATION_DEFAULT)).shortValue();
		this.defaultBlockSize = Long.getLong(HdfsClientConfigKeys.DFS_BLOCK_SIZE_KEY, conf.getLong(HdfsClientConfigKeys.DFS_BLOCK_SIZE_KEY, HdfsClientConfigKeys.DFS_BLOCK_SIZE_DEFAULT));
	}

	public Void call() throws Exception {
		// get all files
		Map<String, List<FileInfo>> filesByExt = new HashMap<>();
		Files.walkFileTree(rdfData, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				FileVisitResult r = super.visitFile(file, attrs);
				FileInfo fi = new FileInfo(file);
				filesByExt.compute(fi.getExtension(), (k,v) -> {
					if (v == null) {
						v = new ArrayList<>();
					}
					v.add(fi);
					return v;
				});
				return r;
			}
		});

		ExecutorService executor = Executors.newCachedThreadPool();
		ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(executor);
		int numTasks = 0;
		for (Map.Entry<String, List<FileInfo>> entry : filesByExt.entrySet()) {
			String ext = entry.getKey();
			List<FileInfo> files = entry.getValue();
			if (canMerge(ext)) {
				int numPartitions;
				long totalSize = sizeOf(files);
				if (totalSize <= MIN_BLOCK_SIZE) {
					numPartitions = 1;
				} else if (totalSize <= GB) {
					numPartitions = Math.min((int) (totalSize/MIN_BLOCK_SIZE), MAX_SMALL_PARTITIONS);
				} else {
					numPartitions = (int) (totalSize/defaultBlockSize) + 1;
				}
				LOGGER.info("Merging {} bytes of {} data...", totalSize, ext);

				// don't create more partitions than files
				numPartitions = Math.min(numPartitions, files.size());
				List<Partition> partitions = new ArrayList<>(numPartitions);
				if (numPartitions > 1) {
					for (int i=0; i<numPartitions; i++) {
						partitions.add(new Partition(hdfsPath, i, ext));
					}
					// "Longest-processing-time-first scheduling"
					// sort large to small
					PriorityQueue<Partition> totalSizeQueue = new PriorityQueue<>(new Comparator<Partition>() {
						@Override
						public int compare(Partition o1, Partition o2) {
							return Long.compare(o1.size, o2.size);
						}
					});
					for (Partition partition : partitions) {
						totalSizeQueue.add(partition);
					}
					files.sort(new LargeToSmallFileSizeComparator());
					for (FileInfo fi : files) {
						Partition p = totalSizeQueue.remove();
						p.add(fi);
						totalSizeQueue.add(p);
					}
				} else {
					Partition partition = new Partition(hdfsPath, 0, ext);
					files.stream().forEach(fi -> partition.add(fi));
					partitions.add(partition);
				}
				for (Partition p : partitions) {
					LOGGER.info(p.toString());
					completionService.submit(new FileMergeTask(p));
					numTasks++;
				}
			} else {
				for (FileInfo fi : files) {
					completionService.submit(new FileCopyTask(fi));
					numTasks++;
				}
			}
		}

		for (int i=0; i<numTasks; i++) {
			try {
				completionService.take().get();
			} catch (InterruptedException e) {
				executor.shutdownNow();
				throw e;
			} catch (ExecutionException e) {
				executor.shutdownNow();
				Throwable thr = e.getCause();
				if (thr instanceof Exception) {
					throw (Exception) thr;
				} else {
					throw e;
				}
			}
		}

		return null;
	}

	private boolean canMerge(String ext) {
		int dotPos = ext.lastIndexOf('.');
		String compressionExt = (dotPos != -1) ? ext.substring(dotPos) : null;
		String rdfExt = (dotPos != -1) ? ext.substring(0, dotPos) : ext;
		return (compressionExt == null || MERGEABLE_COMPRESSION_FORMATS.contains(compressionExt)) && MERGEABLE_RDF_FORMATS.contains(rdfExt);
	}

	private long blockSize(long fileSize) {
		if (fileSize < defaultBlockSize/4) {
			return Math.max(defaultBlockSize/4, MIN_BLOCK_SIZE);
		} else if (fileSize < defaultBlockSize/2) {
			return Math.max(defaultBlockSize/2, MIN_BLOCK_SIZE);
		} else {
			return defaultBlockSize;
		}
	}

	private static long sizeOf(List<FileInfo> files) {
		return files.stream().mapToLong(fi -> fi.size).sum();
	}

	final class FileMergeTask implements Callable<Void> {
		final Partition p;

		FileMergeTask(Partition p) {
			this.p = p;
		}

		@Override
		public Void call() throws Exception {
			long blockSize = blockSize(p.size);
			FileSystem hdfs = hdfsPath.getFileSystem(conf);
			try (FSDataOutputStream out = hdfs.create(p.file, true, bufferSize, replication, blockSize)) {
				for (FileInfo fi : p.parts) {
					try (InputStream in = Files.newInputStream(fi.file)) {
						IOUtils.copy(in, out, bufferSize);
					}
				}
			}
			return null;
		}
	}

	final class FileCopyTask implements Callable<Void> {
		final FileInfo fileInfo;

		FileCopyTask(FileInfo fileInfo) {
			this.fileInfo = fileInfo;
		}

		@Override
		public Void call() throws Exception {
			Path file = fileInfo.file;
			long blockSize = blockSize(fileInfo.size);
			FileSystem hdfs = hdfsPath.getFileSystem(conf);
			try (FSDataOutputStream out = hdfs.create(new org.apache.hadoop.fs.Path(hdfsPath, rdfData.relativize(file).toString()), true, bufferSize, replication, blockSize)) {
				try (InputStream in = Files.newInputStream(file)) {
					IOUtils.copy(in, out, bufferSize);
				}
			}
			return null;
		}
	}


	static final class FileInfo {
		final Path file;
		final long size;

		FileInfo(Path f) throws IOException {
			file = f;
			size = Files.size(f);
		}

		String getExtension() {
			String name = file.getFileName().toString();
			int dotPos = name.indexOf('.');
			return (dotPos != -1) ? name.substring(dotPos) : null;
		}

		@Override
		public String toString() {
			return file.toString();
		}
	}

	static final class Partition {
		final org.apache.hadoop.fs.Path file;
		final List<FileInfo> parts = new ArrayList<>();
		long size = 0L;

		Partition(org.apache.hadoop.fs.Path outputDir, int n, String ext) {
			file = new org.apache.hadoop.fs.Path(outputDir, "partition"+(n+1)+ext);
		}

		void add(FileInfo fi) {
			parts.add(fi);
			size += fi.size;
		}

		@Override
		public String toString() {
			return String.format("Partition %s: size = %d, files = %s", file, size, parts);
		}
	}

	static final class LargeToSmallFileSizeComparator implements Comparator<FileInfo> {
		@Override
		public int compare(FileInfo o1, FileInfo o2) {
			return Long.compare(o2.size, o1.size);
		}
	}
}
