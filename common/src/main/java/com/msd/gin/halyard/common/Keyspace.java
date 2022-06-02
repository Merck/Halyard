package com.msd.gin.halyard.common;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;

public interface Keyspace extends Closeable {
	TableName getTableName();
	KeyspaceConnection getConnection() throws IOException;
	void initMapperJob(Scan scan, Class<? extends TableMapper<?,?>> mapper, Class<?> outputKeyClass, Class<?> outputValueClass, Job job) throws IOException;
	/**
	 * Removes any persisted resources.
	 * @throws IOException
	 */
	void destroy() throws IOException;
}
