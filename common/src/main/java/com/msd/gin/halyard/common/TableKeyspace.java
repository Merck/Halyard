package com.msd.gin.halyard.common;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;

final class TableKeyspace implements Keyspace {
	private final Configuration conf;
	private final boolean isOwner;
	private final TableName tableName;
	private Connection conn;

	public TableKeyspace(Connection conn, TableName tableName) {
		this.conn = conn;
		this.conf = null;
		this.isOwner = false;
		this.tableName = tableName;
	}

	public TableKeyspace(Configuration conf, TableName tableName) throws IOException {
		this.conf = conf;
		this.isOwner = true;
		this.tableName = tableName;
	}

	// Keyspace methods should be thread-safe
	private synchronized Table getTable() throws IOException {
		if (conn == null && conf != null) {
			conn = HalyardTableUtils.getConnection(conf);
		}
		return conn.getTable(tableName);
	}

	@Override
	public KeyspaceConnection getConnection() throws IOException {
		return new TableKeyspaceConnection(getTable());
	}

	@Override
	public void initMapperJob(Scan scan, Class<? extends TableMapper<?,?>> mapper, Class<?> outputKeyClass, Class<?> outputValueClass, Job job) throws IOException {
		TableMapReduceUtil.initTableMapperJob(
			tableName,
			scan,
			mapper,
			outputKeyClass,
			outputValueClass,
			job);
	}

	@Override
	public void initMapperJob(List<Scan> scans, Class<? extends TableMapper<?,?>> mapper, Class<?> outputKeyClass, Class<?> outputValueClass, Job job) throws IOException {
        byte[] tableNameBytes = tableName.toBytes();
        for (Scan scan : scans) {
            scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tableNameBytes);
        }
        TableMapReduceUtil.initTableMapperJob(
            scans,
			mapper,
			outputKeyClass,
			outputValueClass,
            job);
	}

	@Override
	public void close() throws IOException {
		if (isOwner && conn != null) {
			conn.close();
		}
	}

	@Override
	public void destroy() throws IOException {
	}

	static final class TableKeyspaceConnection implements KeyspaceConnection {
		private final Table table;
	
		public TableKeyspaceConnection(Table table) {
			this.table = table;
		}

		@Override
		public Result get(Get get) throws IOException {
			return table.get(get);
		}

		@Override
		public ResultScanner getScanner(Scan scan) throws IOException {
			return table.getScanner(scan);
		}

		@Override
		public void close() throws IOException {
			table.close();
		}
	}
}