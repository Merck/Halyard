package com.msd.gin.halyard.common;

import java.io.IOException;

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
	private final Connection conn;
	private final boolean isOwner;
	private final TableName tableName;

	public TableKeyspace(Connection conn, TableName tableName) {
		this.conn = conn;
		this.isOwner = false;
		this.tableName = tableName;
	}

	public TableKeyspace(Configuration conf, TableName tableName) throws IOException {
		this.conn = HalyardTableUtils.getConnection(conf);
		this.isOwner = true;
		this.tableName = tableName;
	}

	@Override
	public TableName getTableName() {
		return tableName;
	}

	@Override
	public KeyspaceConnection getConnection() throws IOException {
		return new TableKeyspaceConnection(conn.getTable(tableName));
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
	public void close() throws IOException {
		if (isOwner) {
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