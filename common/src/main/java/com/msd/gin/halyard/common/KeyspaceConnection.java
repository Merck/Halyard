package com.msd.gin.halyard.common;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public interface KeyspaceConnection extends Closeable {
	Result get(Get get) throws IOException;
	ResultScanner getScanner(Scan scan) throws IOException;
}
