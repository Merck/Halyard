package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.common.HalyardTableUtils;

import org.apache.hadoop.hbase.client.Connection;
import org.eclipse.rdf4j.sail.RDFStoreTest;
import org.eclipse.rdf4j.sail.Sail;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class HBaseStoreTest extends RDFStoreTest {
	private static int uid = 1;

	private Connection hconn;

	@Before
	public void initConn() throws Exception {
		hconn = HalyardTableUtils.getConnection(HBaseServerTestInstance.getInstanceConfig());
	}

	@Before
	public void setUp() throws Exception {
		super.setUp();
	}

	@After
	public void tearDown() throws Exception {
		super.tearDown();
	}

	@After
	public void closeConn() throws Exception {
		hconn.close();
	}

	@Override
	protected Sail createSail() {
		Sail sail = new HBaseSail(hconn, "storetesttable" + uid++, true, 0, true, 5, null, null);
		sail.init();
		return sail;
	}

	@Override
	@Test
	@Ignore // we return the canonical value
	public void testTimeZoneRoundTrip() throws Exception {
	}
}
