package com.msd.gin.halyard.sail.spin;

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.spin.SailSpifTest;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.eclipse.rdf4j.sail.Sail;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Runs the spif test cases.
 */
@RunWith(Parameterized.class)
public class HBaseSailSpifTest extends SailSpifTest {

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[] { true }, new Object[] { false });
	}

	private final String tableName;

	private boolean usePushStrategy;

	public HBaseSailSpifTest(boolean usePushStrategy) {
		this.usePushStrategy = usePushStrategy;
		this.tableName = "spifTestTable-" + (usePushStrategy ? "push" : "pull");
	}

	@Override
	protected Sail createSail() throws Exception {
		Configuration conf = HBaseServerTestInstance.getInstanceConfig();
		HBaseSail sail = new HBaseSail(conf, tableName, true, 0, usePushStrategy, 10, null, null);
		return sail;
	}

	@Override
	protected void postCleanup() throws Exception {
		try (Connection hconn = HalyardTableUtils.getConnection(HBaseServerTestInstance.getInstanceConfig())) {
			HalyardTableUtils.deleteTable(hconn, TableName.valueOf(tableName));
		}
	}
}
