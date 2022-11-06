package com.msd.gin.halyard.sail.spin;

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.spin.SpinInferencing;
import com.msd.gin.halyard.spin.SailSplTest;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SPIN;
import org.eclipse.rdf4j.model.vocabulary.SPL;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.Sail;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;

/**
 * Runs the spl test cases.
 */
@RunWith(Parameterized.class)
public class HBaseSailSplTest extends SailSplTest {

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[] { true }, new Object[] { false });
	}

	private final String tableName;

	private boolean usePushStrategy;

	public HBaseSailSplTest(boolean usePushStrategy) {
		this.usePushStrategy = usePushStrategy;
		this.tableName = "splTestTable-" + (usePushStrategy ? "push" : "pull");
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
