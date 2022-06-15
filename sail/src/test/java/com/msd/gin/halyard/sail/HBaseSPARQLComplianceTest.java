package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.repository.HBaseRepository;
import com.msd.gin.halyard.repository.HBaseRepositoryFactory;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.repository.config.RepositoryImplConfig;
import org.eclipse.rdf4j.testsuite.sparql.RepositorySPARQLComplianceTestSuite;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class HBaseSPARQLComplianceTest extends RepositorySPARQLComplianceTestSuite {

	@BeforeClass
	public static void setUpFactory() throws Exception {
		Configuration conf = HBaseServerTestInstance.getInstanceConfig();
		setRepositoryFactory(new HBaseRepositoryFactory() {
			@Override
			public Repository getRepository(RepositoryImplConfig config) throws RepositoryConfigException {
				HBaseSail sail = new HBaseSail(conf, "complianceTestSuite", true, 0, true, 10, null, null);
				return new HBaseRepository(sail);
			}
		});
	}

	@AfterClass
	public static void tearDownFactory() throws Exception {
		setRepositoryFactory(null);
	}
}
