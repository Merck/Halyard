/*
 * Copyright 2016 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
 * Inc., Kenilworth, NJ, USA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.msd.gin.halyard.sail;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.config.SailConfigException;
import org.eclipse.rdf4j.sail.config.SailImplConfig;
import org.junit.Test;
import static org.junit.Assert.*;

import java.net.URL;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HBaseSailFactoryTest {

    @Test
    public void testGetSailType() {
        assertEquals("openrdf:HBaseStore", new HBaseSailFactory().getSailType());
    }

    @Test
    public void testGetConfig() {
        assertTrue(new HBaseSailFactory().getConfig() instanceof HBaseSailConfig);
    }

    @Test
	public void testGetSail_table() throws Exception {
        HBaseSailConfig hbsc = new HBaseSailConfig();
        hbsc.setCreate(false);
        hbsc.setPush(false);
        hbsc.setSplitBits(3);
        hbsc.setEvaluationTimeout(480);
        hbsc.setTableName("testtable");
        hbsc.setElasticIndexURL(new URL("http://whatever/index"));
        Sail sail = new HBaseSailFactory().getSail(hbsc);
        assertTrue(sail instanceof HBaseSail);
        HBaseSail hbs = (HBaseSail)sail;
        assertFalse(hbs.create);
        assertFalse(hbs.pushStrategy);
        assertEquals(3, hbs.splitBits);
		assertEquals("testtable", hbs.tableName.getNameAsString());
        assertEquals(480, hbs.evaluationTimeout);
        assertEquals("http", hbs.esSettings.protocol);
        assertEquals("whatever", hbs.esSettings.host);
        assertEquals("index", hbs.esSettings.indexName);
    }

	@Test
	public void testGetSail_snapshot() throws Exception {
		HBaseSailConfig hbsc = new HBaseSailConfig();
		hbsc.setCreate(false);
		hbsc.setPush(false);
		hbsc.setEvaluationTimeout(480);
		hbsc.setSnapshotName("snapshot");
		hbsc.setSnapshotRestorePath("/path");
		hbsc.setElasticIndexURL(new URL("http://whatever/index"));
		Sail sail = new HBaseSailFactory().getSail(hbsc);
		assertTrue(sail instanceof HBaseSail);
		HBaseSail hbs = (HBaseSail) sail;
		assertFalse(hbs.create);
		assertFalse(hbs.pushStrategy);
		assertEquals(-1, hbs.splitBits);
		assertEquals("snapshot", hbs.snapshotName);
		assertEquals("/path", hbs.snapshotRestorePath.toString());
		assertEquals(480, hbs.evaluationTimeout);
        assertEquals("http", hbs.esSettings.protocol);
        assertEquals("whatever", hbs.esSettings.host);
        assertEquals("index", hbs.esSettings.indexName);
	}

    @Test(expected = SailConfigException.class)
    public void testGetSailFail1() throws Exception {
        new HBaseSailFactory().getSail(new SailImplConfig() {
            @Override
            public String getType() {
                return "WrongType";
            }

            @Override
            public long getIterationCacheSyncThreshold() {
                return 0;
            }

            @Override
            public void validate() throws SailConfigException {
            }

            @Override
            public Resource export(Model graph) {
                return null;
            }

            @Override
            public void parse(Model graph, Resource implNode) throws SailConfigException {
            }
        });
    }

    @Test(expected = SailConfigException.class)
    public void testGetSailFail2() throws Exception {
        new HBaseSailFactory().getSail(new SailImplConfig() {
            @Override
            public String getType() {
                return HBaseSailFactory.SAIL_TYPE;
            }

            @Override
            public long getIterationCacheSyncThreshold() {
                return 0;
            }

            @Override
            public void validate() throws SailConfigException {
            }

            @Override
            public Resource export(Model graph) {
                return null;
            }

            @Override
            public void parse(Model graph, Resource implNode) throws SailConfigException {
            }
        });
    }

	@Test(expected = SailConfigException.class)
	public void testGetSailFail3() throws Exception {
		HBaseSailConfig hbsc = new HBaseSailConfig();
		hbsc.setTableName("table");
		hbsc.setSnapshotName("snapshot");
		new HBaseSailFactory().getSail(hbsc);
	}
}
