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
package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.sail.HBaseSail;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardBulkDeleteTest extends AbstractHalyardToolTest {
    private static final String TABLE = "bulkdeletetesttable";

	@Override
	protected AbstractHalyardTool createTool() {
		return new HalyardBulkDelete();
	}

	private static void createData() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Configuration conf = HBaseServerTestInstance.getInstanceConfig();
        HBaseSail sail = new HBaseSail(conf, TABLE, true, -1, true, 0, null, null);
        sail.initialize();
        IRI pred = vf.createIRI("http://whatever/pred");
		try (SailConnection conn = sail.getConnection()) {
			for (int i = 0; i < 5; i++) {
				for (int j = 0; j < 5; j++) {
					Resource subj = vf.createIRI("http://whatever/subj" + i);
					Resource obj = vf.createIRI("http://whatever/obj" + j);
					Resource ctx = (i == 0) ? null : vf.createIRI("http://whatever/ctx" + i);
					conn.addStatement(subj, pred, obj, ctx);
				}
			}
        }
        sail.shutDown();
	}

	@Test
    public void testBulkDelete() throws Exception {
		createData();
        File htableDir = getTempHTableDir("test_htable");
        assertEquals(0, run(new String[]{ "-t", TABLE, "-o", "<http://whatever/obj0>", "-g", HalyardBulkDelete.DEFAULT_GRAPH_KEYWORD, "-g", "<http://whatever/ctx1>", "-w", htableDir.toURI().toURL().toString()}));
        assertCount(23);

        htableDir = getTempHTableDir("test_htable");
        assertEquals(0, run(new String[]{ "-t", TABLE, "-s", "<http://whatever/subj2>", "-w", htableDir.toURI().toURL().toString()}));
        assertCount(18);

        htableDir = getTempHTableDir("test_htable");
        assertEquals(0, run(new String[]{ "-t", TABLE, "-p", "<http://whatever/pred>", "-w", htableDir.toURI().toURL().toString()}));
        assertCount(0);
    }

	@Test
    public void testBulkDelete_snapshot() throws Exception {
		createData();
		String snapshot = TABLE + "Snapshot";
        Configuration conf = HBaseServerTestInstance.getInstanceConfig();
    	try (Connection conn = HalyardTableUtils.getConnection(conf)) {
        	try (Admin admin = conn.getAdmin()) {
        		admin.snapshot(snapshot, TableName.valueOf(TABLE));
        	}
    	}

    	File htableDir = getTempHTableDir("test_htable_snapshot");
        File restoredSnapshot = getTempSnapshotDir("restored_snapshot");
        assertEquals(0, run(new String[]{ "-n", snapshot, "-t", TABLE, "-o", "<http://whatever/obj0>", "-g", HalyardBulkDelete.DEFAULT_GRAPH_KEYWORD, "-g", "<http://whatever/ctx1>", "-w", htableDir.toURI().toURL().toString(), "-u", restoredSnapshot.toURI().toURL().toString()}));
        assertCount(23);

        htableDir = getTempHTableDir("test_htable_snapshot");
        restoredSnapshot = getTempSnapshotDir("restored_snapshot");
        assertEquals(0, run(new String[]{ "-n", snapshot, "-t", TABLE, "-s", "<http://whatever/subj2>", "-w", htableDir.toURI().toURL().toString(), "-u", restoredSnapshot.toURI().toURL().toString()}));
        assertCount(18);

        htableDir = getTempHTableDir("test_htable_snapshot");
        restoredSnapshot = getTempSnapshotDir("restored_snapshot");
        assertEquals(0, run(new String[]{ "-n", snapshot, "-t", TABLE, "-p", "<http://whatever/pred>", "-w", htableDir.toURI().toURL().toString(), "-u", restoredSnapshot.toURI().toURL().toString()}));
        assertCount(0);
    }

    private static void assertCount(int expected) throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), TABLE, false, 0, true, 0, null, null);
        sail.initialize();
        try {
			try (SailConnection conn = sail.getConnection()) {
				int count = 0;
				try (CloseableIteration<? extends Statement, SailException> iter = conn.getStatements(null, null, null, true)) {
					while (iter.hasNext()) {
						iter.next();
						count++;
					}
				}
				Assert.assertEquals(expected, count);
			}
        } finally {
            sail.shutDown();
        }
    }
}
