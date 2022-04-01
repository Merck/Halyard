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

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardPreSplitTest extends AbstractHalyardToolTest {

	@Override
	protected AbstractHalyardTool createTool() {
		return new HalyardPreSplit();
	}

    @Test
    public void testPreSplit() throws Exception {
        File file = File.createTempFile("test_triples", ".nq");
        file.deleteOnExit();
        try (PrintStream ps = new PrintStream(new FileOutputStream(file))) {
            ps.println("<http://whatever/NTsubj1> <http://whatever/NTpred1> \"whatever NT value 1\" <http://whatever/ctx1> .");
            ps.println("<http://whatever/NTsubj2> <http://whatever/NTpred2> \"whatever NT value 2\" <http://whatever/ctx2> .");
        }

        assertEquals(0, run(new String[]{"-d", "1", "-l",  "0", "-s", file.toURI().toURL().toString(), "-t", "preSplitTable"}));

		try (Connection conn = HalyardTableUtils.getConnection(HBaseServerTestInstance.getInstanceConfig())) {
			try (Table t = HalyardTableUtils.getTable(conn, "preSplitTable", false, 0)) {
				try (RegionLocator locator = conn.getRegionLocator(t.getName())) {
					assertEquals(17, locator.getStartKeys().length);
				}
			}
		}
    }

    @Test
    public void testPreSplitOfExisting() throws Exception {
        File file = File.createTempFile("test_triples", ".nq");
        file.deleteOnExit();
        try (PrintStream ps = new PrintStream(new FileOutputStream(file))) {
            ps.println("<http://whatever/NTsubj1> <http://whatever/NTpred1> \"whatever NT value 1\" <http://whatever/ctx1> .");
            ps.println("<http://whatever/NTsubj2> <http://whatever/NTpred2> \"whatever NT value 2\" <http://whatever/ctx2> .");
        }

        HalyardTableUtils.getTable(HBaseServerTestInstance.getInstanceConfig(), "preSplitTable2", true, -1).close();

        assertEquals(-1, run(new String[]{"-d", "1", "-l",  "0", "-s", file.toURI().toURL().toString(), "-t", "preSplitTable2"}));
    }
}
