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
import org.apache.commons.cli.MissingOptionException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.util.ToolRunner;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardPreSplitTest {

    @Test
    public void testPreSplit() throws Exception {
        File file = File.createTempFile("test_triples", ".nq");
        try (PrintStream ps = new PrintStream(new FileOutputStream(file))) {
            ps.println("<http://whatever/NTsubj1> <http://whatever/NTpred1> \"whatever NT value 1\" <http://whatever/ctx1> .");
            ps.println("<http://whatever/NTsubj2> <http://whatever/NTpred2> \"whatever NT value 2\" <http://whatever/ctx2> .");
        }

        assertEquals(0, ToolRunner.run(HBaseServerTestInstance.getInstanceConfig(), new HalyardPreSplit(), new String[]{"-d", "1", "-l",  "0", "-s", file.toURI().toURL().toString(), "-t", "preSplitTable"}));

        try (HTable t = HalyardTableUtils.getTable(HBaseServerTestInstance.getInstanceConfig(), "preSplitTable", false, 0)) {
            assertEquals(17, t.getRegionLocator().getStartKeys().length);
        }
    }

    @Test
    public void testPreSplitOfExisting() throws Exception {
        File file = File.createTempFile("test_triples", ".nq");
        try (PrintStream ps = new PrintStream(new FileOutputStream(file))) {
            ps.println("<http://whatever/NTsubj1> <http://whatever/NTpred1> \"whatever NT value 1\" <http://whatever/ctx1> .");
            ps.println("<http://whatever/NTsubj2> <http://whatever/NTpred2> \"whatever NT value 2\" <http://whatever/ctx2> .");
        }

        HalyardTableUtils.getTable(HBaseServerTestInstance.getInstanceConfig(), "preSplitTable2", true, -1).close();

        assertEquals(-1, ToolRunner.run(HBaseServerTestInstance.getInstanceConfig(), new HalyardPreSplit(), new String[]{"-d", "1", "-l",  "0", "-s", file.toURI().toURL().toString(), "-t", "preSplitTable2"}));
    }

    @Test
    public void testHelp() throws Exception {
        assertEquals(-1, new HalyardPreSplit().run(new String[]{"-h"}));
    }

    @Test(expected = MissingOptionException.class)
    public void testRunNoArgs() throws Exception {
        assertEquals(-1, new HalyardPreSplit().run(new String[]{}));
    }
}
