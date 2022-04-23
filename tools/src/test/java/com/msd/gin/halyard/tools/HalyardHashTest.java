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

import com.msd.gin.halyard.common.Config;
import com.msd.gin.halyard.common.HBaseServerTestInstance;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.*;

public class HalyardHashTest extends AbstractHalyardToolTest {
	@Override
	protected AbstractHalyardTool createTool() {
		return new HalyardHash();
	}

    @Test
    public void testHash() throws Exception {
        File file = File.createTempFile("test_triples", ".nq");
        file.deleteOnExit();
        try (PrintStream ps = new PrintStream(new FileOutputStream(file))) {
            for (int i=1; i<=100; i++) {
                ps.println("<http://whatever/NTsubj"+i+"> <http://whatever/NTpred"+i+"> \"whatever NT value "+i+"\" <http://whatever/ctx"+i+"> .");
            }
        }

        Configuration conf = HBaseServerTestInstance.getInstanceConfig();
        conf.set(Config.ID_HASH, "Murmur3-128");
        conf.setInt(Config.ID_SIZE, 8);
        assertEquals(0, run(conf, new String[]{"-s", file.toURI().toURL().toString()}));
    }

    @Test
    public void testHashFail() throws Exception {
        File file = File.createTempFile("test_triples", ".nq");
        file.deleteOnExit();
        try (PrintStream ps = new PrintStream(new FileOutputStream(file))) {
            for (int i=1; i<=100; i++) {
                ps.println("<http://whatever/NTsubj"+i+"> <http://whatever/NTpred"+i+"> \"whatever NT value "+i+"\" <http://whatever/ctx"+i+"> .");
            }
        }

        Configuration conf = HBaseServerTestInstance.getInstanceConfig();
        conf.set(Config.ID_HASH, "Murmur3-128");
        conf.setInt(Config.ID_SIZE, 1);
        conf.setInt(Config.ID_TYPE_INDEX, 0);
        conf.setInt(Config.KEY_SIZE_SUBJECT, 1);
        conf.setInt(Config.END_KEY_SIZE_SUBJECT, 1);
        conf.setInt(Config.KEY_SIZE_PREDICATE, 1);
        conf.setInt(Config.END_KEY_SIZE_PREDICATE, 1);
        conf.setInt(Config.KEY_SIZE_OBJECT, 1);
        conf.setInt(Config.END_KEY_SIZE_OBJECT, 1);
        conf.setInt(Config.KEY_SIZE_CONTEXT, 1);
        conf.setBoolean(Config.VOCAB, false);
        assertEquals(86, run(conf, new String[]{"-s", file.toURI().toURL().toString()}));
    }
}
