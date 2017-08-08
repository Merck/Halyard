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

import org.eclipse.rdf4j.model.impl.TreeModel;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HBaseSailConfigTest {

    @Test
    public void testTablespace() {
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.setTablespace("whatevertable");
    }

    @Test
    public void testSplitBits() {
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.setSplitBits(7);
        assertEquals(7, cfg.getSplitBits());
    }

    @Test
    public void testCreate() {
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.setCreate(false);
        assertFalse(cfg.isCreate());
        cfg.setCreate(true);
        assertTrue(cfg.isCreate());
    }

    @Test
    public void testPush() {
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.setPush(false);
        assertFalse(cfg.isPush());
        cfg.setPush(true);
        assertTrue(cfg.isPush());
    }

    @Test
    public void testEvaluationTimeout() {
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.setEvaluationTimeout(360);
        assertEquals(360, cfg.getEvaluationTimeout());
    }

    @Test
    public void testElasticIndex() {
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.setElasticIndexURL("http://localhost:12345/index");
        assertEquals("http://localhost:12345/index", cfg.getElasticIndexURL());
    }

    @Test
    public void testExportAndParse() throws Exception {
        HBaseSailConfig cfg = new HBaseSailConfig();
        cfg.setTablespace("whatevertable");
        cfg.setSplitBits(7);
        cfg.setCreate(false);
        cfg.setPush(false);
        cfg.setElasticIndexURL("http://whateverURL/index");
        TreeModel g = new TreeModel();
        cfg.export(g);
        cfg = new HBaseSailConfig();
        cfg.parse(g, null);
        assertEquals("whatevertable", cfg.getTablespace());
        assertEquals(7, cfg.getSplitBits());
        assertFalse(cfg.isCreate());
        assertFalse(cfg.isPush());
        assertEquals("http://whateverURL/index", cfg.getElasticIndexURL());
    }
}
