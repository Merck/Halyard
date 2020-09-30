/*
 * Copyright 2019 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
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

import com.msd.gin.halyard.common.HBaseServerTestInstance;

import java.net.MalformedURLException;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.eclipse.rdf4j.repository.sail.config.SailRepositoryConfig;
import org.eclipse.rdf4j.sail.memory.config.MemoryStoreConfig;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HBaseRepositoryManagerTest {

    @Test (expected = MalformedURLException.class)
    public void testGetLocation() throws Exception {
        new HBaseRepositoryManager().getLocation();
    }

    @Test
    public void testHttpClient() {
        HBaseRepositoryManager rm = new HBaseRepositoryManager();
        assertNull(rm.getHttpClient());
		HttpClient cl = HttpClientBuilder.create().build();
        rm.setHttpClient(cl);
        assertEquals(cl, rm.getHttpClient());
    }

    @Test
    public void testAddRepositoryPersists() throws Exception {
        HBaseRepositoryManager rm = new HBaseRepositoryManager();
        rm.overrideConfiguration(HBaseServerTestInstance.getInstanceConfig());
        rm.init();
        rm.addRepositoryConfig(new RepositoryConfig("repoTest", new SailRepositoryConfig(new MemoryStoreConfig(false))));
        assertTrue(rm.getAllRepositories().contains(rm.getRepository("repoTest")));
        rm.shutDown();
        //test persistence
        rm = new HBaseRepositoryManager();
        rm.overrideConfiguration(HBaseServerTestInstance.getInstanceConfig());
        rm.init();
        assertTrue(rm.getAllRepositories().contains(rm.getRepository("repoTest")));
        rm.shutDown();
    }
}
