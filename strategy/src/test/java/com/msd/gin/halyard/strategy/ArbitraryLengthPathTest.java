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
package com.msd.gin.halyard.strategy;

import java.util.Arrays;
import java.util.Collection;
import static junit.framework.TestCase.assertTrue;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * @author Adam Sotona (MSD)
 */
@RunWith(Parameterized.class)
public class ArbitraryLengthPathTest {
    @Parameterized.Parameters
    public static Collection<Integer> data() {
        return Arrays.asList(10, 100, 1000, 10000, 100000);
    }

    private final int n;
    private Repository repo;
    private RepositoryConnection con;

    public ArbitraryLengthPathTest(int n) {
        this.n = n;
    }

    @Before
    public void setUp() throws Exception {
        repo = new SailRepository(new MemoryStoreWithHalyardStrategy());
        repo.initialize();
        con = repo.getConnection();
    }

    @After
    public void tearDown() throws Exception {
        con.close();
        repo.shutDown();
    }

    @Test
    public void testN() throws Exception {
        ValueFactory vf = con.getValueFactory();
        for (int i = 0; i < n; i++) {
            con.add(vf.createURI("urn:test:root"), vf.createURI("urn:test:hasChild"), vf.createURI("urn:test:node" + i));
        }
        con.add(vf.createURI("urn:test:root"), vf.createURI("urn:test:hasChild"), vf.createURI("urn:test:node-end"));
        String sparql = "ASK { <urn:test:root> <urn:test:hasChild>* <urn:test:node-end> }";
        assertTrue(con.prepareBooleanQuery(QueryLanguage.SPARQL, sparql).evaluate());
    }
}
