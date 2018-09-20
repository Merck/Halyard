/*
 * Copyright 2018 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
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
import org.apache.commons.cli.MissingOptionException;
import org.junit.Test;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.util.ToolRunner;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.repository.RepositoryException;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardProfileTest {

    @Test
    public void testHelp() throws Exception {
        runProfile("-h");
    }

    @Test(expected = MissingOptionException.class)
    public void testRunNoArgs() throws Exception {
        runProfile();
    }

    @Test
    public void testVersion() throws Exception {
        runProfile("-v");
    }

    @Test(expected = ParseException.class)
    public void testMissingArgs() throws Exception {
        runProfile("-s", "whatever");
    }

    @Test(expected = ParseException.class)
    public void testUnknownArg() throws Exception {
        runProfile("-y");
    }

    @Test(expected = ParseException.class)
    public void testDupArgs() throws Exception {
        runProfile("-s", "whatever", "-q", "query", "-s", "whatever2");
    }

    @Test(expected = MalformedQueryException.class)
    public void testInvalidQuery() throws Exception {
        HalyardTableUtils.getTable(HBaseServerTestInstance.getInstanceConfig(), "profile", true, -1);
        runProfile("-s", "profile", "-q", "invalid * query {}");
    }

    @Test(expected = RepositoryException.class)
    public void testNonexistentRepository() throws Exception {
        runProfile("-s", "whatever", "-q", "construct {?s ?p ?o} where {?s ?p ?o}");
    }

    @Test
    public void testProfile() throws Exception {
        HalyardTableUtils.getTable(HBaseServerTestInstance.getInstanceConfig(), "profile", true, -1);
        runProfile("-s", "profile", "-q", "select * where {{?s <http://whatever/pred> ?o, ?o2} union {?s ?p ?o3. optional {?s ?p2 ?o4 }}}");
    }

    @Test
    public void testProfileGraph() throws Exception {
        HalyardTableUtils.getTable(HBaseServerTestInstance.getInstanceConfig(), "profile", true, -1);
        runProfile("-s", "profile", "-q", "construct {?s ?o ?o2} where {{?s <http://whatever/pred> ?o, ?o2} union {?s ?p ?o3. optional {?s ?p2 ?o4 }}}");
    }

    @Test
    public void testProfileBoolean() throws Exception {
        HalyardTableUtils.getTable(HBaseServerTestInstance.getInstanceConfig(), "profile", true, -1);
        runProfile("-s", "profile", "-q", "ask where {{?s <http://whatever/pred> ?o, ?o2} union {?s ?p ?o3. optional {?s ?p2 ?o4 }}}");
    }

    private static int runProfile(String ... args) throws Exception {
        return ToolRunner.run(HBaseServerTestInstance.getInstanceConfig(), new HalyardProfile(), args);
    }
}
