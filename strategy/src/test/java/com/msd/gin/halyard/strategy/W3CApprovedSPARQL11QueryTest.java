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

import junit.framework.Test;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.parser.sparql.manifest.SPARQL11ManifestTest;
import org.eclipse.rdf4j.query.parser.sparql.manifest.SPARQLQueryTest;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.dataset.DatasetRepository;

import org.eclipse.rdf4j.repository.sail.SailRepository;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class W3CApprovedSPARQL11QueryTest extends SPARQLQueryTest {

    public static Test suite() throws Exception {
        return SPARQL11ManifestTest.suite(new Factory() {
            @Override
            public W3CApprovedSPARQL11QueryTest createSPARQLQueryTest(String testURI, String name, String queryFileURL, String resultFileURL, Dataset dataSet, boolean laxCardinality) {
                return createSPARQLQueryTest(testURI, name, queryFileURL, resultFileURL, dataSet, laxCardinality, false);
            }
            @Override
            public W3CApprovedSPARQL11QueryTest createSPARQLQueryTest(String testURI, String name, String queryFileURL, String resultFileURL, Dataset dataSet, boolean laxCardinality, boolean checkOrder) {
                String[] ignoredTests = {
                    // test case incompatible with RDF 1.1 - see
                    // http://lists.w3.org/Archives/Public/public-sparql-dev/2013AprJun/0006.html
                    "STRDT   TypeErrors",
                    // test case incompatible with RDF 1.1 - see
                    // http://lists.w3.org/Archives/Public/public-sparql-dev/2013AprJun/0006.html
                    "STRLANG   TypeErrors",
                    // known issue: SES-937
                    "sq03 - Subquery within graph pattern, graph variable is not bound"};

                return new W3CApprovedSPARQL11QueryTest(testURI, name, queryFileURL, resultFileURL, dataSet, laxCardinality, checkOrder, ignoredTests);
            }
        }, true, true, false, "service");
    }

    protected W3CApprovedSPARQL11QueryTest(String testURI, String name, String queryFileURL, String resultFileURL, Dataset dataSet, boolean laxCardinality, boolean checkOrder, String... ignoredTests) {
        super(testURI, name, queryFileURL, resultFileURL, dataSet, laxCardinality, checkOrder, ignoredTests);
    }

    @Override
    protected Repository newRepository() {
        return new DatasetRepository(new SailRepository(new MemoryStoreWithHalyardStrategy()));
    }
}
