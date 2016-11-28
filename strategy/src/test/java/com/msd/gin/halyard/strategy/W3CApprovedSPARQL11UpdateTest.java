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

import java.util.Map;

import junit.framework.Test;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.parser.sparql.manifest.SPARQL11ManifestTest;
import org.eclipse.rdf4j.query.parser.sparql.manifest.SPARQLUpdateConformanceTest;
import org.eclipse.rdf4j.repository.contextaware.ContextAwareRepository;
import org.eclipse.rdf4j.repository.sail.SailRepository;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class W3CApprovedSPARQL11UpdateTest extends SPARQLUpdateConformanceTest {

    public W3CApprovedSPARQL11UpdateTest(String testURI, String name, String requestFile, IRI defaultGraphURI, Map<String, IRI> inputNamedGraphs, IRI resultDefaultGraphURI, Map<String, IRI> resultNamedGraphs) {
        super(testURI, name, requestFile, defaultGraphURI, inputNamedGraphs, resultDefaultGraphURI, resultNamedGraphs);
    }

    public static Test suite() throws Exception {
        return SPARQL11ManifestTest.suite(
                (String testURI1, String name, String requestFile, IRI defaultGraphURI, Map<String, IRI> inputNamedGraphs1, IRI resultDefaultGraphURI, Map<String, IRI> resultNamedGraphs1)
                        -> new W3CApprovedSPARQL11UpdateTest(testURI1, name, requestFile, defaultGraphURI, inputNamedGraphs1, resultDefaultGraphURI, resultNamedGraphs1), true, true, false);
    }

    @Override
    protected ContextAwareRepository newRepository() throws Exception {
        SailRepository repo = new SailRepository(new MemoryStoreWithHalyardStrategy());
        return new ContextAwareRepository(repo);
    }
}
