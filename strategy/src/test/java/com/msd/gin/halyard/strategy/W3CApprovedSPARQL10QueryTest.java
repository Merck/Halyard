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

import java.net.URL;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.List;

import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.testsuite.query.parser.sparql.manifest.SPARQL10QueryComplianceTest;
import org.eclipse.rdf4j.testsuite.query.parser.sparql.manifest.SPARQLQueryComplianceTest;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.dataset.DatasetRepository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 * @author Adam Sotona (MSD)
 */
@RunWith(Parameterized.class)
public class W3CApprovedSPARQL10QueryTest extends SPARQLQueryComplianceTest {

	private static final String[] defaultIgnoredTests = {
			// incompatible with SPARQL 1.1 - syntax for decimals was modified
			"Basic - Term 6",
			// incompatible with SPARQL 1.1 - syntax for decimals was modified
			"Basic - Term 7",
			// Test is incorrect: assumes timezoned date is comparable with non-timezoned
			"date-2",
			// Incompatible with SPARQL 1.1 - string-typed literals and plain literals are identical
			"Strings: Distinct",
			// Incompatible with SPARQL 1.1 - string-typed literals and plain literals are identical
			"All: Distinct",
			// Incompatible with SPARQL 1.1 - string-typed literals and plain literals are identical
			"SELECT REDUCED ?x with strings",
			// incompatible with non-strict date comparisons
			"date-3",
			"open-cmp-01",
			"open-cmp-02",
			// incompatible with non-sequential retrieval
			"SELECT REDUCED *"
	};

	private static final List<String> excludedSubdirs = Arrays.asList("service");

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> data() {
		return Arrays.asList(getTestData());
	}

	private static Object[][] getTestData() {
		List<Object[]> tests = new ArrayList<>();

		Deque<String> manifests = new ArrayDeque<>();
		manifests.add(getManifestURL().toExternalForm());
		while (!manifests.isEmpty()) {
			String pop = manifests.pop();
			SPARQLQueryTestManifest manifest = new SPARQLQueryTestManifest(pop, excludedSubdirs);
			tests.addAll(manifest.getTests());
			manifests.addAll(manifest.getSubManifests());
		}

		Object[][] result = new Object[tests.size()][6];
		tests.toArray(result);

		return result;
	}

	protected static URL getManifestURL() {
		return SPARQL10QueryComplianceTest.class.getClassLoader()
				.getResource("testcases-sparql-1.0-w3c/data-r2/manifest-evaluation.ttl");
	}

    public W3CApprovedSPARQL10QueryTest(String displayName, String testURI, String name, String queryFileURL,
			String resultFileURL, Dataset dataset, boolean ordered, boolean laxCardinality) {
		super(displayName, testURI, name, queryFileURL, resultFileURL, dataset, ordered, laxCardinality);
    }

    @Override
    protected Repository newRepository() {
        return new DatasetRepository(new SailRepository(new MemoryStoreWithHalyardStrategy()));
    }

	@Override
	@Before
	public void setUp() throws Exception {
		super.setUp();
		for (String defaultIgnoredTest : defaultIgnoredTests) {
			addIgnoredTest(defaultIgnoredTest);
		}
	}
}
