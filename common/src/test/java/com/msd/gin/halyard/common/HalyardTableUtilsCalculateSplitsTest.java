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
package com.msd.gin.halyard.common;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
@RunWith(Parameterized.class)
public class HalyardTableUtilsCalculateSplitsTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
				{ 0, true, null, new String[] { "01", "02", "03", "04", "05" } },
				{ 1, true, null, new String[] { "008000", "01", "018000", "02", "028000", "03", "04", "05" } },
				{ 2, true, null, new String[] { "004000", "008000", "00c000", "01", "014000", "018000", "01c000", "02", "024000", "028000", "02c000", "03", "038000", "04", "048000", "05", "058000" } },
				{ 0, false, null, new String[] { "01", "02" } },
				{ 1, false, null, new String[] { "008000", "01", "018000", "02", "028000" } },
				{ 2, false, null, new String[] { "004000", "008000", "00c000", "01", "014000", "018000", "01c000", "02", "024000", "028000", "02c000" } },
				{ 2, false, Collections.singletonMap(RDF.VALUE, 0.5f), new String[] { "004000", "008000", "00c000", "01", "015fb029d0", "015fb029d08000", "018000", "02", "024000", "028000", "02c000" } },
				{ 2, false, Collections.singletonMap(RDF.VALUE, 0.8f), new String[] { "004000", "008000", "00c000", "01", "015fb029d04000", "015fb029d08000", "015fb029d0c000", "02", "024000", "028000", "02c000" } },
        });
    }

    private final int splits;
	private final boolean quads;
	private final Map<IRI,Float> predicateFractions;
    private final String[] expected;

	public HalyardTableUtilsCalculateSplitsTest(int splits, boolean quads, Map<IRI,Float> predicateFractions, String[] expected) {
        this.splits = splits;
		this.quads = quads;
		this.predicateFractions = predicateFractions;
        this.expected = expected;
    }

    @Test
    public void testCalculateSplits() {
		byte bb[][] = HalyardTableUtils.calculateSplits(splits, quads, predicateFractions);
        if (expected == null) {
            assertNull(bb);
        } else {
            assertEquals(expected.length, bb.length);
            for (int i = 0; i < expected.length; i++) {
                assertEquals("Split "+i, expected[i], Hex.encodeHexString(bb[i]));
            }
        }
    }

}
