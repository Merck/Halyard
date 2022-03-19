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
				{ 0, true, null, 0.5f, new String[] { "01", "02", "03", "04", "05" } },
				{ 1, true, null, 0.5f, new String[] { "00c000", "01", "01c000", "02", "028000", "03", "04", "05" } },
				{ 2, true, null, 0.5f, new String[] { "00a000", "00c000", "00e000", "01", "01a000", "01c000", "01e000", "02", "024000", "028000", "02c000", "03", "03c000", "04", "04c000", "05", "05c000" } },
				{ 0, false, null, 0.5f, new String[] { "01", "02" } },
				{ 1, false, null, 0.5f, new String[] { "00c000", "01", "01c000", "02", "028000" } },
				{ 2, false, null, 0.5f, new String[] { "00a000", "00c000", "00e000", "01", "01a000", "01c000", "01e000", "02", "024000", "028000", "02c000" } },
				{ 2, false, null, 0.0f, new String[] { "00a000", "00c000", "00e000", "01", "01a000", "01c000", "01e000", "02", "02a000", "02c000", "02e000" } },
				{ 2, false, null, 1.0f, new String[] { "00a000", "00c000", "00e000", "01", "01a000", "01c000", "01e000", "02", "022000", "024000", "026000" } },
				{ 2, false, null, 0.8f, new String[] { "00a000", "00c000", "00e000", "01", "01a000", "01c000", "01e000", "02", "022000", "024000", "026000" } },
				{ 2, false, Collections.singletonMap(RDF.VALUE, 0.5f), 0.0f, new String[] { "00a000", "00c000", "00e000", "01", "01c000", "01df3029d0", "01df3029d08000", "02", "02a000", "02c000", "02e000" } },
				{ 2, false, Collections.singletonMap(RDF.VALUE, 0.8f), 0.0f, new String[] { "00a000", "00c000", "00e000", "01", "01df3029d04000", "01df3029d08000", "01df3029d0c000", "02", "02a000", "02c000", "02e000" } },
        });
    }

    private final int splits;
	private final boolean quads;
	private final Map<IRI,Float> predicateFractions;
	private final float literalRatio;
    private final String[] expected;

	public HalyardTableUtilsCalculateSplitsTest(int splits, boolean quads, Map<IRI,Float> predicateFractions, float literalRatio, String[] expected) {
        this.splits = splits;
		this.quads = quads;
		this.predicateFractions = predicateFractions;
		this.literalRatio = literalRatio;
        this.expected = expected;
    }

    @Test
    public void testCalculateSplits() {
		byte bb[][] = HalyardTableUtils.calculateSplits(splits, quads, predicateFractions, literalRatio);
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
