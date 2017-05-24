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
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.codec.binary.Hex;
import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 * @author Adam Sotona (MSD)
 */
@RunWith(Parameterized.class)
public class HalyardTableUtilsCalculateSplitsTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {0, new String[]{"01", "02", "03", "04", "05"}},
            {1, new String[]{"008000", "01", "018000", "02", "028000", "03", "04", "05"}},
            {2, new String[]{"004000", "008000", "00c000", "01", "014000", "018000", "01c000", "02", "024000", "028000", "02c000", "03", "04", "05"}},
        });
    }

    private final int splits;
    private final String[] expected;

    public HalyardTableUtilsCalculateSplitsTest(int splits, String[] expected) {
        this.splits = splits;
        this.expected = expected;
    }

    @Test
    public void testCalculateSplits() {
        Map<String, Integer> cMap = new HashMap<>();
        byte bb[][] = HalyardTableUtils.calculateSplits(splits);
        if (expected == null) {
            assertNull(bb);
        } else {
            assertEquals(expected.length, bb.length);
            for (int i = 0; i < expected.length; i++) {
                assertEquals(expected[i], Hex.encodeHexString(bb[i]));
            }
        }
    }

}
