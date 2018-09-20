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
package com.msd.gin.halyard.strategy.collections;

import java.io.IOException;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class BigHashSetTest {

    @Test
    public void testBigHashSet() throws Exception {
        BigHashSet<String> bhs = new BigHashSet<>();
        bhs.add("hi");
        assertEquals("hi", bhs.iterator().next());
        assertTrue(bhs.contains("hi"));
        bhs.close();
        bhs.close();
    }

    @Test(expected = IOException.class)
    public void testFailAdd() throws Exception {
        BigHashSet<String> bhs = new BigHashSet<>();
        bhs.close();
        bhs.add("hi");
    }

    @Test(expected = IOException.class)
    public void testFailContains() throws Exception {
        BigHashSet<String> bhs = new BigHashSet<>();
        bhs.close();
        bhs.contains("hi");
    }
}
