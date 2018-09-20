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
import java.util.Iterator;
import java.util.Map.Entry;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class SorterTest {

    @Test
    public void testNoLimitNoDistinct() throws Exception {
        Sorter<String> s = new Sorter<>(Long.MAX_VALUE, false);
        s.add("C");
        s.add("C");
        s.add("A");
        s.add("B");
        s.add("B");
        Iterator<Entry<String, Long>> it = s.iterator();
        assertEquals("A", 1, it.next());
        assertEquals("B", 2, it.next());
        assertEquals("C", 2, it.next());
        Assert.assertFalse(it.hasNext());
        s.close();
        s.close();
    }

    @Test
    public void testNoLimitDistinct() throws Exception {
        Sorter<String> s = new Sorter<>(Long.MAX_VALUE, true);
        s.add("C");
        s.add("C");
        s.add("A");
        s.add("B");
        s.add("B");
        Iterator<Entry<String, Long>> it = s.iterator();
        assertEquals("A", 1, it.next());
        assertEquals("B", 1, it.next());
        assertEquals("C", 1, it.next());
        Assert.assertFalse(it.hasNext());
        s.close();
        s.close();
    }

    @Test
    public void testLimitNoDistinct() throws Exception {
        Sorter<String> s = new Sorter<>(3, false);
        s.add("C");
        s.add("C");
        s.add("A");
        s.add("A");
        s.add("B");
        s.add("B");
        s.add("B");
        Iterator<Entry<String, Long>> it = s.iterator();
        assertEquals("A", 2, it.next());
        assertEquals("B", 1, it.next());
        Assert.assertFalse(it.hasNext());
        s.close();
        s.close();
    }

    @Test
    public void testLimitDistinct() throws Exception {
        Sorter<String> s = new Sorter<>(2, true);
        s.add("C");
        s.add("C");
        s.add("A");
        s.add("B");
        s.add("A");
        s.add("B");
        s.add("B");
        Iterator<Entry<String, Long>> it = s.iterator();
        assertEquals("A", 1, it.next());
        assertEquals("B", 1, it.next());
        Assert.assertFalse(it.hasNext());
        s.close();
        s.close();
    }

    private static void assertEquals(String s, long l, Entry<String, Long> e) {
        Assert.assertEquals(s, e.getKey());
        Assert.assertEquals(s, l, (long)e.getValue());
    }

    @Test(expected = IOException.class)
    public void testFailAdd() throws Exception {
        Sorter<String> s = new Sorter<>(Long.MAX_VALUE, false);
        s.close();
        s.add("hi");
    }
}
