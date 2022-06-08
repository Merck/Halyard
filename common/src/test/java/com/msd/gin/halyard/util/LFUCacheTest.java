package com.msd.gin.halyard.util;

import com.google.common.collect.Sets;

import java.util.Collections;

import org.junit.Test;

import static org.junit.Assert.*;

public class LFUCacheTest {
	@Test
	public void testEviction() {
		LFUCache<Integer,String> cache = new LFUCache<>(4, 0.1f);
		cache.put(1, "1");
		cache.put(2, "2");
		cache.put(3, "3");
		cache.put(4, "4");
		assertEquals(4, cache.size());
		cache.get(1);
		cache.get(1);
		cache.put(5, "5");
		assertEquals(4, cache.size());
		assertFalse(cache.isEmpty());
		assertEquals(3, cache.frequencyOf(1));
		assertEquals(1, cache.frequencyOf(5));
		assertEquals(Sets.newHashSet(1, 3, 4, 5), cache.keySet());
		assertTrue(cache.containsKey(1));
		cache.remove(1);
		assertFalse(cache.containsKey(1));
		assertEquals(Sets.newHashSet(3, 4, 5), cache.keySet());
		cache.clear();
		assertEquals(0, cache.size());
		assertTrue(cache.isEmpty());
		assertEquals(Collections.emptySet(), cache.keySet());
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testNotImplemented() {
		LFUCache<Integer,String> cache = new LFUCache<>(4, 0.1f);
		cache.entrySet();
	}
}
