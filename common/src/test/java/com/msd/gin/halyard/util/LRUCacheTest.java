package com.msd.gin.halyard.util;

import com.google.common.collect.Sets;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class LRUCacheTest {
	@Test
	public void testEviction() {
		LRUCache<Integer,String> cache = new LRUCache<>(4);
		cache.put(1, "1");
		cache.put(2, "2");
		cache.put(3, "3");
		cache.put(4, "4");
		assertEquals(4, cache.size());
		cache.get(4);
		cache.get(3);
		cache.get(2);
		cache.get(1);
		cache.put(5, "5");
		assertEquals(4, cache.size());
		assertFalse(cache.isEmpty());
		assertEquals(Sets.newHashSet(1, 2, 3, 5), cache.keySet());
		assertTrue(cache.containsKey(1));
		cache.remove(1);
		assertFalse(cache.containsKey(1));
		assertEquals(Sets.newHashSet(2, 3, 5), cache.keySet());
		cache.clear();
		assertEquals(0, cache.size());
		assertTrue(cache.isEmpty());
		assertEquals(Collections.emptySet(), cache.keySet());
	}
}
