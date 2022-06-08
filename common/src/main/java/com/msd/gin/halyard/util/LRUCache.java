package com.msd.gin.halyard.util;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<Key, Value> extends LinkedHashMap<Key, Value> {

	private static final long serialVersionUID = 1650156164084792963L;

	private final int maxEntries;

	public LRUCache(int maxEntries) {
		super((int) Math.ceil((maxEntries + 1)/0.75), 0.75f, true);
		this.maxEntries = maxEntries;
	}

	@Override
	protected boolean removeEldestEntry(Map.Entry<Key, Value> eldest) {
		return size() > maxEntries;
	}
}
