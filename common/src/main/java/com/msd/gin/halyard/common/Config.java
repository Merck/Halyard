package com.msd.gin.halyard.common;

import org.apache.hadoop.conf.Configuration;

public final class Config {
	public static String getString(String key, String defaultValue) {
		return System.getProperty(key, defaultValue);
	}

	public static String getString(Configuration config, String key, String defaultValue) {
		return getString(key, config.get(key, defaultValue));
	}

	public static boolean getBoolean(String key, boolean defaultValue) {
		String value = System.getProperty(key);
		return (value != null) ? Boolean.parseBoolean(value) : defaultValue;
	}

	public static boolean getBoolean(Configuration config, String key, boolean defaultValue) {
		return getBoolean(key, config.getBoolean(key, defaultValue));
	}

	public static int getInteger(String key, int defaultValue) {
		return Integer.getInteger(key, defaultValue);
	}

	public static int getInteger(Configuration config, String key, int defaultValue) {
		return getInteger(key, config.getInt(key, defaultValue));
	}
}
