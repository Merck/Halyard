package com.msd.gin.halyard.common;

public final class Config {
	public static String getString(String key, String defaultValue) {
		return System.getProperty(key, defaultValue);
	}

	public static int getInteger(String key, int defaultValue) {
		return Integer.getInteger(key, defaultValue);
	}
}
