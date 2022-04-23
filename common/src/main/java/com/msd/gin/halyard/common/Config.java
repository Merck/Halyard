package com.msd.gin.halyard.common;

import org.apache.hadoop.conf.Configuration;

public final class Config {
    public static final String ID_HASH = "halyard.id.hash";
    public static final String ID_SIZE = "halyard.id.size";
    public static final String ID_TYPE_INDEX = "halyard.id.type.index";
    public static final String KEY_SIZE_SUBJECT = "halyard.key.subject.size";
    public static final String END_KEY_SIZE_SUBJECT = "halyard.endKey.subject.size";
    public static final String KEY_SIZE_PREDICATE = "halyard.key.predicate.size";
    public static final String END_KEY_SIZE_PREDICATE = "halyard.endKey.predicate.size";
    public static final String KEY_SIZE_OBJECT = "halyard.key.object.size";
    public static final String END_KEY_SIZE_OBJECT = "halyard.endKey.object.size";
    public static final String KEY_SIZE_CONTEXT = "halyard.key.context.size";

	public static final String VOCAB = "halyard.vocabularies";
	public static final String LANG = "halyard.languages";
	public static final String STRING_COMPRESSION = "halyard.string.compressionThreshold";

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
