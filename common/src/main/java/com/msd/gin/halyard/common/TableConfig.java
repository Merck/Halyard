package com.msd.gin.halyard.common;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;

public final class TableConfig {
    public static final String ID_HASH = "halyard.id.hash";
    public static final String ID_SIZE = "halyard.id.size";
    public static final String ID_TYPE_INDEX = "halyard.id.type.index";
    public static final String ID_TYPE_NIBBLE = "halyard.id.type.nibble";
    public static final String KEY_SIZE_SUBJECT = "halyard.key.subject.size";
    public static final String END_KEY_SIZE_SUBJECT = "halyard.endKey.subject.size";
    public static final String KEY_SIZE_PREDICATE = "halyard.key.predicate.size";
    public static final String END_KEY_SIZE_PREDICATE = "halyard.endKey.predicate.size";
    public static final String KEY_SIZE_OBJECT = "halyard.key.object.size";
    public static final String END_KEY_SIZE_OBJECT = "halyard.endKey.object.size";
    public static final String KEY_SIZE_CONTEXT = "halyard.key.context.size";
    public static final String END_KEY_SIZE_CONTEXT = "halyard.endKey.context.size";

	public static final String VOCAB = "halyard.vocabularies";
	public static final String LANG = "halyard.languages";
	public static final String STRING_COMPRESSION = "halyard.string.compressionThreshold";

	private static final Set<String> PROPERTIES;

	static {
		PROPERTIES = new HashSet<>();
		try {
			for (Field f : TableConfig.class.getFields()) {
				if (Modifier.isStatic(f.getModifiers())) {
					PROPERTIES.add((String) f.get(null));
				}
			}
		} catch (IllegalAccessException e) {
			throw new AssertionError(e);
		}
	}

	public static boolean contains(String prop) {
		return PROPERTIES.contains(prop);
	}

	private TableConfig() {}
}
