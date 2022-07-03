package com.msd.gin.halyard.common;

import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;

public class HalyardConfiguration extends Configuration {
	public HalyardConfiguration(Configuration conf) {
		super(false);
		addResource(Config.class.getResource("default-config.xml"));
		for (Map.Entry<String, String> entry : conf.getPropsWithPrefix("halyard.").entrySet()) {
			set("halyard." + entry.getKey(), entry.getValue());
		}
	}

	public String getString(String key) {
		return Config.getString(key, Objects.requireNonNull(get(key), key));
	}

	public boolean getBoolean(String key) {
		return Config.getBoolean(key, Boolean.parseBoolean(Objects.requireNonNull(get(key), key)));
	}

	public int getInteger(String key) {
		return Config.getInteger(key, Integer.parseInt(Objects.requireNonNull(get(key), key)));
	}
}
