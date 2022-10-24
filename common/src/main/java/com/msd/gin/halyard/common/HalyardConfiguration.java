package com.msd.gin.halyard.common;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

public class HalyardConfiguration extends Configuration {
	public HalyardConfiguration(Configuration conf) {
		super(false);
		addResource(Config.class.getResource("default-config.xml"));
		for (Map.Entry<String, String> entry : conf.getPropsWithPrefix("halyard.").entrySet()) {
			set("halyard." + entry.getKey(), entry.getValue());
		}
	}

	@Override
	public int hashCode() {
		return getProps().hashCode();
	}

	@Override
	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (other == null || this.getClass() != other.getClass()) {
			return false;
		}
		HalyardConfiguration that = (HalyardConfiguration) other;
		return this.getProps().equals(that.getProps());
	}
}
