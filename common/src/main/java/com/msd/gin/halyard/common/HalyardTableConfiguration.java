package com.msd.gin.halyard.common;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * Persistable configuration only.
 */
final class HalyardTableConfiguration extends Configuration {
	public HalyardTableConfiguration(Configuration conf) {
		super(false);
		addResource(TableConfig.class.getResource("default-config.xml"));
		for (Map.Entry<String, String> entry : conf.getPropsWithPrefix("halyard.").entrySet()) {
			String prop = "halyard." + entry.getKey();
			if (TableConfig.contains(prop)) {
				set(prop, entry.getValue());
			}
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
		HalyardTableConfiguration that = (HalyardTableConfiguration) other;
		return this.getProps().equals(that.getProps());
	}
}
