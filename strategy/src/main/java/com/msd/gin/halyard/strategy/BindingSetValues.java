package com.msd.gin.halyard.strategy;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MutableBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;

final class BindingSetValues implements Serializable {

	private static final long serialVersionUID = -1412453629195059805L;

	public static final BindingSetValues EMPTY = new BindingSetValues(new Value[0]);

	private final Value[] values;

	private transient int hashcode;

	public static BindingSetValues create(String[] names, BindingSet bindings) {
		BindingSetValues key;
		int len = names.length;
		if (len > 0) {
			Value[] keyValues = new Value[len];
			for (int i = 0; i < len; i++) {
				Value value = bindings.getValue(names[i]);
				keyValues[i] = value;
			}
			key = new BindingSetValues(keyValues);
		} else {
			key = BindingSetValues.EMPTY;
		}
		return key;
	}

	private BindingSetValues(Value[] values) {
		this.values = values;
	}

	MutableBindingSet setBindings(String[] names, BindingSet bs) {
		QueryBindingSet result = new QueryBindingSet(bs);
		for (int i=0; i<names.length; i++) {
			String name = names[i];
			Value v = values[i];
			if (v != null) {
				result.setBinding(name, v);
			}
		}
		return result;
	}

	boolean canJoin(String[] names, Set<String> joinNames, BindingSet bs) {
		for (int i=0; i<names.length; i++) {
			String name = names[i];
			if (joinNames.contains(name)) {
				Value value = values[i];
				Value bsValue = bs.getValue(name);
				if (value != null && bsValue != null && !value.equals(bsValue)) {
					return false;
				}
			}
		}
		return true;
	}

	BindingSet joinTo(String[] names, BindingSet bs) {
		QueryBindingSet result = new QueryBindingSet(bs);
		for (int i=0; i<names.length; i++) {
			String name = names[i];
			if (!result.hasBinding(name)) {
				Value v = values[i];
				if (v != null) {
					result.setBinding(name, v);
				}
			}
		}
		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (!(o instanceof BindingSetValues)) {
			return false;
		}

		BindingSetValues other = (BindingSetValues) o;
		if (this.values.length != other.values.length) {
			return false;
		}

		if (this.hashcode != 0 && other.hashcode != 0 && this.hashcode != other.hashcode) {
			return false;
		}

		for (int i = values.length - 1; i >= 0; i--) {
			final Value v1 = this.values[i];
			final Value v2 = other.values[i];
			if (!Objects.equals(v1, v2)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public int hashCode() {
		if (hashcode == 0) {
			hashcode = Arrays.hashCode(values);
		}
		return hashcode;
	}

	@Override
	public String toString() {
		return Arrays.toString(values);
	}
}