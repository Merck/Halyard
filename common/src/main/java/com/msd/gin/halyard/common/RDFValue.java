package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

public final class RDFValue<V extends Value> {
	final V val;
	final byte[] ser;
	final byte[] hash;

	public static RDFValue<Resource> createSubject(Resource subj) {
		if(subj == null) {
			return null;
		}
		byte[] b = HalyardTableUtils.writeBytes(subj);
		byte[] h = HalyardTableUtils.hashSubject(b);
		return new RDFValue<>(subj, b, h);
	}


	public static RDFValue<IRI> createPredicate(IRI pred) {
		if(pred == null) {
			return null;
		}
		byte[] b = HalyardTableUtils.writeBytes(pred);
		byte[] h = HalyardTableUtils.hashPredicate(b);
		return new RDFValue<>(pred, b, h);
	}

	public static RDFValue<Value> createObject(Value obj) {
		if(obj == null) {
			return null;
		}
		byte[] b = HalyardTableUtils.writeBytes(obj);
		byte[] h = HalyardTableUtils.hashObject(b);
		return new RDFValue<>(obj, b, h);
	}

	public static RDFValue<Resource> createContext(Resource ctx) {
		if(ctx == null) {
			return null;
		}
		byte[] b = HalyardTableUtils.writeBytes(ctx);
		byte[] h = HalyardTableUtils.hashContext(b);
		return new RDFValue<>(ctx, b, h);
	}

	public static <V extends Value> boolean matches(V value, RDFValue<V> pattern) {
		return pattern == null || pattern.val.equals(value);
	}

	private RDFValue(V val, byte[] ser, byte[] h) {
		this.val = val;
		this.ser = ser;
		this.hash = h;
	}
}
