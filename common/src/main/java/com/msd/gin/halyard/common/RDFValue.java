package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.Value;

public abstract class RDFValue<V extends Value> extends RDFIdentifier {
	final V val;
	private ByteBuffer ser;

	public static <V extends Value> boolean matches(V value, RDFValue<V> pattern) {
		return pattern == null || pattern.val.equals(value);
	}


	protected RDFValue(RDFRole role, V val) {
		super(role);
		this.val = val;
	}

	public final ByteBuffer getSerializedForm() {
		if (ser == null) {
			if (val instanceof SerializableValue) {
				ser = ((SerializableValue) val).getSerializedForm();
			} else {
				ser = ValueIO.CELL_WRITER.toBytes(val);
			}
		}
		return ser.duplicate();
	}

	@Override
	protected final Identifier calculateId() {
		return Identifier.id(val);
	}

	@Override
	public String toString() {
		return val+" "+super.toString();
	}
}
