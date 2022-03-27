package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;
import java.util.Objects;

import org.eclipse.rdf4j.model.Value;

public abstract class RDFValue<V extends Value> extends RDFIdentifier {
	final V val;
	private final IdentifiableValueIO valueIO;
	private ByteBuffer ser;

	public static <V extends Value> boolean matches(V value, RDFValue<V> pattern) {
		return pattern == null || pattern.val.equals(value);
	}


	protected RDFValue(RDFRole role, V val, IdentifiableValueIO valueIO) {
		super(role);
		this.val = Objects.requireNonNull(val);
		this.valueIO = Objects.requireNonNull(valueIO);
	}

	boolean isWellKnownIRI() {
		return valueIO.isWellKnownIRI(val);
	}

	public final ByteBuffer getSerializedForm() {
		if (ser == null) {
			if (val instanceof SerializableValue) {
				ser = ((SerializableValue) val).getSerializedForm();
			} else {
				byte[] b = valueIO.CELL_WRITER.toBytes(val);
				ser = ByteBuffer.wrap(b).asReadOnlyBuffer();
			}
		}
		return ser.duplicate();
	}

	@Override
	protected final Identifier calculateId() {
		return valueIO.id(val);
	}

	@Override
	public String toString() {
		return val+" "+super.toString();
	}
}
