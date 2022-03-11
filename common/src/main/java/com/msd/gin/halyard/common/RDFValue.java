package com.msd.gin.halyard.common;

import com.msd.gin.halyard.common.HalyardTableUtils.TableTripleWriter;

import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.Value;

public abstract class RDFValue<V extends Value> extends RDFIdentifier {
	private static final TableTripleWriter TW = new TableTripleWriter();

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
			ByteBuffer tmp = ByteBuffer.allocate(128);
			tmp = ValueIO.writeBytes(val, tmp, TW);
			tmp.flip();
			ByteBuffer b = ByteBuffer.allocate(tmp.remaining());
			b.put(tmp);
			b.flip();
			ser = b.asReadOnlyBuffer();
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
