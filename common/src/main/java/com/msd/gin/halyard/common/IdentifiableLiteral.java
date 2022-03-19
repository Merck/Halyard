package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;
import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.Literal;

public final class IdentifiableLiteral extends LiteralWrapper implements Identifiable, SerializableValue {
	private static final long serialVersionUID = 4299930477670062440L;
	private Identifier id;
	private ByteBuffer ser;

	IdentifiableLiteral(Literal literal) {
		super(literal);
	}

	@Override
	public Identifier getId() {
		if (id == null) {
			id = Identifier.create(literal, getSerializedForm());
		}
		return id;
	}

	@Override
	public void setId(Identifier id) {
		this.id = id;
	}

	@Override
	public ByteBuffer getSerializedForm() {
		if (ser == null) {
			ser = ValueIO.CELL_WRITER.toBytes(literal);
		}
		return ser.duplicate();
	}

	private Object writeReplace() throws ObjectStreamException {
		ByteBuffer serBuf = getSerializedForm();
		byte[] b = new byte[serBuf.remaining()];
		serBuf.get(b);
		return new SerializedValue(b);
	}
}
