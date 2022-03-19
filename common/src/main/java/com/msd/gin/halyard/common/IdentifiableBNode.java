package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;
import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.BNode;

public final class IdentifiableBNode extends BNodeWrapper implements Identifiable, SerializableValue {
	private static final long serialVersionUID = -6212507967580561560L;
	private Identifier id;
	private ByteBuffer ser;

	IdentifiableBNode(BNode bnode) {
		super(bnode);
	}

	@Override
	public Identifier getId() {
		if (id == null) {
			id = Identifier.create(bnode, getSerializedForm());
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
			ser = ValueIO.CELL_WRITER.toBytes(bnode);
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
