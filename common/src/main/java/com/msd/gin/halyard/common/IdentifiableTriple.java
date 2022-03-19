package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;
import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.Triple;

public final class IdentifiableTriple extends TripleWrapper implements Identifiable, SerializableValue {
	private static final long serialVersionUID = 228285959274911416L;
	private Identifier id;
	private ByteBuffer ser;

	IdentifiableTriple(Triple triple) {
		super(triple);
	}

	@Override
	public Identifier getId() {
		if (id == null) {
			id = Identifier.create(triple, getSerializedForm());
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
			ser = ValueIO.CELL_WRITER.toBytes(triple);
		}
		return ser.duplicate();
	}

	private Object writeReplace() throws ObjectStreamException {
		// NB: CELL_WRITER output is not self-contained for Triples so must use STREAM_WRITER instead
		ByteBuffer serBuf = ValueIO.STREAM_WRITER.toBytes(triple);
		byte[] b = new byte[serBuf.remaining()];
		serBuf.get(b);
		return new SerializedValue(b);
	}
}
