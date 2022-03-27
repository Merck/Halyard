package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.eclipse.rdf4j.model.Triple;

public final class IdentifiableTriple extends TripleWrapper implements Identifiable, SerializableValue {
	private static final long serialVersionUID = 228285959274911416L;
	private final IdentifiableValueIO valueIO;
	private Identifier id;
	private ByteBuffer ser;

	IdentifiableTriple(Triple triple, IdentifiableValueIO valueIO) {
		super(triple);
		this.valueIO = Objects.requireNonNull(valueIO);
	}

	@Override
	public Identifier getId() {
		if (id == null) {
			id = valueIO.id(triple, getSerializedForm());
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
			byte[] b = valueIO.CELL_WRITER.toBytes(triple);
			ser = ByteBuffer.wrap(b).asReadOnlyBuffer();
		}
		return ser.duplicate();
	}

	private Object writeReplace() throws ObjectStreamException {
		// NB: CELL_WRITER output is not self-contained for Triples so must use STREAM_WRITER instead
		byte[] b = valueIO.STREAM_WRITER.toBytes(triple);
		return new SerializedValue(b, valueIO.STREAM_READER);
	}
}
