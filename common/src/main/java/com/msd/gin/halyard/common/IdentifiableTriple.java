package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.eclipse.rdf4j.model.Triple;

public final class IdentifiableTriple extends TripleWrapper implements Identifiable, SerializableValue {
	private static final long serialVersionUID = 228285959274911416L;
	private final RDFFactory rdfFactory;
	private Identifier id;
	private ByteBuffer ser;

	IdentifiableTriple(Triple triple, RDFFactory valueIO) {
		super(triple);
		this.rdfFactory = Objects.requireNonNull(valueIO);
	}

	@Override
	public Identifier getId() {
		if (id == null) {
			id = rdfFactory.id(triple, getSerializedForm());
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
			ser = rdfFactory.getSerializedForm(triple);
		}
		return ser.duplicate();
	}

	private Object writeReplace() throws ObjectStreamException {
		// NB: CELL_WRITER output is not self-contained for Triples so must use STREAM_WRITER instead
		byte[] b = rdfFactory.streamWriter.toBytes(triple);
		return new SerializedValue(b, rdfFactory.streamReader);
	}
}
