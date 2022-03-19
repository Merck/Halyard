package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;
import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.IRI;

public final class IdentifiableIRI extends IRIWrapper implements Identifiable, SerializableValue {
	private static final long serialVersionUID = 8055405742401584331L;
	private Identifier id;
	private ByteBuffer ser;

	IdentifiableIRI(IRI iri) {
		super(iri);
	}

	@Override
	public Identifier getId() {
		if (id == null) {
			id = ValueIO.WELL_KNOWN_IRI_IDS.inverse().get(iri);
			if (id == null) {
				id = Identifier.create(iri, getSerializedForm());
			}
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
			ser = ValueIO.CELL_WRITER.toBytes(iri);
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
