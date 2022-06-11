package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;
import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.Triple;

public final class IdentifiableTriple extends TripleWrapper implements IdentifiableValue, SerializableValue {
	private static final long serialVersionUID = 228285959274911416L;
	private RDFFactory rdfFactory;
	private ValueIdentifier id;
	private ByteBuffer ser;

	IdentifiableTriple(Triple triple) {
		super(triple);
	}

	private void validateCache(RDFFactory rdfFactory) {
		if (this.rdfFactory != rdfFactory) {
			this.id = null;
			this.ser = null;
			this.rdfFactory = rdfFactory;
		}
	}

	@Override
	public ValueIdentifier getId(RDFFactory rdfFactory) {
		validateCache(rdfFactory);
		if (id == null) {
			id = rdfFactory.id(triple, getSerializedForm(rdfFactory));
		}
		return id;
	}

	@Override
	public void setId(RDFFactory rdfFactory, ValueIdentifier id) {
		validateCache(rdfFactory);
		this.id = id;
	}

	@Override
	public ByteBuffer getSerializedForm(RDFFactory rdfFactory) {
		validateCache(rdfFactory);
		if (ser == null) {
			ser = rdfFactory.getSerializedForm(triple);
		}
		return ser.duplicate();
	}

	private Object writeReplace() throws ObjectStreamException {
		byte[] b = ValueIO.getDefault().createStreamWriter().toBytes(triple);
		return new SerializedValue(b);
	}
}
