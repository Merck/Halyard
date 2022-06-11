package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;
import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.Literal;

public final class IdentifiableLiteral extends LiteralWrapper implements IdentifiableValue, SerializableValue {
	private static final long serialVersionUID = 4299930477670062440L;
	private RDFFactory rdfFactory;
	private ValueIdentifier id;
	private ByteBuffer ser;

	IdentifiableLiteral(Literal literal) {
		super(literal);
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
			id = rdfFactory.id(literal, getSerializedForm(rdfFactory));
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
			ser = rdfFactory.getSerializedForm(literal);
		}
		return ser.duplicate();
	}

	private Object writeReplace() throws ObjectStreamException {
		byte[] b = ValueIO.getDefault().createStreamWriter().toBytes(literal);
		return new SerializedValue(b);
	}
}
