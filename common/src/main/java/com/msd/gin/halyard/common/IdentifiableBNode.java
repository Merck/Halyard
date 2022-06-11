package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;
import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.BNode;

public final class IdentifiableBNode extends BNodeWrapper implements IdentifiableValue, SerializableValue {
	private static final long serialVersionUID = -6212507967580561560L;
	private RDFFactory rdfFactory;
	private ValueIdentifier id;
	private ByteBuffer ser;

	IdentifiableBNode(BNode bnode) {
		super(bnode);
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
			id = rdfFactory.id(bnode, getSerializedForm(rdfFactory));
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
			ser = rdfFactory.getSerializedForm(bnode);
		}
		return ser.duplicate();
	}

	private Object writeReplace() throws ObjectStreamException {
		byte[] b = ValueIO.getDefault().createStreamWriter().toBytes(bnode);
		return new SerializedValue(b);
	}
}
