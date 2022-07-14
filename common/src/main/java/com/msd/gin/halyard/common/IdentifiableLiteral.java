package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;
import java.nio.ByteBuffer;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.Literal;

public final class IdentifiableLiteral extends LiteralWrapper implements IdentifiableValue, SerializableValue {
	private static final long serialVersionUID = 4299930477670062440L;
	private transient volatile Pair<ValueIdentifier,RDFFactory> cachedId = Pair.of(null, null);
	private transient volatile Pair<ByteBuffer,RDFFactory> cachedSer = Pair.of(null, null);

	IdentifiableLiteral(Literal literal) {
		super(literal);
	}

	@Override
	public ValueIdentifier getId(RDFFactory rdfFactory) {
		Pair<ValueIdentifier,RDFFactory> current = cachedId;
		if (current.getRight() != rdfFactory) {
			current = Pair.of(rdfFactory.id(literal, getSerializedForm(rdfFactory)), rdfFactory);
			cachedId = current;
		}
		return current.getLeft();
	}

	@Override
	public void setId(RDFFactory rdfFactory, ValueIdentifier id) {
		cachedId = Pair.of(id, rdfFactory);
	}

	@Override
	public ByteBuffer getSerializedForm(RDFFactory rdfFactory) {
		Pair<ByteBuffer,RDFFactory> current = cachedSer;
		if (current.getRight() != rdfFactory) {
			current = Pair.of(rdfFactory.getSerializedForm(literal), rdfFactory);
			cachedSer = current;
		}
		return current.getLeft().duplicate();
	}

	private Object writeReplace() throws ObjectStreamException {
		byte[] b = ValueIO.getDefault().createStreamWriter().toBytes(literal);
		return new SerializedValue(b);
	}
}
