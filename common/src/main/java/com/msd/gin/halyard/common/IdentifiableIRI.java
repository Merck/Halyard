package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.URIUtil;

public final class IdentifiableIRI implements IRI, IdentifiableValue, SerializableValue {
	private static final long serialVersionUID = 8055405742401584331L;
	private final String iri;
	private final int localNameIdx;
	private transient volatile Pair<ValueIdentifier,RDFFactory> cachedId = Pair.of(null, null);
	private transient volatile Pair<ByteBuffer,RDFFactory> cachedSer = Pair.of(null, null);

	IdentifiableIRI(String iri) {
		if (iri.indexOf(':') == -1) {
			throw new IllegalArgumentException(String.format("Not a valid (absolute) IRI: %s", iri));
		}
		this.iri = Objects.requireNonNull(iri);
		this.localNameIdx = URIUtil.getLocalNameIndex(iri);
	}

	IdentifiableIRI(String namespace, String localName) {
		Objects.requireNonNull(namespace, "Namespace is null");
		Objects.requireNonNull(localName, "Local name is null");
		if (namespace.indexOf(':') == -1) {
			throw new IllegalArgumentException(String.format("Not a valid (absolute) IRI: %s", namespace));
		}
		this.iri = namespace + localName;
		this.localNameIdx = namespace.length();
	}

	@Override
	public String getNamespace() {
		return iri.substring(0, localNameIdx);
	}

	@Override
	public String getLocalName() {
		return iri.substring(localNameIdx);
	}

	@Override
	public final String stringValue() {
		return iri;
	}

	@Override
	public boolean equals(Object o) {
		return this == o || o instanceof IRI
				&& iri.equals(((IRI) o).stringValue());
	}

	@Override
	public int hashCode() {
		return iri.hashCode();
	}

	@Override
	public final String toString() {
		return iri;
	}

	@Override
	public ValueIdentifier getId(RDFFactory rdfFactory) {
		Pair<ValueIdentifier,RDFFactory> current = cachedId;
		if (current.getRight() != rdfFactory) {
			current = Pair.of(rdfFactory.id(this, getSerializedForm(rdfFactory)), rdfFactory);
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
			current = Pair.of(rdfFactory.getSerializedForm(this), rdfFactory);
			cachedSer = current;
		}
		return current.getLeft().duplicate();
	}

	private Object writeReplace() throws ObjectStreamException {
		byte[] b = ValueIO.getDefault().createStreamWriter().toBytes(this);
		return new SerializedValue(b);
	}
}
