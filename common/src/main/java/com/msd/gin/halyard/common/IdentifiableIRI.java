package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.URIUtil;

public final class IdentifiableIRI implements IRI, IdentifiableValue, SerializableValue {
	private static final long serialVersionUID = 8055405742401584331L;
	private final String iri;
	private int localNameIdx = -1;
	private RDFFactory rdfFactory;
	private ValueIdentifier id;
	private ByteBuffer ser;

	IdentifiableIRI(String iri) {
		if (iri.indexOf(':') == -1) {
			throw new IllegalArgumentException(String.format("Not a valid (absolute) IRI: %s", iri));
		}
		this.iri = Objects.requireNonNull(iri);
	}

	IdentifiableIRI(String namespace, String localName) {
		this(Objects.requireNonNull(namespace, "Namespace is null") + Objects.requireNonNull(localName, "Local name is null"));
		localNameIdx = namespace.length();
	}

	@Override
	public String getNamespace() {
		if (localNameIdx < 0) {
			localNameIdx = URIUtil.getLocalNameIndex(iri);
		}
		return iri.substring(0, localNameIdx);
	}

	@Override
	public String getLocalName() {
		if (localNameIdx < 0) {
			localNameIdx = URIUtil.getLocalNameIndex(iri);
		}
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
			id = rdfFactory.id(this, getSerializedForm(rdfFactory));
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
			ser = rdfFactory.getSerializedForm(this);
		}
		return ser.duplicate();
	}

	private Object writeReplace() throws ObjectStreamException {
		byte[] b = ValueIO.getDefault().createStreamWriter().toBytes(this);
		return new SerializedValue(b);
	}
}
