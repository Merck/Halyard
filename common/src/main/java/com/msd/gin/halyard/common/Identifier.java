package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.Value;

public final class Identifier {
	static final byte LITERAL_STOP_BITS = (byte) 0x40;
	private static final byte LITERAL_TYPE_BITS = (byte) 0x00;
	private static final byte TRIPLE_TYPE_BITS = (byte) 0x40;
	private static final byte IRI_TYPE_BITS = (byte) 0x80;
	private static final byte BNODE_TYPE_BITS = (byte) 0xC0;
	private static final byte TYPE_MASK = (byte) 0xC0;
	private static final byte CLEAR_TYPE_MASK = ~TYPE_MASK;

	static Identifier create(Value v, int typeIndex, IdentifiableValueIO valueIO) {
		ByteBuffer ser = ByteBuffer.allocate(ValueIO.DEFAULT_BUFFER_SIZE);
		ser = valueIO.CELL_WRITER.writeTo(v, ser);
		ser.flip();
		return Identifier.create(v, ser, typeIndex, valueIO);
	}

	static Identifier create(Value v, ByteBuffer ser, int typeIndex, IdentifiableValueIO valueIO) {
		byte[] hash = valueIO.hash(ser);
		byte typeBits;
		if (v.isIRI()) {
			typeBits = IRI_TYPE_BITS;
		} else if (v.isLiteral()) {
			typeBits = LITERAL_TYPE_BITS;
		} else if (v.isBNode()) {
			typeBits = BNODE_TYPE_BITS;
		} else if (v.isTriple()) {
			typeBits = TRIPLE_TYPE_BITS;
		} else {
			throw new AssertionError(String.format("Unexpected RDF value: %s", v.getClass()));
		}
		hash[typeIndex] = (byte) ((hash[typeIndex] & CLEAR_TYPE_MASK) | typeBits);
		return new Identifier(hash, typeIndex);
	}

	private final byte[] value;
	private final int typeIndex;
	private final int hashcode;

	Identifier(byte[] value, int typeIndex) {
		this.value = value;
		this.typeIndex = typeIndex;
		int h = value[0] & 0xFF;
		for (int i=1; i<Math.min(value.length, 4); i++) {
			h = (h << 8) | (value[i] & 0xFF);
		}
		this.hashcode = h;
	}

	public int size() {
		return value.length;
	}

	public final boolean isIRI() {
		return (value[typeIndex] & TYPE_MASK) == IRI_TYPE_BITS;
	}

	public final boolean isLiteral() {
		return (value[typeIndex] & TYPE_MASK) == LITERAL_TYPE_BITS;
	}

	public final boolean isBNode() {
		return (value[typeIndex] & TYPE_MASK) == BNODE_TYPE_BITS;
	}

	public final boolean isTriple() {
		return (value[typeIndex] & TYPE_MASK) == TRIPLE_TYPE_BITS;
	}

	public ByteBuffer writeTo(ByteBuffer bb) {
		return bb.put(value);
	}

	ByteBuffer writeSliceTo(int offset, int len, ByteBuffer bb) {
		return bb.put(value, offset, len);
	}

	final byte[] rotate(int offset, int len, int shift, byte[] dest) {
		return RDFRole.rotateRight(value, offset, len, shift, dest);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof Identifier)) {
			return false;
		}
		Identifier that = (Identifier) o;
		if (this.value.length != that.value.length) {
			return false;
		}
		for (int i=this.value.length-1; i>=0; i--) {
			if (this.value[i] != that.value[i]) {
				return false;
			}
		}
		return true;
	}

	@Override
	public int hashCode() {
		return hashcode;
	}

	@Override
	public String toString() {
		return Hashes.encode(value);
	}
}
