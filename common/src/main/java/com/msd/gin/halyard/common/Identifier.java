package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.Value;

public final class Identifier {
	public static final int ID_SIZE = Hashes.hashUniqueSize();
	private static final byte LITERAL_TYPE_BITS = (byte) 0x00;
	private static final byte TRIPLE_TYPE_BITS = (byte) 0x40;
	private static final byte IRI_TYPE_BITS = (byte) 0x80;
	private static final byte BNODE_TYPE_BITS = (byte) 0xC0;
	private static final byte TYPE_MASK = (byte) 0xC0;
	private static final byte CLEAR_TYPE_MASK = ~TYPE_MASK;
	private static final int TYPE_INDEX = (ID_SIZE > 1) ? 1 : 0;
	static final int TYPE_SALT_SIZE = 1<<(8*TYPE_INDEX);
	static final byte LITERAL_STOP_BITS = (byte) 0x40;

	private final byte[] value;
	private final int hashcode;

	public static Identifier id(Value v) {
		if (v instanceof Identifiable) {
			return ((Identifiable) v).getId();
		}

		Identifier id = ValueIO.WELL_KNOWN_IRI_IDS.inverse().get(v);
		if (id != null) {
			return id;
		}

		return create(v);
	}

	private static Identifier create(Value v) {
		ByteBuffer ser = ByteBuffer.allocate(ValueIO.DEFAULT_BUFFER_SIZE);
		ser = ValueIO.CELL_WRITER.writeTo(v, ser);
		ser.flip();
		return create(v, ser);
	}

	static Identifier create(Value v, ByteBuffer ser) {
		byte[] hash = Hashes.hashUnique(ser);
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
		hash[TYPE_INDEX] = (byte) ((hash[TYPE_INDEX] & CLEAR_TYPE_MASK) | typeBits);
		return new Identifier(hash);
	}

	public Identifier(byte[] value) {
		if (value.length != ID_SIZE) {
			throw new IllegalArgumentException("Byte array has incorrect length");
		}
		this.value = value;
		int h = value[0] & 0xFF;
		for (int i=1; i<Math.min(value.length, 4); i++) {
			h = (h << 8) | (value[i] & 0xFF);
		}
		this.hashcode = h;
	}

	public final boolean isIRI() {
		return (value[TYPE_INDEX] & TYPE_MASK) == IRI_TYPE_BITS;
	}

	public final boolean isLiteral() {
		return (value[TYPE_INDEX] & TYPE_MASK) == LITERAL_TYPE_BITS;
	}

	public final boolean isBNode() {
		return (value[TYPE_INDEX] & TYPE_MASK) == BNODE_TYPE_BITS;
	}

	public final boolean isTriple() {
		return (value[TYPE_INDEX] & TYPE_MASK) == TRIPLE_TYPE_BITS;
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
		for (int i=ID_SIZE-1; i>=0; i--) {
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
