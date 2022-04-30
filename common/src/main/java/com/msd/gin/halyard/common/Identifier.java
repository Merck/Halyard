package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;

public final class Identifier {
	static final byte LITERAL_STOP_BITS = (byte) 0x40;
	private static final byte LITERAL_TYPE_BITS = (byte) 0x00;
	private static final byte TRIPLE_TYPE_BITS = (byte) 0x40;
	private static final byte IRI_TYPE_BITS = (byte) 0x80;
	private static final byte BNODE_TYPE_BITS = (byte) 0xC0;
	private static final byte TYPE_MASK = (byte) 0xC0;
	private static final byte CLEAR_TYPE_MASK = ~TYPE_MASK;
	static final byte NONSTRING_DATATYPE_BITS = (byte) 0x00;
	static final byte STRING_DATATYPE_BITS = (byte) 0x20;
	private static final byte DATATYPE_MASK = (byte) 0x20;
	private static final byte CLEAR_DATATYPE_MASK = ~DATATYPE_MASK;

	static Identifier create(Value v, byte[] hash, int typeIndex) {
		byte typeBits;
		byte dtBits = 0;
		if (v.isIRI()) {
			typeBits = IRI_TYPE_BITS;
		} else if (v.isLiteral()) {
			typeBits = LITERAL_TYPE_BITS;
			IRI dt = ((Literal)v).getDatatype();
			boolean isString = XSD.STRING.equals(dt) || RDF.LANGSTRING.equals(dt);
			dtBits = isString ? STRING_DATATYPE_BITS : NONSTRING_DATATYPE_BITS;
		} else if (v.isBNode()) {
			typeBits = BNODE_TYPE_BITS;
		} else if (v.isTriple()) {
			typeBits = TRIPLE_TYPE_BITS;
		} else {
			throw new AssertionError(String.format("Unexpected RDF value: %s", v.getClass()));
		}
		byte typeByte = (byte) ((hash[typeIndex] & CLEAR_TYPE_MASK) | typeBits);
		if (typeBits == LITERAL_TYPE_BITS) {
			typeByte = (byte) ((typeByte & CLEAR_DATATYPE_MASK) | dtBits);
		}
		hash[typeIndex] = typeByte;
		return new Identifier(hash, typeIndex);
	}

	private final byte[] idBytes;
	private final int typeIndex;
	private final int hashcode;

	Identifier(byte[] idBytes, int typeIndex) {
		this.idBytes = idBytes;
		this.typeIndex = typeIndex;
		int h = idBytes[0] & 0xFF;
		for (int i=1; i<Math.min(idBytes.length, 4); i++) {
			h = (h << 8) | (idBytes[i] & 0xFF);
		}
		this.hashcode = h;
	}

	public int size() {
		return idBytes.length;
	}

	public boolean isIRI() {
		return (idBytes[typeIndex] & TYPE_MASK) == IRI_TYPE_BITS;
	}

	public boolean isLiteral() {
		return (idBytes[typeIndex] & TYPE_MASK) == LITERAL_TYPE_BITS;
	}

	public boolean isBNode() {
		return (idBytes[typeIndex] & TYPE_MASK) == BNODE_TYPE_BITS;
	}

	public boolean isTriple() {
		return (idBytes[typeIndex] & TYPE_MASK) == TRIPLE_TYPE_BITS;
	}

	public boolean isString() {
		return isLiteral() && (idBytes[typeIndex] & DATATYPE_MASK) == STRING_DATATYPE_BITS;
	}

	public ByteBuffer writeTo(ByteBuffer bb) {
		return bb.put(idBytes);
	}

	ByteBuffer writeSliceTo(int offset, int len, ByteBuffer bb) {
		return bb.put(idBytes, offset, len);
	}

	byte[] rotate(int len, int shift, byte[] dest) {
		byte[] rotated = RDFRole.rotateRight(idBytes, 0, len, shift, dest);
		if (shift != 0) {
			// preserve position of type byte
			int shiftedTypeIndex = (typeIndex + shift) % len;
			byte typeByte = rotated[shiftedTypeIndex];
			byte tmp = rotated[typeIndex];
			rotated[typeIndex] = typeByte;
			rotated[shiftedTypeIndex] = tmp;
		}
		return rotated;
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
		if (this.idBytes.length != that.idBytes.length) {
			return false;
		}
		for (int i=this.idBytes.length-1; i>=0; i--) {
			if (this.idBytes[i] != that.idBytes[i]) {
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
		return Hashes.encode(idBytes);
	}
}
