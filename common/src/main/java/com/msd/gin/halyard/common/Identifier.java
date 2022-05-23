package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;

public final class Identifier {
	enum TypeNibble {
		BIG_NIBBLE(
			(byte) 0x00 /* literal type bits */,
			(byte) 0x40 /* triple type bits */,
			(byte) 0x80 /* IRI type bits */,
			(byte) 0xC0 /* BNode type bits */,

			(byte) 0x00 /* non-string datatype bits */,
			(byte) 0x20 /* string datatype bits */
		),
		LITTLE_NIBBLE(
			(byte) 0x00 /* literal type bits */,
			(byte) 0x04 /* triple type bits */,
			(byte) 0x08 /* IRI type bits */,
			(byte) 0x0C /* BNode type bits */,

			(byte) 0x00 /* non-string datatype bits */,
			(byte) 0x02 /* string datatype bits */
		);
		final byte literalTypeBits;
		final byte tripleTypeBits;
		final byte iriTypeBits;
		final byte bnodeTypeBits;
		final byte typeMask;
		final byte clearTypeMask;
		final byte nonstringDatatypeBits;
		final byte stringDatatypeBits;
		final byte datatypeMask;
		final byte clearDatatypeMask;
		final byte literalStopBits;
		private TypeNibble(byte literalTypeBits, byte tripleTypeBits, byte iriTypeBits, byte bnodeTypeBits, byte nonstringDatatypeBits, byte stringDatatypeBits) {
			this.literalTypeBits = literalTypeBits;
			this.tripleTypeBits = tripleTypeBits;
			this.iriTypeBits = iriTypeBits;
			this.bnodeTypeBits = bnodeTypeBits;
			this.typeMask = bnodeTypeBits;
			this.clearTypeMask = (byte) ~typeMask;
			this.nonstringDatatypeBits = nonstringDatatypeBits;
			this.stringDatatypeBits = stringDatatypeBits;
			this.datatypeMask = stringDatatypeBits;
			this.clearDatatypeMask = (byte) ~datatypeMask;
			this.literalStopBits = tripleTypeBits;
		}
	}

	static Identifier create(Value v, byte[] hash, int typeIndex, TypeNibble typeNibble) {
		byte typeBits;
		byte dtBits = 0;
		if (v.isIRI()) {
			typeBits = typeNibble.iriTypeBits;
		} else if (v.isLiteral()) {
			typeBits = typeNibble.literalTypeBits;
			IRI dt = ((Literal)v).getDatatype();
			boolean isString = XSD.STRING.equals(dt) || RDF.LANGSTRING.equals(dt);
			dtBits = isString ? typeNibble.stringDatatypeBits : typeNibble.nonstringDatatypeBits;
		} else if (v.isBNode()) {
			typeBits = typeNibble.bnodeTypeBits;
		} else if (v.isTriple()) {
			typeBits = typeNibble.tripleTypeBits;
		} else {
			throw new AssertionError(String.format("Unexpected RDF value: %s", v.getClass()));
		}
		byte typeByte = (byte) ((hash[typeIndex] & typeNibble.clearTypeMask) | typeBits);
		if (typeBits == typeNibble.literalTypeBits) {
			typeByte = (byte) ((typeByte & typeNibble.clearDatatypeMask) | dtBits);
		}
		hash[typeIndex] = typeByte;
		return new Identifier(hash, typeIndex, typeNibble);
	}

	private final byte[] idBytes;
	private final int typeIndex;
	private final TypeNibble typeNibble;
	private final int hashcode;

	Identifier(byte[] idBytes, int typeIndex, TypeNibble typeNibble) {
		this.idBytes = idBytes;
		this.typeIndex = typeIndex;
		int h = idBytes[0] & 0xFF;
		for (int i=1; i<Math.min(idBytes.length, 4); i++) {
			h = (h << 8) | (idBytes[i] & 0xFF);
		}
		this.typeNibble = typeNibble;
		this.hashcode = h;
	}

	public int size() {
		return idBytes.length;
	}

	public boolean isIRI() {
		return (idBytes[typeIndex] & typeNibble.typeMask) == typeNibble.iriTypeBits;
	}

	public boolean isLiteral() {
		return (idBytes[typeIndex] & typeNibble.typeMask) == typeNibble.literalTypeBits;
	}

	public boolean isBNode() {
		return (idBytes[typeIndex] & typeNibble.typeMask) == typeNibble.bnodeTypeBits;
	}

	public boolean isTriple() {
		return (idBytes[typeIndex] & typeNibble.typeMask) == typeNibble.tripleTypeBits;
	}

	public boolean isString() {
		return isLiteral() && (idBytes[typeIndex] & typeNibble.datatypeMask) == typeNibble.stringDatatypeBits;
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
