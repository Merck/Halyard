package com.msd.gin.halyard.common;

import com.msd.gin.halyard.common.Hashes.HashFunction;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;

import javax.annotation.concurrent.ThreadSafe;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;

public final class ValueIdentifier implements ByteSequence, Serializable {
	private static final long serialVersionUID = 1293499350691875714L;

	enum TypeNibble {
		BIG_NIBBLE(
			(byte) 0x00 /* literal type bits */,
			(byte) 0x40 /* triple type bits */,
			(byte) 0x80 /* IRI type bits */,
			(byte) 0xC0 /* BNode type bits */,
			(byte) 0x3F /* size of each type */,

			(byte) 0x00 /* non-string datatype bits */,
			(byte) 0x20 /* string datatype bits */
		),
		LITTLE_NIBBLE(
			(byte) 0x00 /* literal type bits */,
			(byte) 0x04 /* triple type bits */,
			(byte) 0x08 /* IRI type bits */,
			(byte) 0x0C /* BNode type bits */,
			(byte) 0x03 /* size of each type */,

			(byte) 0x00 /* non-string datatype bits */,
			(byte) 0x02 /* string datatype bits */
		);

		final byte literalTypeBits;
		final byte tripleTypeBits;
		final byte iriTypeBits;
		final byte bnodeTypeBits;
		final byte typeSize;
		final byte typeMask;
		final byte clearTypeMask;
		final byte nonstringDatatypeBits;
		final byte stringDatatypeBits;
		final byte datatypeMask;
		final byte clearDatatypeMask;

		private TypeNibble(byte literalTypeBits, byte tripleTypeBits, byte iriTypeBits, byte bnodeTypeBits, byte typeSize, byte nonstringDatatypeBits, byte stringDatatypeBits) {
			this.literalTypeBits = literalTypeBits;
			this.tripleTypeBits = tripleTypeBits;
			this.iriTypeBits = iriTypeBits;
			this.bnodeTypeBits = bnodeTypeBits;
			this.typeSize = typeSize;
			this.typeMask = bnodeTypeBits;
			this.clearTypeMask = (byte) ~typeMask;
			this.nonstringDatatypeBits = nonstringDatatypeBits;
			this.stringDatatypeBits = stringDatatypeBits;
			this.datatypeMask = stringDatatypeBits;
			this.clearDatatypeMask = (byte) ~datatypeMask;
		}
	}

	@ThreadSafe
	static final class Format implements Serializable {
		private static final long serialVersionUID = -7777885367792871664L;

		final String algorithm;
		final int size;
		final int typeIndex;
		final TypeNibble typeNibble;
		transient ThreadLocal<HashFunction> hashFuncProvider;

		/**
		 * Identifier format.
		 * @param algorithm algorithm used to generate the ID.
		 * @param size byte size of the ID
		 * @param typeIndex byte index to store type information
		 * @param typeNibble byte nibble to store type information
		 */
		Format(String algorithm, int size, int typeIndex, TypeNibble typeNibble) {
			this.size = size;
			this.algorithm = algorithm;
			this.typeIndex = typeIndex;
			this.typeNibble = typeNibble;
			initHashProvider();
		}

		private void initHashProvider() {
			hashFuncProvider = new ThreadLocal<HashFunction>() {
				@Override
				protected HashFunction initialValue() {
					return Hashes.getHash(algorithm, size);
				}
			};
		}

		int getSaltSize() {
			int typeSaltSize;
			switch (typeNibble) {
				case BIG_NIBBLE:
					typeSaltSize = 1 << (8*typeIndex);
					break;
				case LITTLE_NIBBLE:
					typeSaltSize = 1 << (4*(typeIndex+1));
					break;
				default:
					throw new AssertionError();
			}
			return typeSaltSize;
		}

		/**
		 * Thread-safe.
		 */
		ValueIdentifier id(ValueType type, IRI datatype, ByteBuffer ser) {
			byte[] hash = hashFuncProvider.get().apply(ser);
			writeType(type, datatype, hash, 0);
			return new ValueIdentifier(hash, this);
		}

		@Override
		public boolean equals(Object other) {
			if (this == other) {
				return true;
			}
			if (!(other instanceof Format)) {
				return false;
			}
			Format that = (Format) other;
			return algorithm.equals(that.algorithm)
				&& size == that.size
				&& typeIndex == that.typeIndex
				&& typeNibble == that.typeNibble;
		}

		@Override
		public int hashCode() {
			return Objects.hash(algorithm, size, typeIndex, typeNibble);
		}

		private int getTypeBits(byte[] idBytes) {
			return (idBytes[typeIndex] & typeNibble.typeMask);
		}

		private int getDatatypeBits(byte[] idBytes) {
			return (idBytes[typeIndex] & typeNibble.datatypeMask);
		}

		byte[] unrotate(byte[] src, int offset, int len, int shift, byte[] dest) {
			byte[] rotated = rotateLeft(src, offset, len, shift, dest);
			if (shift != 0) {
				// preserve position of type byte
				int shiftedTypeIndex = (typeIndex + len - shift) % len;
				byte typeByte = rotated[shiftedTypeIndex];
				byte tmp = rotated[typeIndex];
				rotated[typeIndex] = typeByte;
				rotated[shiftedTypeIndex] = tmp;
			}
			return rotated;
		}

		byte[] writeType(ValueType type, IRI datatype, byte[] arr, int offset) {
			int typeBits;
			int dtBits = 0;
			switch (type) {
				case LITERAL:
					typeBits = typeNibble.literalTypeBits;
					dtBits = isString(datatype) ? typeNibble.stringDatatypeBits : typeNibble.nonstringDatatypeBits;
					break;
				case TRIPLE:
					typeBits = typeNibble.tripleTypeBits;
					break;
				case IRI:
					typeBits = typeNibble.iriTypeBits;
					break;
				case BNODE:
					typeBits = typeNibble.bnodeTypeBits;
					break;
				default:
					throw new AssertionError();
			}
			byte typeByte = (byte) ((arr[offset+typeIndex] & typeNibble.clearTypeMask) | typeBits);
			if (datatype != null) {
				typeByte = (byte) ((typeByte & typeNibble.clearDatatypeMask) | dtBits);
			}
			arr[offset+typeIndex] = typeByte;
			return arr;
		}

		private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
			in.defaultReadObject();
			initHashProvider();
		}

		static byte[] rotateLeft(byte[] src, int offset, int len, int shift, byte[] dest) {
			if(shift > len) {
				shift = shift % len;
			}
			if (shift != 0) {
				System.arraycopy(src, offset+shift, dest, 0, len-shift);
				System.arraycopy(src, offset, dest, len-shift, shift);
			} else {
				System.arraycopy(src, offset, dest, 0, len);
			}
			return dest;
		}

		static byte[] rotateRight(byte[] src, int offset, int len, int shift, byte[] dest) {
			if(shift > len) {
				shift = shift % len;
			}
			if (shift != 0) {
				System.arraycopy(src, offset+len-shift, dest, 0, shift);
				System.arraycopy(src, offset, dest, shift, len-shift);
			} else {
				System.arraycopy(src, offset, dest, 0, len);
			}
			return dest;
		}
	}

	static boolean isString(IRI dt) {
		return XSD.STRING.equals(dt) || RDF.LANGSTRING.equals(dt);
	}

	private final byte[] idBytes;
	private final Format format;

	ValueIdentifier(byte[] idBytes, Format format) {
		this.idBytes = idBytes;
		this.format = format;
	}

	public Format getFormat() {
		return format;
	}

	@Override
	public int size() {
		return idBytes.length;
	}

	public boolean isIRI() {
		return format.getTypeBits(idBytes) == format.typeNibble.iriTypeBits;
	}

	public boolean isLiteral() {
		return format.getTypeBits(idBytes) == format.typeNibble.literalTypeBits;
	}

	public boolean isBNode() {
		return format.getTypeBits(idBytes) == format.typeNibble.bnodeTypeBits;
	}

	public boolean isTriple() {
		return format.getTypeBits(idBytes) == format.typeNibble.tripleTypeBits;
	}

	public boolean isString() {
		return isLiteral() && (format.getDatatypeBits(idBytes) == format.typeNibble.stringDatatypeBits);
	}

	@Override
	public ByteBuffer writeTo(ByteBuffer bb) {
		return bb.put(idBytes);
	}

	ByteBuffer writeSliceTo(int offset, int len, ByteBuffer bb) {
		return bb.put(idBytes, offset, len);
	}

	byte[] rotate(int len, int shift, byte[] dest) {
		byte[] rotated = Format.rotateRight(idBytes, 0, len, shift, dest);
		if (shift != 0) {
			// preserve position of type byte
			int shiftedTypeIndex = (format.typeIndex + shift) % len;
			byte typeByte = rotated[shiftedTypeIndex];
			byte tmp = rotated[format.typeIndex];
			rotated[format.typeIndex] = typeByte;
			rotated[shiftedTypeIndex] = tmp;
		}
		return rotated;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof ValueIdentifier)) {
			return false;
		}
		ValueIdentifier that = (ValueIdentifier) o;
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
		int h = idBytes[0] & 0xFF;
		for (int i=1; i<Math.min(idBytes.length, 4); i++) {
			h = (h << 8) | (idBytes[i] & 0xFF);
		}
		return h;
	}

	@Override
	public String toString() {
		return Hashes.encode(idBytes);
	}
}
