package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;

public final class RDFRole<T extends SPOC<?>> {
	enum Name {SUBJECT, PREDICATE, OBJECT, CONTEXT}
	private final Name name;
	private final int idSize;
	private final int keyHashSize;
	private final int endKeyHashSize;
	private final ByteFiller startKey;
	private final ByteFiller stopKey;
	private final ByteFiller endStartKey;
	private final ByteFiller endStopKey;
	private final int sshift;
	private final int pshift;
	private final int oshift;
	private final int typeIndex;
	private final int sizeLength;

	public RDFRole(Name name, int idSize, int keyHashSize, int endKeyHashSize, int sshift, int pshift, int oshift, int typeIndex, int sizeLength) {
		this.name = name;
		this.idSize = idSize;
		this.keyHashSize = keyHashSize;
		this.endKeyHashSize = endKeyHashSize;
		this.startKey = new ByteFiller((byte)0x00, keyHashSize);
		this.stopKey = new ByteFiller((byte)0xFF, keyHashSize);
		this.endStartKey = new ByteFiller((byte)0x00, endKeyHashSize);
		this.endStopKey = new ByteFiller((byte)0xFF, endKeyHashSize);
		this.sshift = sshift;
		this.pshift = pshift;
		this.oshift = oshift;
		this.typeIndex = typeIndex;
		this.sizeLength = sizeLength;
	}

	Name getName() {
		return name;
	}

	/**
	 * Key hash size in bytes.
	 * @return size in bytes.
	 */
	public int keyHashSize() {
		return keyHashSize;
	}

	public int endKeyHashSize() {
		return endKeyHashSize;
	}

	int qualifierHashSize() {
		return idSize - keyHashSize;
	}

	int endQualifierHashSize() {
		return idSize - endKeyHashSize;
	}

	int sizeLength() {
		return sizeLength;
	}

	byte[] keyHash(StatementIndex<?,?,?,?> index, Identifier id) {
		int len = keyHashSize();
		// rotate key so ordering is different for different prefixes
		// this gives better load distribution when traversing between prefixes
		return id.rotate(len, toShift(index), new byte[len]);
	}

	byte[] endKeyHash(StatementIndex<?,?,?,?> index, Identifier id) {
		int len = endKeyHashSize();
		return len > 0 ? id.rotate(len, toShift(index), new byte[len]) : new byte[0];
	}

	byte[] qualifierHash(Identifier id) {
		byte[] b = new byte[qualifierHashSize()];
		writeQualifierHashTo(id, ByteBuffer.wrap(b));
		return b;
	}

	ByteBuffer writeQualifierHashTo(Identifier id, ByteBuffer bb) {
		return id.writeSliceTo(keyHashSize(), qualifierHashSize(), bb);
	}

	ByteBuffer writeEndQualifierHashTo(Identifier id, ByteBuffer bb) {
		return id.writeSliceTo(endKeyHashSize(), endQualifierHashSize(), bb);
	}

	ByteFiller startKey() {
		return startKey;
	}

	ByteFiller stopKey() {
		return stopKey;
	}

	ByteFiller endStartKey() {
		return endStartKey;
	}

	ByteFiller endStopKey() {
		return endStopKey;
	}

	private int toShift(StatementIndex<?,?,?,?> index) {
		switch(index.getName()) {
			case SPO:
			case CSPO:
				return sshift;
			case POS:
			case CPOS:
				return pshift;
			case OSP:
			case COSP:
				return oshift;
			default:
				throw new AssertionError();
		}
	}

	byte[] unrotate(byte[] src, int offset, int len, StatementIndex<?,?,?,?> index, byte[] dest) {
		int shift = toShift(index);
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

	@Override
	public String toString() {
		return name.toString();
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
