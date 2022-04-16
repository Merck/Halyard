package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;

public final class RDFRole {
	private final int keyHashSize;
	private final int endKeyHashSize;
	private final int sshift;
	private final int pshift;
	private final int oshift;

	public RDFRole(int keyHashSize, int endKeyHashSize, int sshift, int pshift, int oshift) {
		this.keyHashSize = keyHashSize;
		this.endKeyHashSize = endKeyHashSize;
		this.sshift = sshift;
		this.pshift = pshift;
		this.oshift = oshift;
	}

	/**
	 * Key hash size in bytes
	 */
	public int keyHashSize() {
		return keyHashSize;
	}

	public int endKeyHashSize() {
		if (endKeyHashSize == -1) {
			throw new AssertionError("This role should never appear at the end of a key");
		}
		return endKeyHashSize;
	}

	int qualifierHashSize(int idSize) {
		return idSize - keyHashSize();
	}

	int endQualifierHashSize(int idSize) {
		return idSize - endKeyHashSize();
	}

	byte[] keyHash(StatementIndex index, Identifier id) {
		int len = keyHashSize();
		// rotate key so ordering is different for different prefixes
		// this gives better load distribution when traversing between prefixes
		return rotate(id, 0, len, index, new byte[len]);
	}

	byte[] endKeyHash(StatementIndex index, Identifier id) {
		int len = endKeyHashSize();
		return rotate(id, 0, len, index, new byte[len]);
	}

	byte[] qualifierHash(Identifier id) {
		byte[] b = new byte[qualifierHashSize(id.size())];
		writeQualifierHashTo(id, ByteBuffer.wrap(b));
		return b;
	}

	ByteBuffer writeQualifierHashTo(Identifier id, ByteBuffer bb) {
		return id.writeSliceTo(keyHashSize(), qualifierHashSize(id.size()), bb);
	}

	ByteBuffer writeEndQualifierHashTo(Identifier id, ByteBuffer bb) {
		return id.writeSliceTo(endKeyHashSize(), endQualifierHashSize(id.size()), bb);
	}

	private int toShift(StatementIndex index) {
		switch(index) {
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

	private byte[] rotate(Identifier id, int offset, int len, StatementIndex index, byte[] dest) {
		return id.rotate(offset, len, toShift(index), dest);
	}

	byte[] unrotate(byte[] src, StatementIndex index) {
		return unrotate(src, 0, src.length, index, new byte[src.length]);
	}

	byte[] unrotate(byte[] src, int offset, int len, StatementIndex index, byte[] dest) {
		return rotateLeft(src, offset, len, toShift(index), dest);
	}

	static byte[] rotateLeft(byte[] src, int offset, int len, int shift, byte[] dest) {
		if(shift > len) {
			shift = shift % len;
		}
		System.arraycopy(src, offset+shift, dest, 0, len-shift);
		System.arraycopy(src, offset, dest, len-shift, shift);
		return dest;
	}

	static byte[] rotateRight(byte[] src, int offset, int len, int shift, byte[] dest) {
		if(shift > len) {
			shift = shift % len;
		}
		System.arraycopy(src, offset+len-shift, dest, 0, shift);
		System.arraycopy(src, offset, dest, shift, len-shift);
		return dest;
	}
}
