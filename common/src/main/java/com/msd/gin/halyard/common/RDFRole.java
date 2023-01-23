package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
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
	private final int sizeLength;

	public RDFRole(Name name, int idSize, int keyHashSize, int endKeyHashSize, int sshift, int pshift, int oshift, int sizeLength) {
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

	byte[] qualifierHash(ValueIdentifier id) {
		byte[] b = new byte[qualifierHashSize()];
		writeQualifierHashTo(id, ByteBuffer.wrap(b));
		return b;
	}

	ByteBuffer writeQualifierHashTo(ValueIdentifier id, ByteBuffer bb) {
		return id.writeSliceTo(keyHashSize(), qualifierHashSize(), bb);
	}

	ByteBuffer writeEndQualifierHashTo(ValueIdentifier id, ByteBuffer bb) {
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

	int toShift(StatementIndex<?,?,?,?> index) {
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

	@Override
	public String toString() {
		return name.toString();
	}
}
