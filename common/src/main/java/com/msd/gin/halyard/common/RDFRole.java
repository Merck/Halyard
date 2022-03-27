package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;

public enum RDFRole {
	SUBJECT() {
		@Override
		int keyHashSize() {
			return RDFSubject.KEY_SIZE;
		}

		@Override
		int endKeyHashSize() {
			return RDFSubject.END_KEY_SIZE;
		}

		@Override
		protected int toShift(StatementIndex index) {
			switch(index) {
				case SPO:
				case CSPO:
					return 0;
				case POS:
				case CPOS:
					return 2;
				case OSP:
				case COSP:
					return 1;
				default:
					throw new AssertionError();
			}
		}
	},
	PREDICATE() {
		@Override
		int keyHashSize() {
			return RDFPredicate.KEY_SIZE;
		}

		@Override
		int endKeyHashSize() {
			return RDFPredicate.END_KEY_SIZE;
		}

		@Override
		protected int toShift(StatementIndex index) {
			switch(index) {
				case SPO:
				case CSPO:
					return 1;
				case POS:
				case CPOS:
					return 0;
				case OSP:
				case COSP:
					return 2;
				default:
					throw new AssertionError();
			}
		}
	},
	OBJECT() {
		@Override
		int keyHashSize() {
			return RDFObject.KEY_SIZE;
		}

		@Override
		int endKeyHashSize() {
			return RDFObject.END_KEY_SIZE;
		}

		@Override
		protected int toShift(StatementIndex index) {
			switch(index) {
				case SPO:
				case CSPO:
					return 2;
				case POS:
				case CPOS:
					return 1;
				case OSP:
				case COSP:
					// NB: preserve non-literal flag for scanning
					return 0;
				default:
					throw new AssertionError();
			}
		}
	},
	CONTEXT() {
		@Override
		int keyHashSize() {
			return RDFContext.KEY_SIZE;
		}

		@Override
		int endKeyHashSize() {
			throw new AssertionError("Context is never at end");
		}

		@Override
		protected int toShift(StatementIndex index) {
			return 0;
		}
	};

	/**
	 * Key hash size in bytes
	 */
	abstract int keyHashSize();
	abstract int endKeyHashSize();

	final int qualifierHashSize(int idSize) {
		return idSize - keyHashSize();
	}

	final int endQualifierHashSize(int idSize) {
		return idSize - endKeyHashSize();
	}

	final byte[] keyHash(StatementIndex index, Identifier id) {
		int len = keyHashSize();
		// rotate key so ordering is different for different prefixes
		// this gives better load distribution when traversing between prefixes
		return rotate(id, 0, len, index, new byte[len]);
	}

	final byte[] endKeyHash(StatementIndex index, Identifier id) {
		int len = endKeyHashSize();
		return rotate(id, 0, len, index, new byte[len]);
	}

	byte[] qualifierHash(Identifier id) {
		byte[] b = new byte[qualifierHashSize(id.size())];
		writeQualifierHashTo(id, ByteBuffer.wrap(b));
		return b;
	}

	final ByteBuffer writeQualifierHashTo(Identifier id, ByteBuffer bb) {
		return id.writeSliceTo(keyHashSize(), qualifierHashSize(id.size()), bb);
	}

	final ByteBuffer writeEndQualifierHashTo(Identifier id, ByteBuffer bb) {
		return id.writeSliceTo(endKeyHashSize(), endQualifierHashSize(id.size()), bb);
	}

	protected abstract int toShift(StatementIndex index);

	private final byte[] rotate(Identifier id, int offset, int len, StatementIndex index, byte[] dest) {
		return id.rotate(offset, len, toShift(index), dest);
	}

	final byte[] unrotate(byte[] src, StatementIndex index) {
		return unrotate(src, 0, src.length, index, new byte[src.length]);
	}

	final byte[] unrotate(byte[] src, int offset, int len, StatementIndex index, byte[] dest) {
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
