package com.msd.gin.halyard.common;

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

	final int qualifierHashSize() {
		return Hashes.ID_SIZE - keyHashSize();
	}

	final int endQualifierHashSize() {
		return Hashes.ID_SIZE - endKeyHashSize();
	}

	final byte[] keyHash(StatementIndex index, byte[] id) {
		int len = keyHashSize();
		// rotate key so ordering is different for different prefixes
		// this gives better load distribution when traversing between prefixes
		return rotateRight(id, 0, len, index, new byte[len]);
	}

	final byte[] endKeyHash(StatementIndex index, byte[] id) {
		int len = endKeyHashSize();
		return rotateRight(id, 0, len, index, new byte[len]);
	}

	final byte[] qualifierHash(byte[] id) {
		return copy(id, keyHashSize(), qualifierHashSize());
	}

	final byte[] endQualifierHash(byte[] id) {
		return copy(id, endKeyHashSize(), endQualifierHashSize());
	}

	protected abstract int toShift(StatementIndex index);

	private final byte[] rotateRight(byte[] src, int offset, int len, StatementIndex index, byte[] dest) {
		int shift = toShift(index);
		if(shift > len) {
			shift = shift % len;
		}
		System.arraycopy(src, offset+len-shift, dest, 0, shift);
		System.arraycopy(src, offset, dest, shift, len-shift);
		return dest;
	}

	final byte[] rotateLeft(byte[] src, int offset, int len, StatementIndex index) {
		return rotateLeft(src, offset, len, index, new byte[len]);
	}

	final byte[] rotateLeft(byte[] src, int offset, int len, StatementIndex index, byte[] dest) {
		int shift = toShift(index);
		if(shift > len) {
			shift = shift % len;
		}
		System.arraycopy(src, offset+shift, dest, 0, len-shift);
		System.arraycopy(src, offset, dest, len-shift, shift);
		return dest;
	}

	private static byte[] copy(byte[] src, int offset, int len) {
		byte[] dest = new byte[len];
		System.arraycopy(src, offset, dest, 0, len);
		return dest;
	}
}
