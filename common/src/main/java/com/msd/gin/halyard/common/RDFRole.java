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
		protected int toShift(byte prefix) {
			switch(prefix) {
				case HalyardTableUtils.SPO_PREFIX:
				case HalyardTableUtils.CSPO_PREFIX:
					return 0;
				case HalyardTableUtils.POS_PREFIX:
				case HalyardTableUtils.CPOS_PREFIX:
					return 2;
				case HalyardTableUtils.OSP_PREFIX:
				case HalyardTableUtils.COSP_PREFIX:
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
		protected int toShift(byte prefix) {
			switch(prefix) {
				case HalyardTableUtils.SPO_PREFIX:
				case HalyardTableUtils.CSPO_PREFIX:
					return 1;
				case HalyardTableUtils.POS_PREFIX:
				case HalyardTableUtils.CPOS_PREFIX:
					return 0;
				case HalyardTableUtils.OSP_PREFIX:
				case HalyardTableUtils.COSP_PREFIX:
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
		protected int toShift(byte prefix) {
			switch(prefix) {
				case HalyardTableUtils.SPO_PREFIX:
				case HalyardTableUtils.CSPO_PREFIX:
					return 2;
				case HalyardTableUtils.POS_PREFIX:
				case HalyardTableUtils.CPOS_PREFIX:
					return 1;
				case HalyardTableUtils.OSP_PREFIX:
				case HalyardTableUtils.COSP_PREFIX:
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
		protected int toShift(byte prefix) {
			return 0;
		}
	};

	/**
	 * Key hash size in bytes
	 */
	abstract int keyHashSize();
	abstract int endKeyHashSize();

	final int qualifierHashSize() {
		return HalyardTableUtils.ID_SIZE - keyHashSize();
	}

	final int endQualifierHashSize() {
		return HalyardTableUtils.ID_SIZE - endKeyHashSize();
	}

	protected abstract int toShift(byte prefix);

	final byte[] rotateRight(byte[] src, int offset, int len, byte prefix) {
		return rotateRight(src, offset, len, prefix, new byte[len]);
	}

	final byte[] rotateRight(byte[] src, int offset, int len, byte prefix, byte[] dest) {
		int shift = toShift(prefix);
		if(shift > len) {
			shift = shift % len;
		}
		System.arraycopy(src, offset+len-shift, dest, 0, shift);
		System.arraycopy(src, offset, dest, shift, len-shift);
		return dest;
	}

	final byte[] rotateLeft(byte[] src, int offset, int len, byte prefix) {
		return rotateLeft(src, offset, len, prefix, new byte[len]);
	}

	final byte[] rotateLeft(byte[] src, int offset, int len, byte prefix, byte[] dest) {
		int shift = toShift(prefix);
		if(shift > len) {
			shift = shift % len;
		}
		System.arraycopy(src, offset+shift, dest, 0, len-shift);
		System.arraycopy(src, offset, dest, len-shift, shift);
		return dest;
	}
}
