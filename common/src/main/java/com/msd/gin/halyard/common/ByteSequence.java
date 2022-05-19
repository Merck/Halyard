package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;

public interface ByteSequence {
	ByteBuffer writeTo(ByteBuffer bb);
	int size();

	static final ByteSequence EMPTY = new ByteSequence() {
		@Override
		public ByteBuffer writeTo(ByteBuffer bb) {
			return bb;
		}

		@Override
		public int size() {
			return 0;
		}
	};
}
