package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;
import java.util.Arrays;

public final class ByteFiller implements ByteSequence {
	private final byte value;
	private final int size;

	public ByteFiller(byte value, int size) {
		this.value = value;
		this.size = size;
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public ByteBuffer writeTo(ByteBuffer bb) {
		int pos = bb.position();
		int startIndex = bb.arrayOffset() + pos;
		Arrays.fill(bb.array(), startIndex, startIndex + size, value);
		bb.position(pos+size);
		return bb;
	}
}
