package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;

public final class ByteArray implements ByteSequence {
	private final byte[] arr;

	public ByteArray(byte[] arr) {
		this.arr = arr;
	}

	@Override
	public int size() {
		return arr.length;
	}

	@Override
	public ByteBuffer writeTo(ByteBuffer bb) {
		return bb.put(arr);
	}
}
