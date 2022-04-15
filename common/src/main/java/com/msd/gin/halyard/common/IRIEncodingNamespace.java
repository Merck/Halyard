package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;

public interface IRIEncodingNamespace {
	String getName();
	ByteBuffer writeBytes(String localName, ByteBuffer b);
	String readBytes(ByteBuffer b);
}
