package com.msd.gin.halyard.vocab;

import com.msd.gin.halyard.common.AbstractIRIEncodingNamespace;
import com.msd.gin.halyard.common.ValueIO;

import java.nio.ByteBuffer;

public class IntegerNamespace extends AbstractIRIEncodingNamespace {
	private static final long serialVersionUID = 571723932297421854L;

	IntegerNamespace(String prefix, String ns) {
		super(prefix, ns);
	}

	@Override
	public ByteBuffer writeBytes(String localName, ByteBuffer b) {
		return ValueIO.writeCompressedInteger(localName, b);
	}

	@Override
	public String readBytes(ByteBuffer b) {
		return ValueIO.readCompressedInteger(b);
	}
}
