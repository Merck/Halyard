package com.msd.gin.halyard.vocab;

import com.msd.gin.halyard.common.AbstractIRIEncodingNamespace;
import com.msd.gin.halyard.common.Hashes;
import com.msd.gin.halyard.common.ValueIO;

import java.nio.ByteBuffer;

public class Base64Namespace extends AbstractIRIEncodingNamespace {
	private static final long serialVersionUID = 2008223953375980683L;

	Base64Namespace(String prefix, String ns) {
		super(prefix, ns);
	}

	@Override
	public ByteBuffer writeBytes(String localName, ByteBuffer b) {
		byte[] b64 = Hashes.decode(localName);
		b = ValueIO.ensureCapacity(b, b64.length);
		b.put(b64);
		return b;
	}

	@Override
	public String readBytes(ByteBuffer b) {
		return Hashes.encode(b).toString();
	}
}
