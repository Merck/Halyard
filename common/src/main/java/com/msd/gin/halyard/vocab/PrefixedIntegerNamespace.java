package com.msd.gin.halyard.vocab;

import java.nio.ByteBuffer;

public class PrefixedIntegerNamespace extends IntegerNamespace {
	private static final long serialVersionUID = -5123838426219115850L;

	private final String idPrefix;

	public PrefixedIntegerNamespace(String prefix, String ns, String idPrefix) {
		super(prefix, ns);
		this.idPrefix = idPrefix;
	}

	@Override
	public ByteBuffer writeBytes(String localName, ByteBuffer b) {
		return super.writeBytes(localName.substring(idPrefix.length()), b);
	}

	@Override
	public String readBytes(ByteBuffer b) {
		return idPrefix + super.readBytes(b);
	}
}
