package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.eclipse.rdf4j.model.Value;

public final class Identifier {
	public static final int ID_SIZE = 20;
	static final byte NON_LITERAL_FLAG = (byte) 0x80;

	private final byte[] value;
	private final int hashcode;

	public static Identifier id(Value v) {
		Identifier id;
		Identifiable idValue;
	
		if (v instanceof Identifiable) {
			idValue = (Identifiable) v;
			id = idValue.getId();
		} else {
			idValue = null;
			id = ValueIO.WELL_KNOWN_IRI_IDS.inverse().get(v);
		}
	
		boolean alreadyHasHash = (id != null);
		if (!alreadyHasHash) {
			byte[] hash = Hashes.hashUnique(v.toString().getBytes(StandardCharsets.UTF_8));
			// literal prefix
			if (v.isLiteral()) {
				hash[0] &= 0x7F; // 0 msb
			} else {
				hash[0] |= NON_LITERAL_FLAG; // 1 msb
			}
			id = new Identifier(hash);
		}
	
		if (idValue != null && !alreadyHasHash) {
			idValue.setId(id);
		}
		return id;
	}

	public Identifier(byte[] value) {
		if (value.length != ID_SIZE) {
			throw new IllegalArgumentException("Byte array has incorrect length");
		}
		this.value = value;
		this.hashcode = ((value[0] & 0xFF) << 24) | ((value[1] & 0xFF) << 16) | ((value[2] & 0xFF) << 8) | (value[3] & 0xFF);
	}

	public boolean isLiteral() {
		return (value[0] & NON_LITERAL_FLAG) == 0;
	}

	public ByteBuffer writeTo(ByteBuffer bb) {
		return bb.put(value);
	}

	ByteBuffer writeSliceTo(int offset, int len, ByteBuffer bb) {
		return bb.put(value, offset, len);
	}

	final byte[] rotate(int offset, int len, int shift, byte[] dest) {
		return RDFRole.rotateRight(value, offset, len, shift, dest);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof Identifier)) {
			return false;
		}
		Identifier that = (Identifier) o;
		for (int i=ID_SIZE-1; i>=0; i--) {
			if (this.value[i] != that.value[i]) {
				return false;
			}
		}
		return true;
	}

	@Override
	public int hashCode() {
		return hashcode;
	}

	@Override
	public String toString() {
		return Hashes.encode(value);
	}
}
