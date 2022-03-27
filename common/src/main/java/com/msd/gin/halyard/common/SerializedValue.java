package com.msd.gin.halyard.common;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.nio.ByteBuffer;

final class SerializedValue implements Externalizable {
	private static final long serialVersionUID = -5353524716487912852L;
	private static volatile boolean isReaderSet = false;
	private static ValueIO.Reader serReader;

	private byte[] ser;

	public SerializedValue() {}

	public SerializedValue(byte[] ser, ValueIO.Reader reader) {
		this.ser = ser;
		if (!isReaderSet) {
			synchronized(SerializedValue.class) {
				if (!isReaderSet) {
					serReader = reader;
					isReaderSet = true;
				}
			}
		}
	}

	private Object readResolve() throws ObjectStreamException {
		return serReader.readValue(ByteBuffer.wrap(ser));
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(ser.length);
		out.write(ser);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		int size = in.readInt();
		ser = new byte[size];
		in.read(ser);
	}
}
