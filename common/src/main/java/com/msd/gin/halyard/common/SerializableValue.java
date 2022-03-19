package com.msd.gin.halyard.common;

import java.io.Serializable;
import java.nio.ByteBuffer;

public interface SerializableValue extends Serializable {
	ByteBuffer getSerializedForm();
}
