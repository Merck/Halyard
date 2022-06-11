package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;

public interface SerializableValue {
	ByteBuffer getSerializedForm(RDFFactory rdfFactory);
}
