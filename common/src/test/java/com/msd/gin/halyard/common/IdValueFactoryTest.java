package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.ValueFactoryTest;

public class IdValueFactoryTest extends ValueFactoryTest {
	private static final IdentifiableValueIO valueIO = IdentifiableValueIO.create();

	@Override
	protected ValueFactory factory() {
		return new IdValueFactory(valueIO);
	}

}
