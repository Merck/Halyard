package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.ValueFactoryTest;

public class IdValueFactoryTest extends ValueFactoryTest {

	@Override
	protected ValueFactory factory() {
		return IdValueFactory.INSTANCE;
	}

}
