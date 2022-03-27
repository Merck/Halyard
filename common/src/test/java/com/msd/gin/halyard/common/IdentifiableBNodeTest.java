package com.msd.gin.halyard.common;

import java.util.Objects;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.BNodeTest;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public class IdentifiableBNodeTest extends BNodeTest {
	private static final IdentifiableValueIO valueIO = IdentifiableValueIO.create();

	@Override
	protected BNode bnode(String id) {
		Objects.requireNonNull(id);
		return new IdentifiableBNode(SimpleValueFactory.getInstance().createBNode(id), valueIO);
	}

}
