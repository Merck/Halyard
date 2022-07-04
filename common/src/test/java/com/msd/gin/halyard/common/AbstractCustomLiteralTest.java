package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public abstract class AbstractCustomLiteralTest {
	protected abstract Literal createLiteral() throws Exception;
	protected abstract Literal createOtherLiteral() throws Exception;

	@Test
	public void testEqualsHashCode() throws Exception {
		Literal actual = createLiteral();
		Literal expected = SimpleValueFactory.getInstance().createLiteral(actual.getLabel(), actual.getDatatype());
		assertEquals(expected.hashCode(), actual.hashCode());
		assertTrue(expected.equals(actual));
	}

	@Test
	public void testNotEqual() throws Exception {
		Literal actual = createLiteral();
		Literal unexpected = createOtherLiteral();
		assertNotEquals(unexpected, actual);
	}

	@Test
	public void testNotEqual_empty() throws Exception {
		Literal actual = createLiteral();
		Literal unexpected = SimpleValueFactory.getInstance().createLiteral("", actual.getDatatype());
		assertNotEquals(unexpected, actual);
	}

	@Test
	public void testStringValue() throws Exception {
		Literal actual = createLiteral();
		Literal expected = SimpleValueFactory.getInstance().createLiteral(actual.getLabel(), actual.getDatatype());
		assertEquals(expected.stringValue(), actual.stringValue());
	}

	@Test
	public void testToString() throws Exception {
		Literal actual = createLiteral();
		Literal expected = SimpleValueFactory.getInstance().createLiteral(actual.getLabel(), actual.getDatatype());
		assertEquals(expected.toString(), actual.toString());
	}
}
