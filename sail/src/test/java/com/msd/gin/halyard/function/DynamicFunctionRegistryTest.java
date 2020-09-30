package com.msd.gin.halyard.function;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.Test;

import static org.junit.Assert.*;

public class DynamicFunctionRegistryTest {
	private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

	@Test
	public void testNoSuchFunction() {
		boolean result = new DynamicFunctionRegistry().has("foo#bar");
		assertFalse(result);
	}

	@Test
	public void testXPathDaysFromDurationFunction() {
		Value result = new DynamicFunctionRegistry().get("http://www.w3.org/2005/xpath-functions#days-from-duration").get().evaluate(SVF,
				SVF.createLiteral("P3DT10H", XSD.DAYTIMEDURATION));
		assertEquals(3, ((Literal) result).intValue());
	}

	@Test
	public void testXPathSineFunction() {
		Value result = new DynamicFunctionRegistry().get("http://www.w3.org/2005/xpath-functions/math#sin").get().evaluate(SVF, SVF.createLiteral(Math.PI / 2.0));
		assertEquals(1.0, ((Literal) result).doubleValue(), 0.001);
	}
}
