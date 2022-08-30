package com.msd.gin.halyard.sail.search;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class EscapeTermFunctionTest {
	private final ValueFactory vf = SimpleValueFactory.getInstance();

	@Test
	public void testNonReserved() {
		Literal escaped = (Literal) new EscapeTermFunction().evaluate(vf, vf.createLiteral("foobar"));
		assertEquals("(foobar)", escaped.stringValue());
	}

	@Test
	public void testRemoveReserved() {
		Literal escaped = (Literal) new EscapeTermFunction().evaluate(vf, vf.createLiteral("f<oo>bar"));
		assertEquals("(foobar)", escaped.stringValue());
	}

	@Test
	public void testEscapeReserved1() {
		Literal escaped = (Literal) new EscapeTermFunction().evaluate(vf, vf.createLiteral("f+oo!b/ar"));
		assertEquals("(f\\+oo\\!b\\/ar)", escaped.stringValue());
	}

	@Test
	public void testEscapeReserved2() {
		Literal escaped = (Literal) new EscapeTermFunction().evaluate(vf, vf.createLiteral("f&&oo||bar"));
		assertEquals("(f\\&&oo\\||bar)", escaped.stringValue());
	}
}
