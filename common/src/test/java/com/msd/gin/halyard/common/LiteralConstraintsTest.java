package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.Test;
import static org.junit.Assert.*;

public class LiteralConstraintsTest {
	@Test
	public void testAllLiteral() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		LiteralConstraints lc = new LiteralConstraints();
		assertTrue(lc.allLiterals());
		assertTrue(lc.test(vf.createLiteral(1)));
		assertTrue(lc.test(vf.createLiteral("foobar")));
		assertTrue(lc.test(vf.createLiteral("foo", "en")));
	}

	@Test
	public void testStringLiteral() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		LiteralConstraints lc = new LiteralConstraints(XSD.STRING);
		assertTrue(lc.stringsOnly());
		assertFalse(lc.test(vf.createLiteral(1)));
		assertTrue(lc.test(vf.createLiteral("foobar")));
		assertFalse(lc.test(vf.createLiteral("foo", "en")));
	}

	@Test
	public void testLangLiteral() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		LiteralConstraints lc = new LiteralConstraints("en");
		assertTrue(lc.stringsOnly());
		assertFalse(lc.test(vf.createLiteral(1)));
		assertFalse(lc.test(vf.createLiteral("foobar")));
		assertTrue(lc.test(vf.createLiteral("foo", "en")));
	}

	@Test
	public void testOtherLiteral() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		LiteralConstraints lc = new LiteralConstraints(XSD.INT);
		assertFalse(lc.stringsOnly());
		assertTrue(lc.test(vf.createLiteral(1)));
		assertFalse(lc.test(vf.createLiteral("foobar")));
		assertFalse(lc.test(vf.createLiteral("foo", "en")));
	}
}
