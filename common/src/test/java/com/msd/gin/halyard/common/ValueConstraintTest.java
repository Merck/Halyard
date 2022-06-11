package com.msd.gin.halyard.common;

import com.msd.gin.halyard.vocab.HALYARD;

import java.util.Date;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.Test;
import static org.junit.Assert.*;

public class ValueConstraintTest {

	@Test
	public void testIRIs() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		ValueConstraint vc = new ValueConstraint(ValueType.IRI);
		assertFalse(vc.test(vf.createLiteral(1)));
		assertTrue(vc.test(vf.createIRI("urn:foo:bar")));
	}

	@Test
	public void testBNodes() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		ValueConstraint vc = new ValueConstraint(ValueType.BNODE);
		assertTrue(vc.test(vf.createBNode()));
		assertFalse(vc.test(vf.createIRI("urn:foo:bar")));
	}

	@Test
	public void testAllLiterals() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		ValueConstraint vc = new ValueConstraint(ValueType.LITERAL);
		assertTrue(vc.test(vf.createLiteral(1)));
		assertTrue(vc.test(vf.createLiteral("foobar")));
		assertTrue(vc.test(vf.createLiteral("foo", "en")));
	}

	@Test
	public void testStringLiteral() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		LiteralConstraint vc = new LiteralConstraint(XSD.STRING);
		assertFalse(vc.test(vf.createLiteral(1)));
		assertTrue(vc.test(vf.createLiteral("foobar")));
		assertFalse(vc.test(vf.createLiteral("foo", "en")));
	}

	@Test
	public void testLangLiteral() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		LiteralConstraint vc = new LiteralConstraint("en");
		assertFalse(vc.test(vf.createLiteral(1)));
		assertFalse(vc.test(vf.createLiteral("foobar")));
		assertTrue(vc.test(vf.createLiteral("foo", "en")));
	}

	@Test
	public void testOtherLiteral() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		LiteralConstraint vc = new LiteralConstraint(HALYARD.NON_STRING);
		assertTrue(vc.test(vf.createLiteral(1)));
		assertTrue(vc.test(vf.createLiteral(new Date())));
		assertFalse(vc.test(vf.createLiteral("foobar")));
		assertFalse(vc.test(vf.createLiteral("foo", "en")));
	}

	@Test
	public void testAnyNumeric() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		LiteralConstraint vc = new LiteralConstraint(HALYARD.ANY_NUMERIC);
		assertTrue(vc.test(vf.createLiteral(1)));
		assertTrue(vc.test(vf.createLiteral(1.8)));
		assertFalse(vc.test(vf.createLiteral("foobar")));
		assertFalse(vc.test(vf.createLiteral("foo", "en")));
	}

	@Test
	public void testEquals() {
		ValueConstraint vc = new ValueConstraint(ValueType.LITERAL);
		LiteralConstraint lc = new LiteralConstraint(XSD.STRING);
		assertNotEquals(vc, lc);
		assertNotEquals(lc, vc);
	}
}
