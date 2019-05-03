package com.msd.gin.halyard.common;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class HalyardTableUtilsRDFTest {
	private static final ValueFactory vf = SimpleValueFactory.getInstance();

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] { { RDF.TYPE }, { vf.createLiteral("foo") }, { vf.createBNode() },
				{ vf.createIRI("test:/foo") }, { vf.createLiteral("5423") }, { vf.createLiteral("\u98DF") },
				{ vf.createLiteral(true) }, { vf.createLiteral((byte) 6) }, { vf.createLiteral((short) 7843) }, { vf.createLiteral(34) }, { vf.createLiteral(87.232) },
				{ vf.createLiteral(74234l) }, { vf.createLiteral(4.809f) },
				{ vf.createLiteral(BigInteger.valueOf(96)) }, { vf.createLiteral(BigDecimal.valueOf(856.03)) },
				{ vf.createIRI(RDF.NAMESPACE) }, { vf.createLiteral(new Date()) }, { vf.createLiteral("13:03:22.000", XMLSchema.TIME) },
				{ vf.createLiteral("1980-02-14", XMLSchema.DATE)},
				{ vf.createLiteral("foo", vf.createIRI("urn:bar:1"))}, { vf.createLiteral("foo", "en-gb") }});
	}

	private Value expected;

	public HalyardTableUtilsRDFTest(Value v) {
		this.expected = v;
	}

	@Test
	public void testToAndFromBytes() {
		byte[] b = HalyardTableUtils.writeBytes(expected);
		Value actual = HalyardTableUtils.readValue(ByteBuffer.wrap(b), vf);
		assertEquals(expected, actual);

		// check readValue() works on a subsequence
		ByteBuffer extbuf = ByteBuffer.allocate(1 + b.length + 1);
		// place b in the middle
		extbuf.position(extbuf.position() + 1);
		extbuf.put(b);
		// set boundary
		extbuf.position(1).limit(extbuf.capacity() - 1);
		actual = HalyardTableUtils.readValue(extbuf, vf);
		assertEquals(expected, actual);
	}

	@Test
	public void testRDFValue() {
		if (expected instanceof Literal) {
			RDFObject obj = RDFObject.create(expected);
			assertTrue(RDFObject.isLiteral(obj.getHash()));
			assertTrue(RDFObject.isLiteral(obj.getEndHash()));
		} else if (expected instanceof IRI) {
			RDFObject obj = RDFObject.create(expected);
			assertFalse(RDFObject.isLiteral(obj.getHash()));
			assertFalse(RDFObject.isLiteral(obj.getEndHash()));
			RDFSubject subj = RDFSubject.create((IRI) expected);
			subj.getHash();
			subj.getEndHash();
			RDFContext ctx = RDFContext.create((IRI) expected);
			ctx.getHash();
			RDFPredicate pred = RDFPredicate.create((IRI) expected);
			pred.getHash();
			pred.getEndHash();
		} else if (expected instanceof Resource) {
			RDFObject obj = RDFObject.create(expected);
			assertFalse(RDFObject.isLiteral(obj.getHash()));
			assertFalse(RDFObject.isLiteral(obj.getEndHash()));
			RDFSubject subj = RDFSubject.create((Resource) expected);
			subj.getHash();
			subj.getEndHash();
			RDFContext ctx = RDFContext.create((Resource) expected);
			ctx.getHash();
		} else {
			throw new AssertionError();
		}
	}
}
