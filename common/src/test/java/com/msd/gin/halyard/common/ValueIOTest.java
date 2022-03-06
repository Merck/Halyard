package com.msd.gin.halyard.common;

import java.io.IOException;
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
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class ValueIOTest {
	private static final ValueFactory vf = SimpleValueFactory.getInstance();

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] { { RDF.TYPE }, { vf.createLiteral("foo") }, { vf.createBNode() },
				{ vf.createIRI("test:/foo") }, { vf.createLiteral("5423") }, { vf.createLiteral("\u98DF") },
				{ vf.createLiteral(true) }, { vf.createLiteral((byte) 6) }, { vf.createLiteral((short) 7843) }, { vf.createLiteral(34) }, { vf.createLiteral(87.232) },
				{ vf.createLiteral(74234l) }, { vf.createLiteral(4.809f) },
				{ vf.createLiteral(BigInteger.valueOf(96)) }, { vf.createLiteral(BigDecimal.valueOf(856.03)) },
				{ vf.createIRI(RDF.NAMESPACE) }, { vf.createLiteral("xyz", vf.createIRI(RDF.NAMESPACE)) },
				{ vf.createLiteral(new Date()) }, { vf.createLiteral("13:03:22.000", XSD.TIME) },
				{ vf.createLiteral("1980-02-14", XSD.DATE)},
				{ vf.createLiteral("foo", vf.createIRI("urn:bar:1"))}, { vf.createLiteral("foo", "en-gb") },
				{ vf.createLiteral("<?xml version=\"1.0\" encoding=\"UTF-8\"?><test attr=\"foo\">bar</test>", RDF.XMLLITERAL)},
				{ vf.createLiteral("invalid xml still works", RDF.XMLLITERAL) },
				{ vf.createLiteral("0000-06-20T00:00:00Z", XSD.DATETIME) }});
	}

	private Value expected;

	public ValueIOTest(Value v) {
		this.expected = v;
	}

	@Test
	public void testToAndFromBytes() throws IOException {
		byte[] b = ValueIO.writeBytes(expected, null);
		Value actual = ValueIO.readValue(ByteBuffer.wrap(b), vf, null);
		assertEquals(expected, actual);

		// check readValue() works on a subsequence
		ByteBuffer extbuf = ByteBuffer.allocate(3 + b.length + 7);
		// place b somewhere in the middle
		extbuf.position(extbuf.position() + 3);
		extbuf.mark();
		extbuf.put(b);
		extbuf.limit(extbuf.position());
		extbuf.reset();
		actual = ValueIO.readValue(extbuf, vf, null);
		assertEquals("Buffer position", extbuf.limit(), extbuf.position());
		assertEquals(expected, actual);
	}

	@Test
	public void testRDFValue() {
		byte[] id = Hashes.id(expected);
		if (expected instanceof Literal) {
			assertTrue(Hashes.isLiteral(id));
			RDFObject obj = RDFObject.create(expected);
			assertRDFValueHashes(id, obj);
		} else {
			assertFalse(Hashes.isLiteral(id));
			if (expected instanceof IRI) {
				RDFObject obj = RDFObject.create(expected);
				assertRDFValueHashes(id, obj);
				RDFSubject subj = RDFSubject.create((IRI) expected);
				assertRDFValueHashes(id, subj);
				RDFContext ctx = RDFContext.create((IRI) expected);
				assertRDFValueHashes(id, ctx);
				RDFPredicate pred = RDFPredicate.create((IRI) expected);
				assertRDFValueHashes(id, pred);
			} else if (expected instanceof Resource) {
				RDFObject obj = RDFObject.create(expected);
				assertRDFValueHashes(id, obj);
				RDFSubject subj = RDFSubject.create((Resource) expected);
				assertRDFValueHashes(id, subj);
				RDFContext ctx = RDFContext.create((Resource) expected);
				assertRDFValueHashes(id, ctx);
			} else {
				throw new AssertionError();
			}
		}
	}

	private static void assertRDFValueHashes(byte[] id, RDFValue<?> v) {
		for(StatementIndex idx : StatementIndex.values()) {
			assertArrayEquals(id, concat(v.getRole().rotateLeft(v.getKeyHash(idx), 0, v.keyHashSize(), idx), v.getQualifierHash()));
			if(!(v instanceof RDFContext)) { // context doesn't have end-hashes
				assertArrayEquals(id, concat(v.getRole().rotateLeft(v.getEndKeyHash(idx), 0, v.endKeyHashSize(), idx), v.getEndQualifierHash()));
			}
		}
	}

	private static byte[] concat(byte[] b1, byte[] b2) {
		byte[] arr = new byte[b1.length+b2.length];
		System.arraycopy(b1, 0, arr, 0, b1.length);
		System.arraycopy(b2, 0, arr, b1.length, b2.length);
		return arr;
	}
}
