package com.msd.gin.halyard.common;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

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
	private static final Date NOW = new Date();

	public static List<Value> createData(ValueFactory vf) {
		return Arrays.asList(RDF.TYPE, vf.createLiteral("foo"), vf.createBNode("__foobar__"),
			vf.createIRI("test:/foo"), vf.createLiteral("5423"), vf.createLiteral("\u98DF"),
			vf.createLiteral(true), vf.createLiteral((byte) 6), vf.createLiteral((short) 7843),
			vf.createLiteral(34), vf.createLiteral(87.232), vf.createLiteral(74234l), vf.createLiteral(4.809f),
			vf.createLiteral(BigInteger.valueOf(96)), vf.createLiteral(BigDecimal.valueOf(856.03)),
			vf.createIRI(RDF.NAMESPACE), vf.createLiteral("xyz", vf.createIRI(RDF.NAMESPACE)),
			vf.createLiteral(NOW), vf.createLiteral("13:03:22.000", XSD.TIME),
			vf.createLiteral("1980-02-14", XSD.DATE),
			vf.createLiteral("foo", vf.createIRI("urn:bar:1")), vf.createLiteral("foo", "en-GB"), vf.createLiteral("bar", "zx-XY"),
			vf.createLiteral("<?xml version=\"1.0\" encoding=\"UTF-8\"?><test attr=\"foo\">bar</test>", RDF.XMLLITERAL),
			vf.createLiteral("invalid xml still works", RDF.XMLLITERAL),
			vf.createLiteral("0000-06-20T00:00:00Z", XSD.DATETIME));
	}

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> data() {
		Function<Value,Object[]> toArg = v -> new Object[] {v};
		List<Object[]> testValues = new ArrayList<>();
		testValues.addAll(createData(SimpleValueFactory.getInstance()).stream().map(toArg).collect(Collectors.toList()));
		testValues.addAll(createData(IdValueFactory.getInstance()).stream().map(toArg).collect(Collectors.toList()));
		return testValues;
	}

	private Value expected;

	public ValueIOTest(Value v) {
		this.expected = v;
	}

	@Test
	public void testToAndFromBytes() {
		ByteBuffer buf = ByteBuffer.allocate(0);
		buf = ValueIO.CELL_WRITER.writeTo(expected, buf);
		buf.flip();
		int size = buf.limit();
		Value actual = ValueIO.SIMPLE_READER.readValue(buf);
		assertEquals(expected, actual);

		// check readValue() works on a subsequence
		ByteBuffer extbuf = ByteBuffer.allocate(3 + size + 7);
		// place b somewhere in the middle
		extbuf.position(extbuf.position() + 3);
		extbuf.mark();
		buf.flip();
		extbuf.put(buf);
		extbuf.limit(extbuf.position());
		extbuf.reset();
		actual = ValueIO.SIMPLE_READER.readValue(extbuf);
		assertEquals("Buffer position", 3 + size, extbuf.position());
		assertEquals("Buffer state", extbuf.limit(), extbuf.position());
		assertEquals(expected, actual);
	}

	@Test
	public void testRDFValue() {
		Identifier id = Identifier.id(expected);
		if (expected instanceof Identifiable) {
			assertEquals(id, ((Identifiable)expected).getId());
		}

		if (expected instanceof Literal) {
			assertTrue(id.isLiteral());
			RDFObject obj = RDFObject.create(expected);
			assertRDFValueHashes(id, obj);
		} else {
			assertFalse(id.isLiteral());
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

	private static void assertRDFValueHashes(Identifier id, RDFValue<?> v) {
		for(StatementIndex idx : StatementIndex.values()) {
			byte[] keyHash = v.getKeyHash(idx);
			assertEquals(v.keyHashSize(), keyHash.length);

			ByteBuffer idxId = ByteBuffer.allocate(Identifier.ID_SIZE);
			idxId.put(v.getRole().unrotate(keyHash, idx));
			v.writeQualifierHashTo(idxId);
			assertEquals(id, new Identifier(idxId.array()));

			if(!(v instanceof RDFContext)) { // context doesn't have end-hashes
				byte[] endKeyHash = v.getEndKeyHash(idx);
				assertEquals(v.endKeyHashSize(), endKeyHash.length);

				ByteBuffer cidxId = ByteBuffer.allocate(Identifier.ID_SIZE);
				cidxId.put(v.getRole().unrotate(endKeyHash, idx));
				v.writeEndQualifierHashTo(cidxId);
				assertEquals(id, new Identifier(cidxId.array()));
			}
		}
	}
}
