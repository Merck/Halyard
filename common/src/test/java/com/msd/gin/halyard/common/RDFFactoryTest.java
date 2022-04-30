package com.msd.gin.halyard.common;

import com.msd.gin.halyard.vocab.WIKIDATA;

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

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class RDFFactoryTest {
    private static final RDFFactory rdfFactory = RDFFactory.create();
	private static final Date NOW = new Date();

	private static String longString(String s) {
		String[] copies = new String[200/s.length()+1];
		Arrays.fill(copies, s);
		return String.join(" ", copies);
	}

	static List<Value> createData(ValueFactory vf) {
		return Arrays.asList(RDF.TYPE, vf.createLiteral("foo"), vf.createBNode("__foobar__"),
			vf.createIRI("test:/foo"),
			vf.createIRI("http://www.testmyiri.com"),
			vf.createIRI("https://www.testmyiri.com"),
			vf.createIRI("http://dx.doi.org/", "blah"),
			vf.createIRI("https://dx.doi.org/", "blah"),
			vf.createLiteral("5423"), vf.createLiteral("\u98DF"),
			vf.createLiteral(true), vf.createLiteral((byte) 6), vf.createLiteral((short) 7843),
			vf.createLiteral(34), vf.createLiteral(87.232), vf.createLiteral(74234l), vf.createLiteral(4.809f),
			vf.createLiteral(BigInteger.valueOf(96)),
			vf.createLiteral(BigInteger.valueOf(Integer.MIN_VALUE)),
			vf.createLiteral(String.valueOf(Long.MAX_VALUE)+String.valueOf(Long.MAX_VALUE), XSD.INTEGER),
			vf.createLiteral(BigDecimal.valueOf(856.03)),
			vf.createLiteral("z", XSD.INT),
			vf.createIRI(RDF.NAMESPACE), vf.createLiteral("xyz", vf.createIRI(RDF.NAMESPACE)),
			vf.createLiteral(NOW), vf.createLiteral("13:03:22.000", XSD.TIME),
			vf.createLiteral("1980-02-14", XSD.DATE),
			vf.createLiteral("foo", vf.createIRI("urn:bar:1")), vf.createLiteral("foo", "en-GB"), vf.createLiteral("bar", "zx-XY"),
			vf.createLiteral("POINT (139.81 35.6972)", GEO.WKT_LITERAL),
			vf.createLiteral("invalid still works (139.81 35.6972)", GEO.WKT_LITERAL),
			vf.createLiteral("<?xml version=\"1.0\" encoding=\"UTF-8\"?><test attr=\"foo\">bar</test>", RDF.XMLLITERAL),
			vf.createLiteral("invalid xml still works", RDF.XMLLITERAL),
			vf.createLiteral("0000-06-20T00:00:00Z", XSD.DATETIME),
			vf.createLiteral(longString("The cat slept on the mat.")),
			vf.createLiteral(longString("¿Dónde está el gato?"), "es"),
			vf.createIRI(WIKIDATA.WDV_NAMESPACE, "400f9abd3fd761c62af23dbe8f8432158a6ce272"),
			vf.createIRI(WIKIDATA.WDV_NAMESPACE, "invalid")
		);
	}

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> data() {
		Function<Value,Object[]> toArg = v -> new Object[] {v};
		List<Object[]> testValues = new ArrayList<>();
		testValues.addAll(createData(SimpleValueFactory.getInstance()).stream().map(toArg).collect(Collectors.toList()));
		testValues.addAll(createData(rdfFactory.getIdValueFactory()).stream().map(toArg).collect(Collectors.toList()));
		return testValues;
	}

	private Value expected;

	public RDFFactoryTest(Value v) {
		this.expected = v;
	}

	@Test
	public void testToAndFromBytesNoBuffer() {
		testToAndFromBytes(0);
	}

	@Test
	public void testToAndFromBytesBigBuffer() {
		testToAndFromBytes(10240);
	}

	private void testToAndFromBytes(int bufferSize) {
        ValueIO.Writer writer = rdfFactory.createWriter();
        ValueIO.Reader reader = rdfFactory.createReader(rdfFactory.getIdValueFactory());

        ByteBuffer buf = ByteBuffer.allocate(bufferSize);
		buf = writer.writeTo(expected, buf);
		buf.flip();
		int size = buf.limit();
		Value actual = reader.readValue(buf);
		assertEquals(expected, actual);
		assertEquals(actual, expected);
		assertEquals(expected.hashCode(), actual.hashCode());

		// check readValue() works on a subsequence
		ByteBuffer extbuf = ByteBuffer.allocate(3 + size + 7);
		// place b somewhere in the middle
		extbuf.position(extbuf.position() + 3);
		extbuf.mark();
		buf.flip();
		extbuf.put(buf);
		extbuf.limit(extbuf.position());
		extbuf.reset();
		actual = reader.readValue(extbuf);
		assertEquals("Buffer position", 3 + size, extbuf.position());
		assertEquals("Buffer state", extbuf.limit(), extbuf.position());
		assertEquals(expected, actual);
	}

	@Test
	public void testRDFValue() {
		Identifier id = rdfFactory.id(expected);
		if (expected instanceof Identifiable) {
			assertEquals(id, ((Identifiable)expected).getId());
		}

		assertEquals("isIRI", expected.isIRI(), id.isIRI());
		assertEquals("isLiteral", expected.isLiteral(), id.isLiteral());
		assertEquals("isBNode", expected.isBNode(), id.isBNode());
		assertEquals("isTriple", expected.isTriple(), id.isTriple());

		if (expected instanceof Literal) {
			IRI dt = ((Literal)expected).getDatatype();
			assertEquals("isString", XSD.STRING.equals(dt) || RDF.LANGSTRING.equals(dt), id.isString());
			RDFObject obj = rdfFactory.createObject(expected);
			assertRDFValueHashes(id, obj);
		} else if (expected instanceof IRI) {
			RDFObject obj = rdfFactory.createObject(expected);
			assertRDFValueHashes(id, obj);
			RDFSubject subj = rdfFactory.createSubject((IRI) expected);
			assertRDFValueHashes(id, subj);
			RDFContext ctx = rdfFactory.createContext((IRI) expected);
			assertRDFValueHashes(id, ctx);
			RDFPredicate pred = rdfFactory.createPredicate((IRI) expected);
			assertRDFValueHashes(id, pred);
		} else if (expected instanceof BNode) {
			RDFObject obj = rdfFactory.createObject(expected);
			assertRDFValueHashes(id, obj);
			RDFSubject subj = rdfFactory.createSubject((Resource) expected);
			assertRDFValueHashes(id, subj);
			RDFContext ctx = rdfFactory.createContext((Resource) expected);
			assertRDFValueHashes(id, ctx);
		} else {
			throw new AssertionError();
		}
	}

	private static void assertRDFValueHashes(Identifier id, RDFValue<?,?> v) {
        StatementIndex<SPOC.S,SPOC.P,SPOC.O,SPOC.C> spo = rdfFactory.getSPOIndex();
        StatementIndex<SPOC.P,SPOC.O,SPOC.S,SPOC.C> pos = rdfFactory.getPOSIndex();
        StatementIndex<SPOC.O,SPOC.S,SPOC.P,SPOC.C> osp = rdfFactory.getOSPIndex();
        StatementIndex<SPOC.C,SPOC.S,SPOC.P,SPOC.O> cspo = rdfFactory.getCSPOIndex();
        StatementIndex<SPOC.C,SPOC.P,SPOC.O,SPOC.S> cpos = rdfFactory.getCPOSIndex();
        StatementIndex<SPOC.C,SPOC.O,SPOC.S,SPOC.P> cosp = rdfFactory.getCOSPIndex();
		for(StatementIndex<?,?,?,?> idx : new StatementIndex[] {spo, pos, osp, cspo, cpos, cosp}) {
			String testName = v.toString() + " for " + idx.toString();
			byte[] keyHash = v.getKeyHash(idx);
			int keyHashSize = v.getRole().keyHashSize();
			assertEquals(testName, keyHashSize, keyHash.length);

			byte[] idxIdBytes = new byte[rdfFactory.getIdSize()];
			v.getRole().unrotate(keyHash, 0, keyHashSize, idx, idxIdBytes);
			v.writeQualifierHashTo(ByteBuffer.wrap(idxIdBytes, keyHashSize, idxIdBytes.length-keyHashSize));
			assertEquals(testName, id, rdfFactory.id(idxIdBytes));

			if(!(v instanceof RDFContext)) { // context doesn't have end-hashes
				byte[] endKeyHash = v.getEndKeyHash(idx);
				int endKeyHashSize = v.getRole().endKeyHashSize();
				assertEquals(testName, endKeyHashSize, endKeyHash.length);

				byte[] cidxIdBytes = new byte[rdfFactory.getIdSize()];
				v.getRole().unrotate(endKeyHash, 0, endKeyHashSize, idx, cidxIdBytes);
				v.writeEndQualifierHashTo(ByteBuffer.wrap(cidxIdBytes, endKeyHashSize, cidxIdBytes.length-endKeyHashSize));
				assertEquals(testName, id, rdfFactory.id(cidxIdBytes));
			}
		}
	}
}
