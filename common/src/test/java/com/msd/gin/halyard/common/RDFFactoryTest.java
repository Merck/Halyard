package com.msd.gin.halyard.common;

import com.msd.gin.halyard.vocab.HALYARD;
import com.msd.gin.halyard.vocab.WIKIDATA;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

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
    private static final StatementIndices stmtIndices = StatementIndices.create();
	private static final Date NOW = new Date();

	private static String longString(String s) {
		String[] copies = new String[1000/s.length()+1];
		Arrays.fill(copies, s);
		return String.join(" ", copies);
	}

	static List<Object[]> createData(ValueFactory vf) {
		return Arrays.asList(
			new Object[] {RDF.TYPE, ValueIO.IRI_HASH_TYPE},
			new Object[] {vf.createLiteral("foo"), ValueIO.UNCOMPRESSED_STRING_TYPE},
			new Object[] {vf.createBNode("__foobar__"), ValueIO.BNODE_TYPE},
			new Object[] {vf.createIRI("test:/foo"), ValueIO.IRI_TYPE},
			new Object[] {vf.createIRI("http://www.testmyiri.com"), ValueIO.COMPRESSED_IRI_TYPE},
			new Object[] {vf.createIRI("https://www.testmyiri.com"), ValueIO.COMPRESSED_IRI_TYPE},
			new Object[] {vf.createIRI("http://dx.doi.org/", "blah"), ValueIO.COMPRESSED_IRI_TYPE},
			new Object[] {vf.createIRI("https://dx.doi.org/", "blah"), ValueIO.COMPRESSED_IRI_TYPE},
			new Object[] {vf.createLiteral("5423"), ValueIO.UNCOMPRESSED_STRING_TYPE},
			new Object[] {vf.createLiteral("\u98DF"), ValueIO.UNCOMPRESSED_STRING_TYPE},
			new Object[] {vf.createLiteral(true), ValueIO.TRUE_TYPE},
			new Object[] {vf.createLiteral((byte) 6), ValueIO.BYTE_TYPE},
			new Object[] {vf.createLiteral((short) 7843), ValueIO.SHORT_TYPE},
			new Object[] {vf.createLiteral(34), ValueIO.INT_TYPE},
			new Object[] {vf.createLiteral(87.232), ValueIO.DOUBLE_TYPE},
			new Object[] {vf.createLiteral(74234l), ValueIO.LONG_TYPE},
			new Object[] {vf.createLiteral(4.809f), ValueIO.FLOAT_TYPE},
			new Object[] {vf.createLiteral(BigInteger.valueOf(96)), ValueIO.SHORT_COMPRESSED_BIG_INT_TYPE},
			new Object[] {vf.createLiteral(BigInteger.valueOf(Integer.MIN_VALUE)), ValueIO.INT_COMPRESSED_BIG_INT_TYPE},
			new Object[] {vf.createLiteral(String.valueOf(Long.MAX_VALUE)+String.valueOf(Long.MAX_VALUE), XSD.INTEGER), ValueIO.BIG_INT_TYPE},
			new Object[] {vf.createLiteral(BigDecimal.valueOf(856.03)), ValueIO.BIG_FLOAT_TYPE},
			new Object[] {vf.createLiteral("z", XSD.INT), ValueIO.DATATYPE_LITERAL_TYPE},
			new Object[] {vf.createIRI(RDF.NAMESPACE), ValueIO.NAMESPACE_HASH_TYPE},
			new Object[] {vf.createLiteral("xyz", vf.createIRI(RDF.NAMESPACE)), ValueIO.DATATYPE_LITERAL_TYPE},
			new Object[] {vf.createLiteral(NOW), ValueIO.DATETIME_TYPE},
			new Object[] {vf.createLiteral(LocalDateTime.of(1990, 6, 20, 0, 0, 0, 20005000)), ValueIO.DATETIME_TYPE},
			new Object[] {vf.createLiteral("13:03:22", XSD.TIME), ValueIO.TIME_TYPE},
			new Object[] {vf.createLiteral(LocalTime.of(13, 3, 22, 40030000)), ValueIO.TIME_TYPE},
			new Object[] {vf.createLiteral("1980-02-14", XSD.DATE), ValueIO.DATE_TYPE},
			new Object[] {vf.createLiteral("foo", vf.createIRI("urn:bar:1")), ValueIO.DATATYPE_LITERAL_TYPE},
			new Object[] {vf.createLiteral("foo", "en-GB"), ValueIO.LANGUAGE_HASH_LITERAL_TYPE},
			new Object[] {vf.createLiteral("bar", "zx-XY"), ValueIO.LANGUAGE_LITERAL_TYPE},
			new Object[] {vf.createLiteral("漫画", "ja"), ValueIO.LANGUAGE_HASH_LITERAL_TYPE},
			new Object[] {vf.createLiteral("POINT (139.81 35.6972)", GEO.WKT_LITERAL), ValueIO.WKT_LITERAL_TYPE},
			new Object[] {vf.createLiteral("invalid still works (139.81 35.6972)", GEO.WKT_LITERAL), ValueIO.WKT_LITERAL_TYPE},
			new Object[] {vf.createLiteral("<?xml version=\"1.0\" encoding=\"UTF-8\"?><test attr=\"foo\">bar</test>", RDF.XMLLITERAL), ValueIO.XML_TYPE},
			new Object[] {vf.createLiteral("<invalid xml still works", RDF.XMLLITERAL), ValueIO.XML_TYPE},
			new Object[] {vf.createLiteral("0000-06-20T00:00:00Z", XSD.DATETIME), ValueIO.DATATYPE_LITERAL_TYPE},
			new Object[] {vf.createLiteral(longString("The cat slept on the mat.")), ValueIO.COMPRESSED_STRING_TYPE},
			new Object[] {vf.createLiteral(longString("¿Dónde está el gato?"), "es"), ValueIO.LANGUAGE_HASH_LITERAL_TYPE},
			new Object[] {vf.createIRI(HALYARD.VALUE_ID_NS.getName(), "eRg5UlsxjZuh-4meqlYQe3-J8X8"), ValueIO.ENCODED_IRI_TYPE},
			new Object[] {vf.createIRI(WIKIDATA.WDV_NAMESPACE, "400f9abd3fd761c62af23dbe8f8432158a6ce272"), ValueIO.ENCODED_IRI_TYPE},
			new Object[] {vf.createIRI(WIKIDATA.WDV_NAMESPACE, "invalid"), ValueIO.NAMESPACE_HASH_TYPE},
			new Object[] {vf.createIRI(WIKIDATA.WDV_NAMESPACE+"400f9abd3fd761c62af23dbe8f8432158a6ce272/"), ValueIO.END_SLASH_ENCODED_IRI_TYPE}
		);
	}

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> data() {
		List<Object[]> testValues = new ArrayList<>();
		testValues.addAll(createData(SimpleValueFactory.getInstance()));
		testValues.addAll(createData(IdValueFactory.INSTANCE));
		return testValues;
	}

	private final RDFFactory rdfFactory;
	private Value expectedValue;
	private byte expectedEncodingType;

	public RDFFactoryTest(Value v, byte encodingType) {
		this.rdfFactory = stmtIndices.getRDFFactory();
		this.expectedValue = v;
		this.expectedEncodingType = encodingType;
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
        ValueIO.Reader reader = rdfFactory.createReader(IdValueFactory.INSTANCE);

        ByteBuffer buf = ByteBuffer.allocate(bufferSize);
		buf = writer.writeTo(expectedValue, buf);
		buf.flip();
		byte actualEncodingType = buf.get(0);
		assertEquals(expectedEncodingType, actualEncodingType);
		int size = buf.limit();
		Value actual = reader.readValue(buf);
		assertEquals(expectedValue, actual);
		assertEquals(actual, expectedValue);
		assertEquals(expectedValue.hashCode(), actual.hashCode());

		// check readValue() works on a subsequence
		int beforeLen = 3;
		int afterLen = 7;
		ByteBuffer extbuf = ByteBuffer.allocate(beforeLen + size + afterLen);
		// place b somewhere in the middle
		extbuf.position(beforeLen);
		extbuf.mark();
		buf.flip();
		extbuf.put(buf);
		extbuf.limit(extbuf.position());
		extbuf.reset();
		actual = reader.readValue(extbuf);
		assertEquals("Buffer position", beforeLen + size, extbuf.position());
		assertEquals("Buffer state", extbuf.limit(), extbuf.position());
		assertEquals(expectedValue, actual);
	}

	@Test
	public void testRDFValue() {
		ValueIdentifier id = rdfFactory.id(expectedValue);
		if (expectedValue instanceof IdentifiableValue) {
			assertEquals(id, ((IdentifiableValue)expectedValue).getId(rdfFactory));
		}

		assertEquals("isIRI", expectedValue.isIRI(), id.isIRI());
		assertEquals("isLiteral", expectedValue.isLiteral(), id.isLiteral());
		assertEquals("isBNode", expectedValue.isBNode(), id.isBNode());
		assertEquals("isTriple", expectedValue.isTriple(), id.isTriple());

		if (expectedValue instanceof Literal) {
			IRI dt = ((Literal)expectedValue).getDatatype();
			assertEquals("isString", XSD.STRING.equals(dt) || RDF.LANGSTRING.equals(dt), id.isString());
			RDFObject obj = rdfFactory.createObject(expectedValue);
			assertRDFValueHashes(id, obj);
		} else if (expectedValue instanceof IRI) {
			RDFObject obj = rdfFactory.createObject(expectedValue);
			assertRDFValueHashes(id, obj);
			RDFSubject subj = rdfFactory.createSubject((IRI) expectedValue);
			assertRDFValueHashes(id, subj);
			RDFContext ctx = rdfFactory.createContext((IRI) expectedValue);
			assertRDFValueHashes(id, ctx);
			RDFPredicate pred = rdfFactory.createPredicate((IRI) expectedValue);
			assertRDFValueHashes(id, pred);
		} else if (expectedValue instanceof BNode) {
			RDFObject obj = rdfFactory.createObject(expectedValue);
			assertRDFValueHashes(id, obj);
			RDFSubject subj = rdfFactory.createSubject((Resource) expectedValue);
			assertRDFValueHashes(id, subj);
			RDFContext ctx = rdfFactory.createContext((Resource) expectedValue);
			assertRDFValueHashes(id, ctx);
		} else {
			throw new AssertionError();
		}
	}

	private static void assertRDFValueHashes(ValueIdentifier id, RDFValue<?,?> v) {
        StatementIndex<SPOC.S,SPOC.P,SPOC.O,SPOC.C> spo = stmtIndices.getSPOIndex();
        StatementIndex<SPOC.P,SPOC.O,SPOC.S,SPOC.C> pos = stmtIndices.getPOSIndex();
        StatementIndex<SPOC.O,SPOC.S,SPOC.P,SPOC.C> osp = stmtIndices.getOSPIndex();
        StatementIndex<SPOC.C,SPOC.S,SPOC.P,SPOC.O> cspo = stmtIndices.getCSPOIndex();
        StatementIndex<SPOC.C,SPOC.P,SPOC.O,SPOC.S> cpos = stmtIndices.getCPOSIndex();
        StatementIndex<SPOC.C,SPOC.O,SPOC.S,SPOC.P> cosp = stmtIndices.getCOSPIndex();
        RDFFactory rdfFactory = stmtIndices.getRDFFactory();
		for(StatementIndex<?,?,?,?> idx : new StatementIndex[] {spo, pos, osp, cspo, cpos, cosp}) {
			String testName = v.toString() + " for " + idx.toString();
			byte[] keyHash = v.getKeyHash(idx);
			int keyHashSize = v.getRole().keyHashSize();
			assertEquals(testName, keyHashSize, keyHash.length);

			byte[] idxIdBytes = new byte[rdfFactory.getIdSize()];
			id.getFormat().unrotate(keyHash, 0, keyHashSize, v.getRole().toShift(idx), idxIdBytes);
			v.writeQualifierHashTo(ByteBuffer.wrap(idxIdBytes, keyHashSize, idxIdBytes.length-keyHashSize));
			assertEquals(testName, id, rdfFactory.id(idxIdBytes));

			if(!(v instanceof RDFContext)) { // context doesn't have end-hashes
				byte[] endKeyHash = v.getEndKeyHash(idx);
				int endKeyHashSize = v.getRole().endKeyHashSize();
				assertEquals(testName, endKeyHashSize, endKeyHash.length);

				byte[] cidxIdBytes = new byte[rdfFactory.getIdSize()];
				id.getFormat().unrotate(endKeyHash, 0, endKeyHashSize, v.getRole().toShift(idx), cidxIdBytes);
				v.writeEndQualifierHashTo(ByteBuffer.wrap(cidxIdBytes, endKeyHashSize, cidxIdBytes.length-endKeyHashSize));
				assertEquals(testName, id, rdfFactory.id(cidxIdBytes));
			}
		}
	}
}
