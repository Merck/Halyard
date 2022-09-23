package com.msd.gin.halyard.common;

import com.msd.gin.halyard.vocab.HALYARD;
import com.msd.gin.halyard.vocab.WIKIDATA;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class IdValueFactoryExtendedTest {
	private static final Date NOW = new Date();

	private static List<Value> createData(ValueFactory vf) {
		return Arrays.asList(
			vf.createLiteral("foo"),
			vf.createBNode("__foobar__"),
			vf.createIRI("test:/foo"),
			vf.createIRI("http://www.testmyiri.com"),
			vf.createIRI("https://www.testmyiri.com"),
			vf.createIRI("http://dx.doi.org/", "blah"),
			vf.createIRI("https://dx.doi.org/", "blah"),
			vf.createLiteral("5423"),
			vf.createLiteral("\u98DF"),
			vf.createLiteral(true),
			vf.createLiteral((byte) 6),
			vf.createLiteral((short) 7843),
			vf.createLiteral(34),
			vf.createLiteral(87.232),
			vf.createLiteral(74234l),
			vf.createLiteral(4.809f),
			vf.createLiteral(BigInteger.valueOf(96)),
			vf.createLiteral(BigInteger.valueOf(Integer.MIN_VALUE)),
			vf.createLiteral(String.valueOf(Long.MAX_VALUE)+String.valueOf(Long.MAX_VALUE), XSD.INTEGER),
			vf.createLiteral(BigDecimal.valueOf(856.03)),
			vf.createLiteral("z", XSD.INT),
			vf.createIRI(RDF.NAMESPACE),
			vf.createLiteral("xyz", vf.createIRI(RDF.NAMESPACE)),
			vf.createLiteral(NOW),
			vf.createLiteral(LocalDateTime.of(1990, 6, 20, 0, 0, 0, 20005000)),
			vf.createLiteral("13:03:22", XSD.TIME),
			vf.createLiteral(LocalTime.of(13, 3, 22, 40030000)),
			vf.createLiteral("1980-02-14", XSD.DATE),
			vf.createLiteral("foo", vf.createIRI("urn:bar:1")),
			vf.createLiteral("foo", "en-GB"),
			vf.createLiteral("bar", "zx-XY"),
			vf.createLiteral("漫画", "ja"),
			vf.createLiteral("POINT (139.81 35.6972)", GEO.WKT_LITERAL),
			vf.createLiteral("invalid still works (139.81 35.6972)", GEO.WKT_LITERAL),
			vf.createLiteral("<?xml version=\"1.0\" encoding=\"UTF-8\"?><test attr=\"foo\">bar</test>", RDF.XMLLITERAL),
			vf.createLiteral("<invalid xml still works", RDF.XMLLITERAL),
			vf.createLiteral("0000-06-20T00:00:00Z", XSD.DATETIME),
			vf.createIRI(HALYARD.VALUE_ID_NS.getName(), "eRg5UlsxjZuh-4meqlYQe3-J8X8"),
			vf.createIRI(WIKIDATA.WDV_NAMESPACE, "400f9abd3fd761c62af23dbe8f8432158a6ce272"),
			vf.createIRI(WIKIDATA.WDV_NAMESPACE, "invalid"),
			vf.createIRI(WIKIDATA.WDV_NAMESPACE+"400f9abd3fd761c62af23dbe8f8432158a6ce272/")
		);
	}

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> data() {
		List<Value> expected = createData(SimpleValueFactory.getInstance());
		List<Value> actual = createData(IdValueFactory.INSTANCE);
		List<Object[]> testValues = new ArrayList<>();
		for (int i=0; i<expected.size(); i++) {
			testValues.add(new Object[] {expected.get(i), actual.get(i)});
		}
		return testValues;
	}

	private Value expected;
	private IdentifiableValue actual;

	public IdValueFactoryExtendedTest(Value expected, IdentifiableValue actual) {
		this.expected = expected;
		this.actual = actual;
	}

	@Test
	public void testEquals() {
		assertEquals(expected, actual);
		assertEquals(actual, expected);
	}

	@Test
	public void testHashCode() {
		assertEquals(expected.hashCode(), actual.hashCode());
	}

	@Test
	public void testSerialize() throws IOException, ClassNotFoundException {
		ByteArrayOutputStream expectedOut = new ByteArrayOutputStream();
		try (ObjectOutputStream oos = new ObjectOutputStream(expectedOut)) {
			oos.writeObject(expected);
		}

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
			oos.writeObject(actual);
		}
		Value deser;
		try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(out.toByteArray()))) {
			deser = (Value) ois.readObject();
		}

		assertEquals(actual, deser);
		assertEquals(deser, actual);
		assertEquals(actual.hashCode(), deser.hashCode());

		assertEquals(expected, deser);
		assertEquals(deser, expected);
		assertEquals(expected.hashCode(), deser.hashCode());

		// should have better compression than the standard classes
		assertThat("Serialized size", out.toByteArray().length, lessThan(expectedOut.toByteArray().length));
	}

	@Test
	public void testId() {
		Configuration conf1 = new Configuration(false);
		conf1.setInt(Config.ID_SIZE, 8);
		RDFFactory rdfFactory1 = RDFFactory.create(conf1);
		assertEquals(rdfFactory1.id(expected), actual.getId(rdfFactory1));

		Configuration conf2 = new Configuration(false);
		conf2.setInt(Config.ID_SIZE, 10);
		RDFFactory rdfFactory2 = RDFFactory.create(conf2);
		assertEquals(rdfFactory2.id(expected), actual.getId(rdfFactory2));
	}

	@Test
	public void testSerializedForm() {
		Configuration conf1 = new Configuration(false);
		conf1.setInt(Config.ID_SIZE, 8);
		RDFFactory rdfFactory1 = RDFFactory.create(conf1);
		assertEquals(rdfFactory1.getSerializedForm(expected), ((SerializableValue)actual).getSerializedForm(rdfFactory1));

		Configuration conf2 = new Configuration(false);
		conf2.setInt(Config.ID_SIZE, 10);
		RDFFactory rdfFactory2 = RDFFactory.create(conf2);
		assertEquals(rdfFactory2.getSerializedForm(expected), ((SerializableValue)actual).getSerializedForm(rdfFactory2));
	}
}
