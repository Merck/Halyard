package com.msd.gin.halyard.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.lessThan;

@RunWith(Parameterized.class)
public class IdValueFactoryExtendedTest {
	private static final IdentifiableValueIO valueIO = IdentifiableValueIO.create();
	private static final Date NOW = new Date();

	private static List<Value> createData(ValueFactory vf) {
		return Arrays.asList(
			vf.createLiteral("foo"),
			vf.createBNode("__foobar__"),
			vf.createIRI("test:/foo"),
			vf.createIRI("http://www.testmyiri.com"),
			vf.createIRI("https://www.testmyiri.com"),
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
			vf.createIRI(RDF.NAMESPACE),
			vf.createLiteral("xyz", vf.createIRI(RDF.NAMESPACE)),
			vf.createLiteral(NOW),
			vf.createLiteral("13:03:22.000", XSD.TIME),
			vf.createLiteral("1980-02-14", XSD.DATE),
			vf.createLiteral("foo", vf.createIRI("urn:bar:1")),
			vf.createLiteral("foo", "en-GB"),
			vf.createLiteral("bar", "zx-XY"),
			vf.createLiteral("<?xml version=\"1.0\" encoding=\"UTF-8\"?><test attr=\"foo\">bar</test>", RDF.XMLLITERAL),
			vf.createLiteral("invalid xml still works", RDF.XMLLITERAL),
			vf.createLiteral("0000-06-20T00:00:00Z", XSD.DATETIME));
	}

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> data() {
		List<Value> expected = createData(SimpleValueFactory.getInstance());
		List<Value> actual = createData(new IdValueFactory(valueIO));
		List<Object[]> testValues = new ArrayList<>();
		for (int i=0; i<expected.size(); i++) {
			testValues.add(new Object[] {expected.get(i), actual.get(i)});
		}
		return testValues;
	}

	private Value expected;
	private Value actual;

	public IdValueFactoryExtendedTest(Value expected, Value actual) {
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
}
