package com.msd.gin.halyard.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@RunWith(Parameterized.class)
public class IdValueFactoryTest {

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> data() {
		List<Value> expected = ValueIOTest.createData(SimpleValueFactory.getInstance());
		List<Value> actual = ValueIOTest.createData(IdValueFactory.getInstance());
		List<Object[]> testValues = new ArrayList<>();
		for (int i=0; i<expected.size(); i++) {
			testValues.add(new Object[] {expected.get(i), actual.get(i)});
		}
		return testValues;
	}

	private Value expected;
	private Value actual;

	public IdValueFactoryTest(Value expected, Value actual) {
		this.expected = expected;
		this.actual = actual;
	}

	@Test
	public void testEquals() {
		assertTrue(expected.equals(actual));
		assertTrue(actual.equals(expected));
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
		assertThat(out.toByteArray().length, lessThanOrEqualTo(expectedOut.toByteArray().length));
	}

	@Test
	public void testId() {
		if (actual instanceof Identifiable) {
			assertEquals(Identifier.id(expected), ((Identifiable)actual).getId());
		}
	}
}
