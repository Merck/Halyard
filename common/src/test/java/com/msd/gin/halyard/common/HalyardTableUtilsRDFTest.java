package com.msd.gin.halyard.common;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
				{ vf.createIRI(RDF.NAMESPACE) }, { vf.createLiteral(new Date()) } });
	}

	private Value expected;

	public HalyardTableUtilsRDFTest(Value v) {
		this.expected = v;
	}

	@Test
	public void testToAndFromBytes() {
		byte[] b = HalyardTableUtils.writeBytes(expected);
		Value actual = HalyardTableUtils.readValue(b, vf);
		assertEquals(expected, actual);
		if (expected instanceof Resource) {
			Resource actualResource = HalyardTableUtils.readResource(b, vf);
			assertEquals(expected, actualResource);
		}
		if (expected instanceof IRI) {
			Resource actualIRI = HalyardTableUtils.readIRI(b, vf);
			assertEquals(expected, actualIRI);
		}
	}
}
