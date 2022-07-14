package com.msd.gin.halyard.common;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;

/**
 * Test different RDFFactory configurations.
 */
@RunWith(Parameterized.class)
public class RDFFactoryExtendedTest {

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		Configuration littleNibbleConf = new Configuration();
		littleNibbleConf.setInt(Config.ID_TYPE_INDEX, 0);
		littleNibbleConf.setBoolean(Config.ID_TYPE_NIBBLE, true);

		Configuration bigNibbleConf = new Configuration();
		bigNibbleConf.setInt(Config.ID_TYPE_INDEX, 1);
		bigNibbleConf.setBoolean(Config.ID_TYPE_NIBBLE, false);

		return Arrays.<Object[]>asList(
			new Object[] {littleNibbleConf},
			new Object[] {bigNibbleConf}
		);
	}

	private final Configuration conf;
	private final RDFFactory rdfFactory;

	public RDFFactoryExtendedTest(Configuration conf) {
		this.conf = conf;
		this.rdfFactory = RDFFactory.create(conf);
	}

	@Test
	public void testUniqueEncodingTypeFlags() throws IllegalAccessException {
		Map<Byte,String> flags = new HashMap<>();
		for (Field f : ValueIO.class.getDeclaredFields()) {
			f.setAccessible(true);
			String fName = f.getName();
			if (fName.endsWith("_TYPE")) {
				byte flag = f.getByte(null);
				String oldName = flags.put(flag, fName);
				if (oldName != null) {
					throw new AssertionError(String.format("%s: %c already used by %s", fName, flag, oldName));
				}
			}
		}
	}

	@Test
	public void testWriteSaltAndTypeIRI() {
		Set<ByteBuffer> salts = new LinkedHashSet<>();
		for (int i=0; i<rdfFactory.typeSaltSize; i++) {
			byte[] idBytes = new byte[rdfFactory.getIdSize()];
			ByteBuffer bb = ByteBuffer.wrap(idBytes);
			rdfFactory.writeSaltAndType(i, ValueType.IRI, null, new ByteFiller((byte)0xFF, rdfFactory.getIdSize())).writeTo(bb);
			assertFalse(bb.hasRemaining());
			bb.flip();
			salts.add(bb);
			ValueIdentifier id = rdfFactory.id(idBytes);
			assertTrue(id.isIRI());
		}
		assertEquals(rdfFactory.typeSaltSize, salts.size());
	}

	@Test
	public void testWriteSaltAndTypeStringLiteral() {
		Set<ByteBuffer> salts = new LinkedHashSet<>();
		for (int i=0; i<rdfFactory.typeSaltSize; i++) {
			byte[] idBytes = new byte[rdfFactory.getIdSize()];
			ByteBuffer bb = ByteBuffer.wrap(idBytes);
			rdfFactory.writeSaltAndType(i, ValueType.LITERAL, XSD.STRING, new ByteFiller((byte)0xFF, rdfFactory.getIdSize())).writeTo(bb);
			assertFalse(bb.hasRemaining());
			bb.flip();
			salts.add(bb);
			ValueIdentifier id = rdfFactory.id(idBytes);
			assertTrue(id.isLiteral());
			assertTrue(id.isString());
		}
		assertEquals(rdfFactory.typeSaltSize, salts.size());
	}

	@Test
	public void testIdIsEqual() {
        ValueFactory vf = SimpleValueFactory.getInstance();
        assertEquals(
	    	rdfFactory.id(vf.createLiteral("1", vf.createIRI("local:type"))),
	    	rdfFactory.id(vf.createLiteral("1", vf.createIRI("local:type")))
	    );
	}

	@Test
    public void testIdIsUnique() {
        ValueFactory vf = SimpleValueFactory.getInstance();
        List<Value> values = RDFFactoryTest.createData(vf).stream().map(arr -> (Value)arr[0]).collect(Collectors.toList());
        Set<ValueIdentifier> ids = new HashSet<>(values.size());
        for (Value v : values) {
        	ids.add(rdfFactory.id(v));
        }
        assertEquals(values.size(), ids.size());
    }

	@Test
	public void testWellKnownIRI() {
		assertTrue(rdfFactory.isWellKnownIRI(RDF.TYPE));
		ValueIdentifier id = rdfFactory.id(RDF.TYPE);
		IRI typeIri = rdfFactory.getWellKnownIRI(id);
		assertEquals(RDF.TYPE, typeIri);
	}

	@Test
	public void testIsSingleton() {
		Configuration conf2 = new Configuration(conf);
		RDFFactory rdfFactory2 = RDFFactory.create(conf2);
		assertTrue(rdfFactory == rdfFactory2);
	}
}
