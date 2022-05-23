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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class RDFFactoryExtendedTest {

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		Configuration littleNibbleConf = new Configuration();
		littleNibbleConf.setInt(Config.ID_TYPE_INDEX, 0);
		littleNibbleConf.setBoolean(Config.ID_TYPE_NIBBLE, true);
		byte[] littleNibbleMaxTypeSalt = new byte[] {(byte) 0xF0};

		Configuration bigNibbleConf = new Configuration();
		bigNibbleConf.setInt(Config.ID_TYPE_INDEX, 1);
		bigNibbleConf.setBoolean(Config.ID_TYPE_NIBBLE, false);
		byte[] bigNibbleMaxTypeSalt = new byte[] {(byte) 0xFF, 0x00};

		return Arrays.<Object[]>asList(
			new Object[] {littleNibbleConf, littleNibbleMaxTypeSalt},
			new Object[] {bigNibbleConf, bigNibbleMaxTypeSalt}
		);
	}

	private final RDFFactory rdfFactory;
	private final byte[] maxTypeSalt;

	public RDFFactoryExtendedTest(Configuration conf, byte[] maxTypeSalt) {
		this.rdfFactory = RDFFactory.create(conf);
		this.maxTypeSalt = maxTypeSalt;
	}

	@Test
	public void testUniqueTypeFlags() throws IllegalAccessException {
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
	public void testTypeSalt() {
		Set<ByteBuffer> salts = new LinkedHashSet<>();
		for (int i=0; i<rdfFactory.typeSaltSize; i++) {
			byte[] salt = rdfFactory.createTypeSalt(i, rdfFactory.typeNibble.iriTypeBits);
			salts.add(ByteBuffer.wrap(salt));
			byte[] idBytes = new byte[rdfFactory.getIdSize()];
			System.arraycopy(salt, 0, idBytes, 0, salt.length);
			Identifier id = rdfFactory.id(idBytes);
			assertTrue(id.isIRI());
		}
		assertEquals(rdfFactory.typeSaltSize, salts.size());
	}

	@Test
	public void testMaxTypeSalt() {
		byte[] salt = rdfFactory.createTypeSalt(rdfFactory.typeSaltSize-1, (byte) 0x00);
		assertArrayEquals(maxTypeSalt, salt);
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
        Set<Identifier> ids = new HashSet<>(values.size());
        for (Value v : values) {
        	ids.add(rdfFactory.id(v));
        }
        assertEquals(values.size(), ids.size());
    }

	@Test
	public void testWellKnownId() {
		Identifier id = rdfFactory.wellKnownId(RDF.TYPE);
		assertNotNull(id);
		IRI typeIri = rdfFactory.getWellKnownIRI(id);
		assertEquals(RDF.TYPE, typeIri);
	}

	@Test
	public void testWellKnownIRI() {
		assertTrue(rdfFactory.isWellKnownIRI(RDF.TYPE));
	}
}
