package com.msd.gin.halyard.common;

import com.msd.gin.halyard.vocab.WIKIDATA;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class NamespaceTest {
    private static final ValueFactory vf = SimpleValueFactory.getInstance();

    private static Object[] createData(Namespace ns, String localName) {
    	return new Object[] {ns, vf.createIRI(ns.getName(), localName)};
    }

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> createData() {
		return Arrays.asList(
			createData(WIKIDATA.WD_NS, "Q51515413"),
			createData(WIKIDATA.WD_NS, "L252248-F2"),
			createData(WIKIDATA.WDS_NS, "Q78246295-047c20cf-4fd2-8173-f044-c04d3ec21f45"),
			createData(WIKIDATA.ORCID_NS, "0123-4567-8923-456X"),
			createData(WIKIDATA.OS_NS, "4000000074547913"),
			createData(WIKIDATA.PUBCHEM_CID_NS, "CID220848"),
			createData(WIKIDATA.PUBCHEM_SID_NS, "SID220848"),
			createData(WIKIDATA.MUSICBRAINZ_ARTIST_NS, "78b00a09-a941-413c-a917-691111608daa")
		);
    }

	private AbstractIRIEncodingNamespace ns;
	private IRI expected;

	public NamespaceTest(AbstractIRIEncodingNamespace ns, IRI expected) {
		this.ns = ns;
		this.expected = expected;
	}

	@Test
	public void testEncoding() {
		ByteBuffer b = ByteBuffer.allocate(0);
		b = ns.writeBytes(expected.getLocalName(), b);
		b.flip();
		String localName = ns.readBytes(b);
		IRI actual = vf.createIRI(ns.getName(), localName);
		assertEquals(expected, actual);
	}
}
