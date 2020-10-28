package com.msd.gin.halyard.common;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.msd.gin.halyard.common.HalyardTableUtils.TripleFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeConstants;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.transform.TransformerException;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.util.Vocabularies;
import org.eclipse.rdf4j.model.vocabulary.DC;
import org.eclipse.rdf4j.model.vocabulary.DCTERMS;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.eclipse.rdf4j.model.vocabulary.ORG;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ValueIO {
	static final BiMap<ByteBuffer, IRI> WELL_KNOWN_IRIS = HashBiMap.create(256);
	static final BiMap<ByteBuffer, IRI> WELL_KNOWN_IRI_IDS = HashBiMap.create(256);
	private static final BiMap<ByteBuffer, String> WELL_KNOWN_NAMESPACES = HashBiMap.create(256);

	private static void loadNamespacesAndIRIs(Class<?> vocab) {
		Set<Namespace> namespaces = getNamespace(vocab);
		for (Namespace namespace : namespaces) {
			String name = namespace.getName();
			ByteBuffer hash = ByteBuffer.wrap(Hashes.hash16(name.getBytes(StandardCharsets.UTF_8)));
			if (WELL_KNOWN_NAMESPACES.putIfAbsent(hash, name) != null) {
				throw new AssertionError(String.format("Hash collision between %s and %s",
						WELL_KNOWN_NAMESPACES.get(hash), name));
			}
		}

		Set<IRI> iris = Vocabularies.getIRIs(vocab);
		for (IRI iri : iris) {
			byte[] id = Hashes.id(iri);
			IdentifiableIRI idIri = new IdentifiableIRI(id, iri);

			ByteBuffer idbb = ByteBuffer.wrap(id).asReadOnlyBuffer();
			if (WELL_KNOWN_IRI_IDS.putIfAbsent(idbb, idIri) != null) {
				throw new AssertionError(String.format("Hash collision between %s and %s",
						WELL_KNOWN_IRI_IDS.get(idbb), idIri));
			}

			ByteBuffer hash = ByteBuffer.wrap(Hashes.hash32(idIri.toString().getBytes(StandardCharsets.UTF_8))).asReadOnlyBuffer();
			if (WELL_KNOWN_IRIS.putIfAbsent(hash, idIri) != null) {
				throw new AssertionError(String.format("Hash collision between %s and %s",
						WELL_KNOWN_IRIS.get(hash), idIri));
			}
		}
	}

	private static Set<Namespace> getNamespace(Class<?> vocabulary) {
		Set<Namespace> namespaces = new HashSet<>();
		for (Field f : vocabulary.getFields()) {
			if (f.getType() == Namespace.class) {
				try {
					namespaces.add((Namespace) f.get(null));
				} catch (IllegalAccessException ex) {
					throw new AssertionError(ex);
				}
			}
		}
		return namespaces;
	}

	static {
		Class<?>[] defaultVocabs = { RDF.class, RDFS.class, XSD.class, SD.class, VOID.class, FOAF.class,
				OWL.class, DC.class, DCTERMS.class, ORG.class, GEO.class };
		for(Class<?> vocab : defaultVocabs) {
			loadNamespacesAndIRIs(vocab);
		}

		Logger logger = LoggerFactory.getLogger(HalyardTableUtils.class);
		logger.info("Searching for vocabularies...");
		for(Vocabulary vocab : ServiceLoader.load(Vocabulary.class)) {
			logger.info("Loading vocabulary {}", vocab.getClass());
			loadNamespacesAndIRIs(vocab.getClass());
		}
	}

	interface ByteWriter {
		byte[] writeBytes(Literal l);
	}

	interface ByteReader {
		Literal readBytes(ByteBuffer b, ValueFactory vf);
	}

	private static final Map<IRI, ByteWriter> BYTE_WRITERS = new HashMap<>(32);
	private static final Map<Byte, ByteReader> BYTE_READERS = new HashMap<>(32);

	private static final DatatypeFactory DATATYPE_FACTORY;
	static {
		try {
			DATATYPE_FACTORY = DatatypeFactory.newInstance();
		}
		catch (DatatypeConfigurationException e) {
			throw new AssertionError(e);
		}
	}

	private static final byte IRI_TYPE = '<';
	private static final byte IRI_HASH_TYPE = '#';
	private static final byte NAMESPACE_HASH_TYPE = ':';
	private static final byte BNODE_TYPE = '_';
	private static final byte FULL_LITERAL_TYPE = '\"';
	private static final byte LANGUAGE_TAG = '@';
	private static final byte TRIPLE_TYPE = '*';
	private static final byte FALSE_TYPE = '0';
	private static final byte TRUE_TYPE = '1';
	private static final byte BYTE_TYPE = 'b';
	private static final byte SHORT_TYPE = 's';
	private static final byte INT_TYPE = 'i';
	private static final byte LONG_TYPE = 'l';
	private static final byte FLOAT_TYPE = 'f';
	private static final byte DOUBLE_TYPE = 'd';
	private static final byte STRING_TYPE = 'z';
	private static final byte TIME_TYPE = 't';
	private static final byte DATE_TYPE = 'D';
	private static final byte DATETIME_TYPE = 'T';
	private static final byte XML_TYPE = 'x';

	static {
		BYTE_WRITERS.put(XSD.BOOLEAN, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				return new byte[] { l.booleanValue() ? TRUE_TYPE : FALSE_TYPE };
			}
		});
		BYTE_READERS.put(FALSE_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(false);
			}
		});
		BYTE_READERS.put(TRUE_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(true);
			}
		});

		BYTE_WRITERS.put(XSD.BYTE, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				return new byte[] { BYTE_TYPE, l.byteValue() };
			}
		});
		BYTE_READERS.put(BYTE_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(b.get());
			}
		});

		BYTE_WRITERS.put(XSD.SHORT, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				byte[] b = new byte[3];
				ByteBuffer.wrap(b).put(SHORT_TYPE).putShort(l.shortValue());
				return b;
			}
		});
		BYTE_READERS.put(SHORT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(b.getShort());
			}
		});

		BYTE_WRITERS.put(XSD.INT, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				byte[] b = new byte[5];
				ByteBuffer.wrap(b).put(INT_TYPE).putInt(l.intValue());
				return b;
			}
		});
		BYTE_READERS.put(INT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(b.getInt());
			}
		});

		BYTE_WRITERS.put(XSD.LONG, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				byte[] b = new byte[9];
				ByteBuffer.wrap(b).put(LONG_TYPE).putLong(l.longValue());
				return b;
			}
		});
		BYTE_READERS.put(LONG_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(b.getLong());
			}
		});

		BYTE_WRITERS.put(XSD.FLOAT, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				byte[] b = new byte[5];
				ByteBuffer.wrap(b).put(FLOAT_TYPE).putFloat(l.floatValue());
				return b;
			}
		});
		BYTE_READERS.put(FLOAT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(b.getFloat());
			}
		});

		BYTE_WRITERS.put(XSD.DOUBLE, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				byte[] b = new byte[9];
				ByteBuffer.wrap(b).put(DOUBLE_TYPE).putDouble(l.doubleValue());
				return b;
			}
		});
		BYTE_READERS.put(DOUBLE_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(b.getDouble());
			}
		});

		BYTE_WRITERS.put(XSD.STRING, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				byte[] b = l.getLabel().getBytes(StandardCharsets.UTF_8);
				byte[] zb = new byte[1+b.length];
				zb[0] = STRING_TYPE;
				System.arraycopy(b, 0, zb, 1, b.length);
				return zb;
			}
		});
		BYTE_READERS.put(STRING_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(StandardCharsets.UTF_8.decode(b).toString());
			}
		});

		BYTE_WRITERS.put(XSD.TIME, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				return calendarTypeToBytes(TIME_TYPE, l.calendarValue());
			}
		});
		BYTE_READERS.put(TIME_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				long millis = b.getLong();
				int tz = b.getShort();
				GregorianCalendar c = new GregorianCalendar();
				c.setTimeInMillis(millis);
				XMLGregorianCalendar cal = DATATYPE_FACTORY.newXMLGregorianCalendar(c);
				cal.setYear(null);
				cal.setMonth(DatatypeConstants.FIELD_UNDEFINED);
				cal.setDay(DatatypeConstants.FIELD_UNDEFINED);
				if(tz == Short.MIN_VALUE) {
					tz = DatatypeConstants.FIELD_UNDEFINED;
				}
				cal.setTimezone(tz);
				return vf.createLiteral(cal);
			}
		});

		BYTE_WRITERS.put(XSD.DATE, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				return calendarTypeToBytes(DATE_TYPE, l.calendarValue());
			}
		});
		BYTE_READERS.put(DATE_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				long millis = b.getLong();
				int tz = b.getShort();
				GregorianCalendar c = new GregorianCalendar();
				c.setTimeInMillis(millis);
				XMLGregorianCalendar cal = DATATYPE_FACTORY.newXMLGregorianCalendar(c);
				cal.setTime(DatatypeConstants.FIELD_UNDEFINED, DatatypeConstants.FIELD_UNDEFINED, DatatypeConstants.FIELD_UNDEFINED, null);
				if(tz == Short.MIN_VALUE) {
					tz = DatatypeConstants.FIELD_UNDEFINED;
				}
				cal.setTimezone(tz);
				return vf.createLiteral(cal);
			}
		});

		BYTE_WRITERS.put(XSD.DATETIME, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				return calendarTypeToBytes(DATETIME_TYPE, l.calendarValue());
			}
		});
		BYTE_READERS.put(DATETIME_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				long millis = b.getLong();
				int tz = b.getShort();
				GregorianCalendar c = new GregorianCalendar();
				c.setTimeInMillis(millis);
				XMLGregorianCalendar cal = DATATYPE_FACTORY.newXMLGregorianCalendar(c);
				if(tz == Short.MIN_VALUE) {
					tz = DatatypeConstants.FIELD_UNDEFINED;
				}
				cal.setTimezone(tz);
				return vf.createLiteral(cal);
			}
		});

		BYTE_WRITERS.put(RDF.XMLLITERAL, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				try {
					ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
					out.write(XML_TYPE);
					out.write(XML_TYPE); // mark xml as valid
					XMLLiteral.writeInfoset(l.getLabel(), out);
					return out.toByteArray();
				} catch(TransformerException e) {
					byte[] b = l.getLabel().getBytes(StandardCharsets.UTF_8);
					byte[] xzb = new byte[2+b.length];
					xzb[0] = XML_TYPE;
					xzb[1] = STRING_TYPE; // mark xml as invalid
					System.arraycopy(b, 0, xzb, 2, b.length);
					return xzb;
				}
			}
		});
		BYTE_READERS.put(XML_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				if(b.get() == XML_TYPE) {
					// valid xml
					byte[] fiBytes = new byte[b.remaining()];
					b.get(fiBytes);
					return new XMLLiteral(fiBytes);
				} else {
					// invalid xml
					return vf.createLiteral(StandardCharsets.UTF_8.decode(b).toString(), RDF.XMLLITERAL);
				}
			}
		});
	}

	private static byte[] calendarTypeToBytes(byte type, XMLGregorianCalendar cal) {
		byte[] b = new byte[11];
		ByteBuffer buf = ByteBuffer.wrap(b).put(type).putLong(cal.toGregorianCalendar().getTimeInMillis());
		if(cal.getTimezone() != DatatypeConstants.FIELD_UNDEFINED) {
			buf.putShort((short) cal.getTimezone());
		} else {
			buf.putShort(Short.MIN_VALUE);
		}
		return b;
	}

	public static byte[] writeBytes(Value v) {
    	if (v instanceof IRI) {
    		ByteBuffer hash = WELL_KNOWN_IRIS.inverse().get(v);
    		if (hash != null) {
    			byte[] b = new byte[1 + hash.remaining()];
    			b[0] = IRI_HASH_TYPE;
    			// NB: do not alter original hash buffer which is shared across threads
    			hash.duplicate().get(b, 1, hash.remaining());
    			return b;
    		} else {
    			IRI iri = (IRI) v;
    			hash = WELL_KNOWN_NAMESPACES.inverse().get(iri.getNamespace());
    			if (hash != null) {
    				byte[] localBytes = iri.getLocalName().getBytes(StandardCharsets.UTF_8);
    				byte[] b = new byte[1 + hash.remaining() + localBytes.length];
    				b[0] = NAMESPACE_HASH_TYPE;
        			// NB: do not alter original hash buffer which is shared across threads
        			hash.duplicate().get(b, 1, hash.remaining());
        			System.arraycopy(localBytes, 0, b, b.length-localBytes.length, localBytes.length);
        			return b;
    			} else {
    				return ("<"+v.stringValue()+">").getBytes(StandardCharsets.UTF_8);
    			}
    		}
    	} else if (v instanceof BNode) {
    		return v.toString().getBytes(StandardCharsets.UTF_8);
    	} else if (v instanceof Literal) {
			Literal l = (Literal) v;
			if(l.getLanguage().isPresent()) {
				return l.toString().getBytes(StandardCharsets.UTF_8);
			} else {
				ByteWriter writer = BYTE_WRITERS.get(l.getDatatype());
				if (writer != null) {
					return writer.writeBytes(l);
				} else {
					byte[] labelBytes = l.getLabel().getBytes(StandardCharsets.UTF_8);
					byte[] datatypeBytes = writeBytes(l.getDatatype());
					byte[] lBytes = new byte[1+labelBytes.length+1+datatypeBytes.length];
					lBytes[0] = '\"';
					System.arraycopy(labelBytes, 0, lBytes, 1, labelBytes.length);
					int pos = 1+labelBytes.length;
					lBytes[pos++] = '\"';
					System.arraycopy(datatypeBytes, 0, lBytes, lBytes.length-datatypeBytes.length, datatypeBytes.length);
					return lBytes;
				}
			}
    	} else if (v instanceof Triple) {
    		byte[] b = new byte[1+3*Hashes.ID_SIZE];
    		b[0] = TRIPLE_TYPE;
    		Triple t = (Triple) v;
    		writeTripleIdentifier(t.getSubject(), t.getPredicate(), t.getObject(), b, 1);
    		return b;
		} else {
			throw new AssertionError(String.format("Unexpected RDF value: %s (%s)", v, v.getClass().getName()));
		}
    }

    public static void writeTripleIdentifier(Resource subj, IRI pred, Value obj, byte[] b, int offset) {
    	int i = offset;
		byte[] sid = Hashes.id(subj);
		System.arraycopy(sid, 0, b, i, Hashes.ID_SIZE);
		i += Hashes.ID_SIZE;
		byte[] pid = Hashes.id(pred);
		System.arraycopy(pid, 0, b, i, Hashes.ID_SIZE);
		i += Hashes.ID_SIZE;
		byte[] oid = Hashes.id(obj);
		System.arraycopy(oid, 0, b, i, Hashes.ID_SIZE);
    }

    public static Value readValue(ByteBuffer b, ValueFactory vf, TripleFactory tf) throws IOException {
    	int originalLimit = b.limit();
		b.mark();
		byte type = b.get();
		switch(type) {
			case IRI_TYPE:
				b.limit(originalLimit-1); // ignore trailing '>'
				IRI iri = vf.createIRI(StandardCharsets.UTF_8.decode(b).toString());
				b.limit(originalLimit);
				b.position(originalLimit);
				return iri;
			case IRI_HASH_TYPE:
				iri = WELL_KNOWN_IRIS.get(b);
				if (iri == null) {
					throw new IllegalStateException(String.format("Unknown IRI hash: %s", Hashes.encode(b)));
				}
				b.position(originalLimit);
				return iri;
			case NAMESPACE_HASH_TYPE:
				b.limit(b.position()+2); // 16-byte hash
				String namespace = WELL_KNOWN_NAMESPACES.get(b);
				if (namespace == null) {
					throw new IllegalStateException(String.format("Unknown namespace hash: %s", Hashes.encode(b)));
				}
				b.limit(originalLimit);
				b.position(b.position()+2);
				return vf.createIRI(namespace, StandardCharsets.UTF_8.decode(b).toString());
			case BNODE_TYPE:
				b.get(); // skip ':'
				return vf.createBNode(StandardCharsets.UTF_8.decode(b).toString());
			case FULL_LITERAL_TYPE:
				int endOfLabel = lastIndexOf(b, (byte) '\"');
				b.limit(endOfLabel);
				String label = StandardCharsets.UTF_8.decode(b).toString();
				b.limit(originalLimit);
				b.position(endOfLabel+1);
				byte sep = b.get(endOfLabel+1); // peak
				if(sep == LANGUAGE_TAG) { // lang tag
					b.position(b.position()+1);
					return vf.createLiteral(label, StandardCharsets.UTF_8.decode(b).toString());
				} else {
					IRI datatype = (IRI) readValue(b, vf, null);
					return vf.createLiteral(label, datatype);
				}
			case TRIPLE_TYPE:
				if (tf == null) {
					throw new IllegalStateException("Unexpected triple value or missing TripleFactory");
				}
				byte[] sid = new byte[Hashes.ID_SIZE];
				byte[] pid = new byte[Hashes.ID_SIZE];
				byte[] oid = new byte[Hashes.ID_SIZE];
				b.get(sid).get(pid).get(oid);
				return tf.readTriple(sid, pid, oid, vf);
			default:
				ByteReader reader = BYTE_READERS.get(type);
				if (reader == null) {
					throw new AssertionError(String.format("Unexpected type: %s", type));
				}
				return reader.readBytes((ByteBuffer) b, vf);
		}
    }

	private static int lastIndexOf(ByteBuffer buf, byte b) {
		int start = buf.position();
		int end = buf.limit();
		for (int i = end - 1; i >= start; i--) {
			if (buf.get(i) == b) {
				return i;
			}
		}
		return -1;
	}
}
