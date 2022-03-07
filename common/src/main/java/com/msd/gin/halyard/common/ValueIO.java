package com.msd.gin.halyard.common;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Date;
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
import org.eclipse.rdf4j.model.vocabulary.PROV;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.ROV;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.SKOS;
import org.eclipse.rdf4j.model.vocabulary.SKOSXL;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.model.vocabulary.WGS84;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ValueIO {
	static final BiMap<ByteBuffer, IRI> WELL_KNOWN_IRIS = HashBiMap.create(256);
	static final BiMap<ByteBuffer, IRI> WELL_KNOWN_IRI_IDS = HashBiMap.create(256);
	private static final BiMap<ByteBuffer, String> WELL_KNOWN_NAMESPACES = HashBiMap.create(256);

	private static void loadNamespacesAndIRIs(Class<?> vocab) {
		Collection<Namespace> namespaces = getNamespace(vocab);
		addNamespaces(namespaces);

		Collection<IRI> iris = Vocabularies.getIRIs(vocab);
		addIRIs(iris);

		try {
			Method getIRIs = vocab.getMethod("getIRIs");
			iris = (Collection<IRI>) getIRIs.invoke(null);
			addIRIs(iris);
		} catch (NoSuchMethodException e) {
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		} catch (InvocationTargetException e) {
			throw new RuntimeException(e.getCause());
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

	private static void addNamespaces(Collection<Namespace> namespaces) {
		for (Namespace namespace : namespaces) {
			String name = namespace.getName();
			ByteBuffer hash = ByteBuffer.wrap(Hashes.hash16(name.getBytes(StandardCharsets.UTF_8)));
			if (WELL_KNOWN_NAMESPACES.putIfAbsent(hash, name) != null) {
				throw new AssertionError(String.format("Hash collision between %s and %s",
						WELL_KNOWN_NAMESPACES.get(hash), name));
			}
		}
	}

	private static void addIRIs(Collection<IRI> iris) {
		for (IRI iri : iris) {
			IdentifiableIRI idIri = IdentifiableIRI.create(iri);
			byte[] id = idIri.getId();

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

	static {
		Class<?>[] defaultVocabs = { RDF.class, RDFS.class, XSD.class, SD.class, VOID.class, FOAF.class,
				OWL.class, DC.class, DCTERMS.class, SKOS.class, SKOSXL.class, ORG.class, GEO.class,
				WGS84.class, PROV.class, ROV.class };
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

	private static final Date GREGORIAN_ONLY = new Date(Long.MIN_VALUE);

	private static GregorianCalendar newGregorianCalendar() {
		GregorianCalendar cal = new GregorianCalendar();
		cal.setGregorianChange(GREGORIAN_ONLY);
		return cal;
	}

	private static final byte IRI_TYPE = '<';
	private static final byte IRI_HASH_TYPE = '#';
	private static final byte NAMESPACE_HASH_TYPE = ':';
	private static final byte BNODE_TYPE = '_';
	private static final byte DATATYPE_LITERAL_TYPE = '\"';
	private static final byte LANGUAGE_LITERAL_TYPE = '@';
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
				byte[] b = writeString(l.getLabel());
				byte[] zb = new byte[1+b.length];
				zb[0] = STRING_TYPE;
				System.arraycopy(b, 0, zb, 1, b.length);
				return zb;
			}
		});
		BYTE_READERS.put(STRING_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(readString(b));
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
				GregorianCalendar c = newGregorianCalendar();
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
				GregorianCalendar c = newGregorianCalendar();
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
				GregorianCalendar c = newGregorianCalendar();
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
					byte[] b = writeString(l.getLabel());
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
				int xmlContentType = b.get();
				if(xmlContentType == XML_TYPE) {
					// valid xml
					byte[] fiBytes = new byte[b.remaining()];
					b.get(fiBytes);
					return new XMLLiteral(fiBytes);
				} else {
					// invalid xml
					return vf.createLiteral(readString(b), RDF.XMLLITERAL);
				}
			}
		});
	}

	private static byte[] writeString(String s) {
		return s.getBytes(StandardCharsets.UTF_8);
	}

	private static String readString(ByteBuffer b) {
		return StandardCharsets.UTF_8.decode(b).toString();
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

	private static final ByteWriter DEFAULT_BYTE_WRITER = new ByteWriter() {
		@Override
		public byte[] writeBytes(Literal l) {
			byte[] labelBytes = writeString(l.getLabel());
			byte[] datatypeBytes = ValueIO.writeIRI(l.getDatatype());
			byte[] lBytes = new byte[3+labelBytes.length+datatypeBytes.length];
			ByteBuffer b = ByteBuffer.wrap(lBytes);
			b.put(DATATYPE_LITERAL_TYPE);
			b.putShort((short)datatypeBytes.length);
			b.put(datatypeBytes);
			b.put(labelBytes);
			return lBytes;
		}
	};

	public static byte[] writeBytes(Value v, TripleWriter tw) {
		if (v.isIRI()) {
			return writeIRI((IRI)v);
		} else if (v.isBNode()) {
			return writeBNode((BNode)v);
		} else if (v.isLiteral()) {
			return writeLiteral((Literal)v);
		} else if (v.isTriple()) {
			Triple t = (Triple) v;
			byte[] tripleBytes = tw.writeTriple(t.getSubject(), t.getPredicate(), t.getObject());
			byte[] b = new byte[1+tripleBytes.length];
			b[0] = TRIPLE_TYPE;
			System.arraycopy(tripleBytes, 0, b, 1, tripleBytes.length);
			return b;
		} else {
			throw new AssertionError(String.format("Unexpected RDF value: %s (%s)", v, v.getClass().getName()));
		}
    }

	private static byte[] writeIRI(IRI v) {
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
				byte[] localBytes = writeString(iri.getLocalName());
				byte[] b = new byte[1 + hash.remaining() + localBytes.length];
				b[0] = NAMESPACE_HASH_TYPE;
				// NB: do not alter original hash buffer which is shared across threads
				hash.duplicate().get(b, 1, hash.remaining());
				System.arraycopy(localBytes, 0, b, b.length-localBytes.length, localBytes.length);
				return b;
			} else {
				byte[] iriBytes = writeString(v.stringValue());
				byte[] b = new byte[1+iriBytes.length+1];
				b[0] = IRI_TYPE;
				System.arraycopy(iriBytes, 0, b, 1, iriBytes.length);
				b[1+iriBytes.length] = '>';
				return b;
			}
		}
    }

	private static byte[] writeBNode(BNode n) {
		byte[] idBytes = writeString(n.getID());
		byte[] b = new byte[1+idBytes.length];
		b[0] = BNODE_TYPE;
		System.arraycopy(idBytes, 0, b, 1, idBytes.length);
		return b;
	}

	private static byte[] writeLiteral(Literal l) {
		if(l.getLanguage().isPresent()) {
			byte[] labelBytes = writeString(l.getLabel());
			byte[] langBytes = writeString(l.getLanguage().get());
			byte[] b = new byte[2+langBytes.length+labelBytes.length];
			b[0] = LANGUAGE_LITERAL_TYPE;
			b[1] = (byte) langBytes.length;
			System.arraycopy(langBytes, 0, b, 2, langBytes.length);
			System.arraycopy(labelBytes, 0, b, langBytes.length+2, labelBytes.length);
			return b;
		} else {
			ByteWriter writer = BYTE_WRITERS.get(l.getDatatype());
			if (writer != null) {
				try {
					return writer.writeBytes(l);
				} catch (Exception e) {
					// if the dedicated writer fails then fallback to the generic writer
					return DEFAULT_BYTE_WRITER.writeBytes(l);
				}
			} else {
				return DEFAULT_BYTE_WRITER.writeBytes(l);
			}
		}
	}

	public static Value readValue(ByteBuffer b, ValueFactory vf, TripleFactory tf) throws IOException {
		final int originalLimit = b.limit();
		b.mark();
		byte type = b.get();
		switch(type) {
			case IRI_TYPE:
				b.limit(originalLimit-1); // ignore trailing '>'
				IRI iri = vf.createIRI(readString(b));
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
				return vf.createIRI(namespace, readString(b));
			case BNODE_TYPE:
				return vf.createBNode(readString(b));
			case LANGUAGE_LITERAL_TYPE:
				int langSize = b.get();
				b.limit(b.position()+langSize);
				String lang = readString(b);
				b.limit(originalLimit);
				String label = readString(b);
				return vf.createLiteral(label, lang);
			case DATATYPE_LITERAL_TYPE:
				int dtSize = b.getShort();
				b.limit(b.position()+dtSize);
				IRI datatype = (IRI) readValue(b, vf, null);
				b.limit(originalLimit);
				label = readString(b);
				return vf.createLiteral(label, datatype);
			case TRIPLE_TYPE:
				if (tf == null) {
					throw new IllegalStateException("Unexpected triple value or missing TripleFactory");
				}
				return tf.readTriple(b, vf);
			default:
				ByteReader reader = BYTE_READERS.get(type);
				if (reader == null) {
					throw new AssertionError(String.format("Unexpected type: %s", type));
				}
				return reader.readBytes((ByteBuffer) b, vf);
		}
    }
}
