package com.msd.gin.halyard.common;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.BiFunction;

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
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
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
	public static final ValueIO.Writer CELL_WRITER = new ValueIO.Writer(new CellTripleWriter());
	public static final ValueIO.Writer STREAM_WRITER = new ValueIO.Writer(new StreamTripleWriter());
	public static final ValueIO.Reader STREAM_READER = new ValueIO.Reader(IdValueFactory.getInstance(),  new StreamTripleReader());
	public static final ValueIO.Reader SIMPLE_READER = new ValueIO.Reader(SimpleValueFactory.getInstance(), null);

	static final int DEFAULT_BUFFER_SIZE = 128;
	static final BiMap<Identifier, IRI> WELL_KNOWN_IRI_IDS = HashBiMap.create(256);
	private static final BiMap<Integer, IRI> WELL_KNOWN_IRIS = HashBiMap.create(1024);
	private static final BiMap<Short, String> WELL_KNOWN_NAMESPACES = HashBiMap.create(256);
	private static final BiMap<Short, String> WELL_KNOWN_LANGS = HashBiMap.create(256);
	private static final int IRI_HASH_SIZE = 4;
	private static final int NAMESPACE_HASH_SIZE = 2;
	private static final int LANG_HASH_SIZE = 2;

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
			Short hash = Hashes.hash16(name.getBytes(StandardCharsets.UTF_8));
			if (WELL_KNOWN_NAMESPACES.putIfAbsent(hash, name) != null) {
				throw new AssertionError(String.format("Hash collision between %s and %s",
						WELL_KNOWN_NAMESPACES.get(hash), name));
			}
		}
	}

	private static void addIRIs(Collection<IRI> iris) {
		for (IRI iri : iris) {
			IdentifiableIRI idIri = new IdentifiableIRI(iri);
			Identifier id = idIri.getId();
			if (WELL_KNOWN_IRI_IDS.putIfAbsent(id, idIri) != null) {
				throw new AssertionError(String.format("Hash collision between %s and %s",
						WELL_KNOWN_IRI_IDS.get(id), idIri));
			}

			Integer hash = Hashes.hash32(idIri.toString().getBytes(StandardCharsets.UTF_8));
			if (WELL_KNOWN_IRIS.putIfAbsent(hash, idIri) != null) {
				throw new AssertionError(String.format("Hash collision between %s and %s",
						WELL_KNOWN_IRIS.get(hash), idIri));
			}
		}
	}

	private static void loadLanguageTags() {
		for (Locale l : Locale.getAvailableLocales()) {
			String langTag = l.toLanguageTag();
			Short hash = Hashes.hash16(langTag.getBytes(StandardCharsets.UTF_8));
			if (WELL_KNOWN_LANGS.putIfAbsent(hash, langTag) != null) {
				throw new AssertionError(String.format("Hash collision between %s and %s",
						WELL_KNOWN_LANGS.get(hash), langTag));
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

		loadLanguageTags();
	}

	static boolean isWellKnownIRI(Value v) {
		return WELL_KNOWN_IRIS.containsValue(v);
	}

	interface ByteWriter {
		ByteBuffer writeBytes(Literal l, ByteBuffer b);
	}

	interface ByteReader {
		Literal readBytes(ByteBuffer b, ValueFactory vf);
	}

	private static final Map<IRI, ByteWriter> BYTE_WRITERS = new HashMap<>(32);
	private static final Map<Integer, ByteReader> BYTE_READERS = new HashMap<>(32);

	private static void addByteWriter(IRI datatype, ByteWriter bw) {
		if (BYTE_WRITERS.putIfAbsent(datatype, bw) != null) {
			throw new AssertionError(String.format("%s already exists for %s", ByteWriter.class.getSimpleName(), datatype));
		}
	}

	private static void addByteReader(int valueType, ByteReader br) {
		if (BYTE_READERS.putIfAbsent(valueType, br) != null) {
			throw new AssertionError(String.format("%s already exists for %s", ByteReader.class.getSimpleName(), (char)valueType));
		}
	}

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
	private static final byte LANGUAGE_HASH_LITERAL_TYPE = 'a';
	private static final byte BYTE_TYPE = 'b';
	private static final byte SHORT_TYPE = 's';
	private static final byte INT_TYPE = 'i';
	private static final byte LONG_TYPE = 'l';
	private static final byte FLOAT_TYPE = 'f';
	private static final byte DOUBLE_TYPE = 'd';
	private static final byte STRING_TYPE = 'z';
	private static final byte TIME_TYPE = 't';
	private static final byte DATE_TYPE = 'D';
	private static final byte BIG_FLOAT_TYPE = 'F';
	private static final byte BIG_INT_TYPE = 'I';
	private static final byte DATETIME_TYPE = 'T';
	private static final byte XML_TYPE = 'x';

	private static final ByteWriter DEFAULT_BYTE_WRITER = new ByteWriter() {
		@Override
		public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
			b = ensureCapacity(b, 1+2);
			b.put(DATATYPE_LITERAL_TYPE);
			int sizePos = b.position();
			int startPos = b.position()+2;
			b.position(startPos);
			b = ValueIO.Writer.writeIRI(l.getDatatype(), b);
			int endPos = b.position();
			b.position(sizePos);
			b.putShort((short) (endPos-startPos));
			b.position(endPos);
			ByteBuffer labelBytes = writeString(l.getLabel());
			b = ensureCapacity(b, labelBytes.remaining());
			b.put(labelBytes);
			return b;
		}
	};

	static {
		addByteWriter(XSD.BOOLEAN, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				b = ensureCapacity(b, 1);
				return b.put(l.booleanValue() ? TRUE_TYPE : FALSE_TYPE);
			}
		});
		addByteReader(FALSE_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(false);
			}
		});
		addByteReader(TRUE_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(true);
			}
		});

		addByteWriter(XSD.BYTE, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				b = ensureCapacity(b, 2);
				return b.put(BYTE_TYPE).put(l.byteValue());
			}
		});
		addByteReader(BYTE_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(b.get());
			}
		});

		addByteWriter(XSD.SHORT, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				b = ensureCapacity(b, 3);
				return b.put(SHORT_TYPE).putShort(l.shortValue());
			}
		});
		addByteReader(SHORT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(b.getShort());
			}
		});

		addByteWriter(XSD.INT, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				b = ensureCapacity(b, 5);
				return b.put(INT_TYPE).putInt(l.intValue());
			}
		});
		addByteReader(INT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(b.getInt());
			}
		});

		addByteWriter(XSD.LONG, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				b = ensureCapacity(b, 9);
				return b.put(LONG_TYPE).putLong(l.longValue());
			}
		});
		addByteReader(LONG_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(b.getLong());
			}
		});

		addByteWriter(XSD.FLOAT, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				b = ensureCapacity(b, 5);
				return b.put(FLOAT_TYPE).putFloat(l.floatValue());
			}
		});
		addByteReader(FLOAT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(b.getFloat());
			}
		});

		addByteWriter(XSD.DOUBLE, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				b = ensureCapacity(b, 9);
				return b.put(DOUBLE_TYPE).putDouble(l.doubleValue());
			}
		});
		addByteReader(DOUBLE_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(b.getDouble());
			}
		});

		addByteWriter(XSD.INTEGER, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				byte[] bytes = l.integerValue().toByteArray();
				b = ensureCapacity(b, 1 + bytes.length);
				return b.put(BIG_INT_TYPE).put(bytes);
			}
		});
		addByteReader(BIG_INT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				byte[] bytes = new byte[b.remaining()];
				b.get(bytes);
				return vf.createLiteral(new BigInteger(bytes));
			}
		});

		addByteWriter(XSD.DECIMAL, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				BigDecimal x = l.decimalValue();
				byte[] bytes = x.unscaledValue().toByteArray();
				int scale = x.scale();
				b = ensureCapacity(b, 1 + bytes.length + 4);
				return b.put(BIG_FLOAT_TYPE).putInt(scale).put(bytes);
			}
		});
		addByteReader(BIG_FLOAT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				int scale = b.getInt();
				byte[] bytes = new byte[b.remaining()];
				b.get(bytes);
				return vf.createLiteral(new BigDecimal(new BigInteger(bytes), scale));
			}
		});

		addByteWriter(XSD.STRING, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				ByteBuffer zb = writeString(l.getLabel());
				b = ensureCapacity(b, 1 + zb.remaining());
				return b.put(STRING_TYPE).put(zb);
			}
		});
		addByteReader(STRING_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(readString(b));
			}
		});

		addByteWriter(XSD.TIME, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				return calendarTypeToBytes(TIME_TYPE, l.calendarValue(), b);
			}
		});
		addByteReader(TIME_TYPE, new ByteReader() {
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

		addByteWriter(XSD.DATE, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				return calendarTypeToBytes(DATE_TYPE, l.calendarValue(), b);
			}
		});
		addByteReader(DATE_TYPE, new ByteReader() {
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

		addByteWriter(XSD.DATETIME, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				return calendarTypeToBytes(DATETIME_TYPE, l.calendarValue(), b);
			}
		});
		addByteReader(DATETIME_TYPE, new ByteReader() {
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

		addByteWriter(RDF.XMLLITERAL, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				try {
					ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
					XMLLiteral.writeInfoset(l.getLabel(), out);
					byte[] xb = out.toByteArray();
					b = ensureCapacity(b, 2 + xb.length);
					b.put(XML_TYPE);
					b.put(XML_TYPE); // mark xml as valid
					return b.put(xb);
				} catch(TransformerException e) {
					ByteBuffer zb = writeString(l.getLabel());
					b = ensureCapacity(b, 2 + zb.remaining());
					b.put(XML_TYPE);
					b.put(STRING_TYPE); // mark xml as invalid
					return b.put(zb);
				}
			}
		});
		addByteReader(XML_TYPE, new ByteReader() {
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

	private static ByteBuffer calendarTypeToBytes(byte type, XMLGregorianCalendar cal, ByteBuffer b) {
		b = ensureCapacity(b, 11);
		b.put(type).putLong(cal.toGregorianCalendar().getTimeInMillis());
		if(cal.getTimezone() != DatatypeConstants.FIELD_UNDEFINED) {
			b.putShort((short) cal.getTimezone());
		} else {
			b.putShort(Short.MIN_VALUE);
		}
		return b;
	}

	private static ByteBuffer writeString(String s) {
		return StandardCharsets.UTF_8.encode(s);
	}

	private static String readString(ByteBuffer b) {
		return StandardCharsets.UTF_8.decode(b).toString();
	}

	public static ByteBuffer ensureCapacity(ByteBuffer b, int requiredSize) {
		if (b.remaining() < requiredSize) {
			// leave some spare capacity
			ByteBuffer newb = ByteBuffer.allocate(3*b.capacity()/2 + 2*requiredSize);
			b.flip();
			newb.put(b);
			return newb;
		} else {
			return b;
		}
	}

	public static ByteBuffer writeValue(Value v, ValueIO.Writer writer, ByteBuffer buf, int sizeBytes) {
		buf = ValueIO.ensureCapacity(buf, sizeBytes);
		int sizePos = buf.position();
		int startPos = buf.position() + sizeBytes;
		buf.position(startPos);
		buf = writer.writeTo(v, buf);
		int endPos = buf.position();
		int len = endPos - startPos;
		buf.position(sizePos);
		if (sizeBytes == 2) {
			buf.putShort((short) len);
		} else if (sizeBytes == 4) {
			buf.putInt(len);
		} else {
			throw new AssertionError();
		}
		buf.position(endPos);
		return buf;
	}

	public static Value readValue(ByteBuffer buf, ValueIO.Reader reader, int sizeBytes) {
		int len;
		if (sizeBytes == 2) {
			len = buf.getShort();
		} else if (sizeBytes == 4) {
			len = buf.getInt();
		} else {
			throw new AssertionError();
		}
		int originalLimit = buf.limit();
		buf.limit(buf.position() + len);
		Value v = reader.readValue(buf);
		buf.limit(originalLimit);
		return v;
	}

	public static byte[] statementId(Resource subj, IRI pred, Value obj) {
		byte[] id = new byte[3 * Identifier.ID_SIZE];
		ByteBuffer buf = ByteBuffer.wrap(id);
		buf = ValueIO.writeStatementId(subj, pred, obj, buf);
		buf.flip();
		buf.get(id);
		return id;
	}

	public static ByteBuffer writeStatementId(Resource subj, IRI pred, Value obj, ByteBuffer buf) {
		buf = ensureCapacity(buf, 3*Identifier.ID_SIZE);
		Identifier.id(subj).writeTo(buf);
		Identifier.id(pred).writeTo(buf);
		Identifier.id(obj).writeTo(buf);
		return buf;
	}

	public static final class Writer {
		private final TripleWriter tw;

		public Writer(TripleWriter tw) {
			this.tw = tw;
		}

		public ByteBuffer toBytes(Value v) {
			ByteBuffer tmp = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
			tmp = writeTo(v, tmp);
			tmp.flip();
			byte[] b = new byte[tmp.remaining()];
			tmp.get(b);
			return ByteBuffer.wrap(b).asReadOnlyBuffer();
		}

		public ByteBuffer writeTo(Value v, ByteBuffer b) {
			if (v.isIRI()) {
				return writeIRI((IRI)v, b);
			} else if (v.isBNode()) {
				return writeBNode((BNode)v, b);
			} else if (v.isLiteral()) {
				return writeLiteral((Literal)v, b);
			} else if (v.isTriple()) {
				Triple t = (Triple) v;
				b = ensureCapacity(b, 1);
				b.put(TRIPLE_TYPE);
				b = tw.writeTriple(t.getSubject(), t.getPredicate(), t.getObject(), this, b);
				return b;
			} else {
				throw new AssertionError(String.format("Unexpected RDF value: %s (%s)", v, v.getClass().getName()));
			}
		}

		private static ByteBuffer writeIRI(IRI v, ByteBuffer b) {
			Integer irihash = WELL_KNOWN_IRIS.inverse().get(v);
			if (irihash != null) {
				b = ensureCapacity(b, 1 + IRI_HASH_SIZE);
				b.put(IRI_HASH_TYPE);
				b.putInt(irihash);
				return b;
			} else {
				IRI iri = v;
				Short nshash = WELL_KNOWN_NAMESPACES.inverse().get(iri.getNamespace());
				if (nshash != null) {
					ByteBuffer localBytes = writeString(iri.getLocalName());
					b = ensureCapacity(b, 1 + NAMESPACE_HASH_SIZE + localBytes.remaining());
					b.put(NAMESPACE_HASH_TYPE);
					b.putShort(nshash);
					b.put(localBytes);
					return b;
				} else {
					ByteBuffer iriBytes = writeString(v.stringValue());
					b = ensureCapacity(b, 1+iriBytes.remaining());
					b.put(IRI_TYPE);
					b.put(iriBytes);
					return b;
				}
			}
		}

		private static ByteBuffer writeBNode(BNode n, ByteBuffer b) {
			ByteBuffer idBytes = writeString(n.getID());
			b = ensureCapacity(b, 1+idBytes.remaining());
			b.put(BNODE_TYPE);
			b.put(idBytes);
			return b;
		}

		private static ByteBuffer writeLiteral(Literal l, ByteBuffer b) {
			if(l.getLanguage().isPresent()) {
				ByteBuffer labelBytes = writeString(l.getLabel());
				String langTag = l.getLanguage().get();
				Short hash = WELL_KNOWN_LANGS.inverse().get(langTag);
				if (hash != null) {
					b = ensureCapacity(b, 1+LANG_HASH_SIZE+labelBytes.remaining());
					b.put(LANGUAGE_HASH_LITERAL_TYPE);
					b.putShort(hash);
				} else {
					if (langTag.length() > Short.MAX_VALUE) {
						int truncatePos = langTag.lastIndexOf('-', Short.MAX_VALUE-1);
						// check for single tag
						if (langTag.charAt(truncatePos-2) == '-') {
							truncatePos -= 2;
						}
						langTag = langTag.substring(0, truncatePos);
					}
					ByteBuffer langBytes = writeString(langTag);
					b = ensureCapacity(b, 2+langBytes.remaining()+labelBytes.remaining());
					b.put(LANGUAGE_LITERAL_TYPE);
					b.put((byte) langBytes.remaining());
					b.put(langBytes);
				}
				b.put(labelBytes);
				return b;
			} else {
				ByteWriter writer = BYTE_WRITERS.get(l.getDatatype());
				if (writer != null) {
					try {
						return writer.writeBytes(l, b);
					} catch (Exception e) {
						// if the dedicated writer fails then fallback to the generic writer
						return DEFAULT_BYTE_WRITER.writeBytes(l, b);
					}
				} else {
					return DEFAULT_BYTE_WRITER.writeBytes(l, b);
				}
			}
		}
	}


	public static final class Reader {
		private final ValueFactory vf;
		private final TripleReader tf;
		private final BiFunction<String,ValueFactory,Resource> bnodeTransformer;

		public Reader(ValueFactory vf, TripleReader tf) {
			this(vf, tf, (id,valueFactory) -> valueFactory.createBNode(id));
		}

		public Reader(ValueFactory vf, TripleReader tf, BiFunction<String,ValueFactory,Resource> bnodeTransformer) {
			this.vf = vf;
			this.tf = tf;
			this.bnodeTransformer = bnodeTransformer;
		}

		public ValueFactory getValueFactory() {
			return vf;
		}

		public Value readValue(ByteBuffer b) {
			final int originalLimit = b.limit();
			int type = b.get();
			switch(type) {
				case IRI_TYPE:
					IRI iri = vf.createIRI(readString(b));
					b.position(originalLimit);
					return iri;
				case IRI_HASH_TYPE:
					b.mark();
					Integer irihash = b.getInt(); // 32-bit hash
					iri = WELL_KNOWN_IRIS.get(irihash);
					if (iri == null) {
						b.limit(b.position()).reset();
						throw new IllegalStateException(String.format("Unknown IRI hash: %s", Hashes.encode(b)));
					}
					return iri;
				case NAMESPACE_HASH_TYPE:
					b.mark();
					Short nshash = b.getShort(); // 16-bit hash
					String namespace = WELL_KNOWN_NAMESPACES.get(nshash);
					if (namespace == null) {
						b.limit(b.position()).reset();
						throw new IllegalStateException(String.format("Unknown namespace hash: %s", Hashes.encode(b)));
					}
					return vf.createIRI(namespace, readString(b));
				case BNODE_TYPE:
					return bnodeTransformer.apply(readString(b), vf);
				case LANGUAGE_HASH_LITERAL_TYPE:
					b.mark();
					Short langHash = b.getShort(); // 16-bit hash
					String lang = WELL_KNOWN_LANGS.get(langHash);
					if (lang == null) {
						b.limit(b.position()).reset();
						throw new IllegalStateException(String.format("Unknown language tag hash: %s", Hashes.encode(b)));
					}
					return vf.createLiteral(readString(b), lang);
				case LANGUAGE_LITERAL_TYPE:
					int langSize = b.get();
					b.limit(b.position()+langSize);
					lang = readString(b);
					b.limit(originalLimit);
					return vf.createLiteral(readString(b), lang);
				case DATATYPE_LITERAL_TYPE:
					int dtSize = b.getShort();
					b.limit(b.position()+dtSize);
					IRI datatype = (IRI) readValue(b);
					b.limit(originalLimit);
					return vf.createLiteral(readString(b), datatype);
				case TRIPLE_TYPE:
					if (tf == null) {
						throw new IllegalStateException("Unexpected triple value or missing TripleFactory");
					}
					return tf.readTriple(b, this);
				default:
					ByteReader reader = BYTE_READERS.get(type);
					if (reader == null) {
						throw new AssertionError(String.format("Unexpected type: %s", type));
					}
					return reader.readBytes(b, vf);
			}
		}
	}


	private static final class CellTripleWriter implements TripleWriter {
		@Override
		public ByteBuffer writeTriple(Resource subj, IRI pred, Value obj, ValueIO.Writer writer, ByteBuffer buf) {
			return writeStatementId(subj, pred, obj, buf);
		}
	}


	public static final class StreamTripleWriter implements TripleWriter {
		@Override
		public ByteBuffer writeTriple(Resource subj, IRI pred, Value obj, ValueIO.Writer writer, ByteBuffer buf) {
			buf = writeValue(subj, writer, buf, 2);
			buf = writeValue(pred, writer, buf, 2);
			buf = writeValue(obj, writer, buf, 4);
			return buf;
		}
	}


	public static final class StreamTripleReader implements TripleReader {
		@Override
		public Triple readTriple(ByteBuffer b, ValueIO.Reader reader) {
			Resource s = (Resource) readValue(b, reader, 2);
			IRI p = (IRI) readValue(b, reader, 2);
			Value o = readValue(b, reader, 4);
			return reader.getValueFactory().createTriple(s, p, o);
		}
	}
}
