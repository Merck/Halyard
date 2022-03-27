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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

public class ValueIO {
	public static final int DEFAULT_BUFFER_SIZE = 128;

    private static final Logger LOGGER = LoggerFactory.getLogger(ValueIO.class);
    private static final List<Class<?>> VOCABULARIES = getVocabularies();
	private static final BiMap<Short, String> WELL_KNOWN_NAMESPACES = HashBiMap.create(256);
	private static final BiMap<Short, String> WELL_KNOWN_LANGS = HashBiMap.create(256);
	private static final int IRI_HASH_SIZE = 4;
	private static final int NAMESPACE_HASH_SIZE = 2;
	private static final int LANG_HASH_SIZE = 2;
	private static final int SHORT_SIZE = 2;
	private static final int INT_SIZE = 4;

	private static List<Class<?>> getVocabularies() {
		List<Class<?>> vocabs = new ArrayList<>(25);
		Class<?>[] defaultVocabs = { RDF.class, RDFS.class, XSD.class, SD.class, VOID.class, FOAF.class,
				OWL.class, DC.class, DCTERMS.class, SKOS.class, SKOSXL.class, ORG.class, GEO.class,
				WGS84.class, PROV.class, ROV.class };
		Collections.addAll(vocabs, defaultVocabs);

		Logger logger = LoggerFactory.getLogger(HalyardTableUtils.class);
		logger.info("Searching for vocabularies...");
		for(Vocabulary vocab : ServiceLoader.load(Vocabulary.class)) {
			logger.info("Loading vocabulary {}", vocab.getClass());
			vocabs.add(vocab.getClass());
		}
		return Collections.unmodifiableList(vocabs);
	}

	static {
		for(Class<?> vocab : VOCABULARIES) {
			loadNamespaces(vocab);
		}
		loadLanguageTags();
	}

	private static void loadNamespaces(Class<?> vocab) {
		Collection<Namespace> namespaces = getNamespace(vocab);
		addNamespaces(namespaces);
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

	interface ByteWriter {
		ByteBuffer writeBytes(Literal l, ByteBuffer b);
	}

	interface ByteReader {
		Literal readBytes(ByteBuffer b, ValueFactory vf);
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

	private static final LZ4Factory LZ4 = LZ4Factory.fastestJavaInstance();
	private static final Date GREGORIAN_ONLY = new Date(Long.MIN_VALUE);

	private static GregorianCalendar newGregorianCalendar() {
		GregorianCalendar cal = new GregorianCalendar();
		cal.setGregorianChange(GREGORIAN_ONLY);
		return cal;
	}

	private static final byte IRI_TYPE = '<';
	private static final byte COMPRESSED_IRI_TYPE = 'w';
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
	private static final byte COMPRESSED_STRING_TYPE = 'z';
	private static final byte UNCOMPRESSED_STRING_TYPE = 'Z';
	private static final byte TIME_TYPE = 't';
	private static final byte DATE_TYPE = 'D';
	private static final byte BIG_FLOAT_TYPE = 'F';
	private static final byte BIG_INT_TYPE = 'I';
	private static final byte COMPRESSED_BIG_INT_TYPE = 'J';
	private static final byte DATETIME_TYPE = 'T';
	private static final byte XML_TYPE = 'x';

	private static final byte HTTP_SCHEME = 'h';
	private static final byte HTTPS_SCHEME = 's';

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

	private static String readString(ByteBuffer b) {
		int type = b.get();
		switch (type) {
			case UNCOMPRESSED_STRING_TYPE:
				return readUncompressedString(b);
			case COMPRESSED_STRING_TYPE:
				return readCompressedString(b);
			default:
				throw new AssertionError(String.format("Unrecognized string type: %d", type));
		}
	}

	private static ByteBuffer writeUncompressedString(String s) {
		return StandardCharsets.UTF_8.encode(s);
	}

	private static String readUncompressedString(ByteBuffer b) {
		return StandardCharsets.UTF_8.decode(b).toString();
	}

	private static ByteBuffer writeCompressedString(String s, ByteBuffer b) {
		ByteBuffer uncompressed = writeUncompressedString(s);
		int uncompressedLen = uncompressed.remaining();
		ByteBuffer compressed = compress(uncompressed);
		b = ensureCapacity(b, INT_SIZE + compressed.remaining());
		return b.putInt(uncompressedLen).put(compressed);
	}

	private static String readCompressedString(ByteBuffer b) {
		ByteBuffer uncompressed = decompress(b);
		return readUncompressedString(uncompressed);
	}

	private static ByteBuffer compress(ByteBuffer b) {
		LZ4Compressor compressor = LZ4.highCompressor();
		int maxLen = compressor.maxCompressedLength(b.remaining());
		ByteBuffer compressed = ByteBuffer.allocate(maxLen);
		compressor.compress(b, compressed);
		b.flip();
		compressed.flip();
		return compressed;
	}

	private static ByteBuffer decompress(ByteBuffer b) {
		LZ4FastDecompressor decompressor = LZ4.fastDecompressor();
		int len = b.getInt();
		ByteBuffer uncompressed = ByteBuffer.allocate(len);
		decompressor.decompress(b, uncompressed);
		uncompressed.flip();
		return uncompressed;
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
		if (sizeBytes == SHORT_SIZE) {
			buf.putShort((short) len);
		} else if (sizeBytes == INT_SIZE) {
			buf.putInt(len);
		} else {
			throw new AssertionError();
		}
		buf.position(endPos);
		return buf;
	}

	public static Value readValue(ByteBuffer buf, ValueIO.Reader reader, int sizeBytes) {
		int len;
		if (sizeBytes == SHORT_SIZE) {
			len = buf.getShort();
		} else if (sizeBytes == INT_SIZE) {
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

	public static ValueIO create() {
		int stringCompressionThreshold = Config.getInteger("halyard.string.compressionThreshold", 200);
		return new ValueIO(stringCompressionThreshold);
	}

	protected final BiMap<Integer, IRI> WELL_KNOWN_IRIS = HashBiMap.create(1024);
	private final Map<IRI, ByteWriter> BYTE_WRITERS = new HashMap<>(32);
	private final Map<Integer, ByteReader> BYTE_READERS = new HashMap<>(32);
	private final int stringCompressionThreshold;

	public ValueIO(int stringCompressionThreshold) {
		this.stringCompressionThreshold = stringCompressionThreshold;

		for(Class<?> vocab : VOCABULARIES) {
			loadIRIs(vocab);
		}

		addByteReaderWriters();
	}

	boolean isWellKnownIRI(Value v) {
		return WELL_KNOWN_IRIS.containsValue(v);
	}

	private void loadIRIs(Class<?> vocab) {
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

	private void addIRIs(Collection<IRI> iris) {
		for (IRI iri : iris) {
			addIRI(iri);
		}
	}

	protected void addIRI(IRI iri) {
		Integer hash = Hashes.hash32(iri.stringValue().getBytes(StandardCharsets.UTF_8));
		if (WELL_KNOWN_IRIS.putIfAbsent(hash, iri) != null) {
			throw new AssertionError(String.format("Hash collision between %s and %s",
					WELL_KNOWN_IRIS.get(hash), iri));
		}
	}

	private void addByteWriter(IRI datatype, ByteWriter bw) {
		if (BYTE_WRITERS.putIfAbsent(datatype, bw) != null) {
			throw new AssertionError(String.format("%s already exists for %s", ByteWriter.class.getSimpleName(), datatype));
		}
	}

	private void addByteReader(int valueType, ByteReader br) {
		if (BYTE_READERS.putIfAbsent(valueType, br) != null) {
			throw new AssertionError(String.format("%s already exists for %s", ByteReader.class.getSimpleName(), (char)valueType));
		}
	}

	private void addByteReaderWriters() {
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
				BigInteger bigInt;
				int x;
				if (l.getLabel().length() < 10) {
					x = l.intValue();
					bigInt = null;
				} else {
					bigInt = l.integerValue();
					long u = bigInt.longValue();
					if (u >= Integer.MIN_VALUE && u <= Integer.MAX_VALUE) {
						x = bigInt.intValueExact();
						bigInt = null;
					} else {
						x = 0;
					}
				}
				if (bigInt != null) {
					byte[] bytes = bigInt.toByteArray();
					b = ensureCapacity(b, 1 + bytes.length);
					return b.put(BIG_INT_TYPE).put(bytes);
				} else {
					b = ensureCapacity(b, 1 + INT_SIZE);
					return b.put(COMPRESSED_BIG_INT_TYPE).putInt(x);
				}
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
		addByteReader(COMPRESSED_BIG_INT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return new IntLiteral(b.getInt(), XSD.INTEGER);
			}
		});

		addByteWriter(XSD.DECIMAL, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				BigDecimal x = l.decimalValue();
				byte[] bytes = x.unscaledValue().toByteArray();
				int scale = x.scale();
				b = ensureCapacity(b, 1 + INT_SIZE + bytes.length);
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
				return writeString(l.getLabel(), b);
			}
		});
		addByteReader(COMPRESSED_STRING_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(readCompressedString(b));
			}
		});
		addByteReader(UNCOMPRESSED_STRING_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(readUncompressedString(b));
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
					b = ensureCapacity(b, 2);
					b.put(XML_TYPE);
					b.put(COMPRESSED_STRING_TYPE); // mark xml as invalid
					return writeCompressedString(l.getLabel(), b);
				}
			}
		});
		addByteReader(XML_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				int xmlType = b.get();
				switch (xmlType) {
					case XML_TYPE:
						// valid xml
						byte[] fiBytes = new byte[b.remaining()];
						b.get(fiBytes);
						return new XMLLiteral(fiBytes);
					case COMPRESSED_STRING_TYPE:
						// invalid xml
						return vf.createLiteral(readCompressedString(b), RDF.XMLLITERAL);
					default:
						throw new AssertionError(String.format("Unrecognized XML type: %d", xmlType));
				}
			}
		});
	}

	private ByteBuffer writeString(String s, ByteBuffer b) {
		ByteBuffer uncompressed = writeUncompressedString(s);
		int uncompressedLen = uncompressed.remaining();
		ByteBuffer compressed;
		if (uncompressedLen > stringCompressionThreshold) {
			compressed = compress(uncompressed);
		} else {
			compressed = null;
		}
		if (compressed != null && INT_SIZE + compressed.remaining() < uncompressedLen) {
			b = ensureCapacity(b, 1 + INT_SIZE + compressed.remaining());
			return b.put(COMPRESSED_STRING_TYPE).putInt(uncompressedLen).put(compressed);
		} else {
			b = ensureCapacity(b, 1 + uncompressedLen);
			return b.put(UNCOMPRESSED_STRING_TYPE).put(uncompressed);
		}
	}

	public Writer createWriter(TripleWriter tw) {
		return new Writer(tw);
	}

	public Reader createReader(ValueFactory vf, TripleReader tf) {
		return createReader(vf, tf, (id,valueFactory) -> valueFactory.createBNode(id));
	}

	public Reader createReader(ValueFactory vf, TripleReader tf, BiFunction<String,ValueFactory,Resource> bnodeTransformer) {
		return new Reader(vf, tf, bnodeTransformer);
	}


	public final class Writer {
		private final TripleWriter tw;

		private Writer(TripleWriter tw) {
			this.tw = tw;
		}

		public ValueIO getValueIO() {
			return ValueIO.this;
		}

		public byte[] toBytes(Value v) {
			ByteBuffer tmp = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
			tmp = writeTo(v, tmp);
			tmp.flip();
			byte[] b = new byte[tmp.remaining()];
			tmp.get(b);
			return b;
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

		private ByteBuffer writeIRI(IRI v, ByteBuffer b) {
			Integer irihash = WELL_KNOWN_IRIS.inverse().get(v);
			if (irihash != null) {
				b = ensureCapacity(b, 1 + IRI_HASH_SIZE);
				b.put(IRI_HASH_TYPE);
				b.putInt(irihash);
			} else {
				IRI iri = v;
				Short nshash = WELL_KNOWN_NAMESPACES.inverse().get(iri.getNamespace());
				if (nshash != null) {
					ByteBuffer localBytes = writeUncompressedString(iri.getLocalName());
					b = ensureCapacity(b, 1 + NAMESPACE_HASH_SIZE + localBytes.remaining());
					b.put(NAMESPACE_HASH_TYPE);
					b.putShort(nshash);
					b.put(localBytes);
				} else {
					String s = v.stringValue();
					byte schemeType;
					int prefixLen;
					if (s.startsWith("http")) {
						if (s.startsWith("://", 4)) {
							schemeType = HTTP_SCHEME;
							prefixLen = 7;
						} else if (s.startsWith("s://", 4)) {
							schemeType = HTTPS_SCHEME;
							prefixLen = 8;
						} else {
							schemeType = 0;
							prefixLen = 0;
						}
					} else {
						schemeType = 0;
						prefixLen = 0;
					}
					if (schemeType != 0) {
						ByteBuffer restBytes = writeUncompressedString(s.substring(prefixLen));
						b = ensureCapacity(b, 2+restBytes.remaining());
						b.put(COMPRESSED_IRI_TYPE).put(schemeType);
						b.put(restBytes);
					} else {
						ByteBuffer iriBytes = writeUncompressedString(s);
						b = ensureCapacity(b, 1+iriBytes.remaining());
						b.put(IRI_TYPE);
						b.put(iriBytes);
					}
				}
			}
			return b;
		}

		private ByteBuffer writeBNode(BNode n, ByteBuffer b) {
			ByteBuffer idBytes = writeUncompressedString(n.getID());
			b = ensureCapacity(b, 1+idBytes.remaining());
			b.put(BNODE_TYPE);
			b.put(idBytes);
			return b;
		}

		private ByteBuffer writeLiteral(Literal l, ByteBuffer b) {
			if(l.getLanguage().isPresent()) {
				String langTag = l.getLanguage().get();
				Short hash = WELL_KNOWN_LANGS.inverse().get(langTag);
				if (hash != null) {
					b = ensureCapacity(b, 1+LANG_HASH_SIZE);
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
					ByteBuffer langBytes = writeUncompressedString(langTag);
					b = ensureCapacity(b, 1+1+langBytes.remaining());
					b.put(LANGUAGE_LITERAL_TYPE);
					b.put((byte) langBytes.remaining());
					b.put(langBytes);
				}
				return writeString(l.getLabel(), b);
			} else {
				ByteWriter writer = BYTE_WRITERS.get(l.getDatatype());
				if (writer != null) {
					try {
						return writer.writeBytes(l, b);
					} catch (Exception e) {
						LOGGER.warn("Possibly invalid literal: {}", l, e);
						// if the dedicated writer fails then fallback to the generic writer
						return defaultLiteralWriteBytes(l, b);
					}
				} else {
					return defaultLiteralWriteBytes(l, b);
				}
			}
		}

		private ByteBuffer defaultLiteralWriteBytes(Literal l, ByteBuffer b) {
			b = ensureCapacity(b, 1+SHORT_SIZE);
			b.put(DATATYPE_LITERAL_TYPE);
			int sizePos = b.position();
			int startPos = b.position()+2;
			b.position(startPos);
			b = writeIRI(l.getDatatype(), b);
			int endPos = b.position();
			b.position(sizePos);
			b.putShort((short) (endPos-startPos));
			b.position(endPos);
			ByteBuffer labelBytes = writeUncompressedString(l.getLabel());
			b = ensureCapacity(b, labelBytes.remaining());
			b.put(labelBytes);
			return b;
		}
	}


	public class Reader {
		private final ValueFactory vf;
		private final TripleReader tf;
		private final BiFunction<String,ValueFactory,Resource> bnodeTransformer;

		private Reader(ValueFactory vf, TripleReader tf, BiFunction<String,ValueFactory,Resource> bnodeTransformer) {
			this.vf = vf;
			this.tf = tf;
			this.bnodeTransformer = bnodeTransformer;
		}

		public ValueIO getValueIO() {
			return ValueIO.this;
		}

		public ValueFactory getValueFactory() {
			return vf;
		}

		public Value readValue(ByteBuffer b) {
			final int originalLimit = b.limit();
			int type = b.get();
			switch(type) {
				case IRI_TYPE:
					return vf.createIRI(readUncompressedString(b));
				case COMPRESSED_IRI_TYPE:
					int schemeType = b.get();
					String prefix;
					switch (schemeType) {
						case HTTP_SCHEME:
							prefix = "http://";
							break;
						case HTTPS_SCHEME:
							prefix = "https://";
							break;
						default:
							throw new AssertionError(String.format("Unexpected scheme type: %d", schemeType));
					}
					String s = readUncompressedString(b);
					return vf.createIRI(prefix + s);
				case IRI_HASH_TYPE:
					b.mark();
					Integer irihash = b.getInt(); // 32-bit hash
					IRI iri = WELL_KNOWN_IRIS.get(irihash);
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
					return vf.createIRI(namespace, readUncompressedString(b));
				case BNODE_TYPE:
					return bnodeTransformer.apply(readUncompressedString(b), vf);
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
					lang = readUncompressedString(b);
					b.limit(originalLimit);
					return vf.createLiteral(readString(b), lang);
				case DATATYPE_LITERAL_TYPE:
					int dtSize = b.getShort();
					b.limit(b.position()+dtSize);
					IRI datatype = (IRI) readValue(b);
					b.limit(originalLimit);
					return vf.createLiteral(readUncompressedString(b), datatype);
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


	public static final class StreamTripleWriter implements TripleWriter {
		@Override
		public ByteBuffer writeTriple(Resource subj, IRI pred, Value obj, ValueIO.Writer writer, ByteBuffer buf) {
			buf = writeValue(subj, writer, buf, SHORT_SIZE);
			buf = writeValue(pred, writer, buf, SHORT_SIZE);
			buf = writeValue(obj, writer, buf, INT_SIZE);
			return buf;
		}
	}


	public static final class StreamTripleReader implements TripleReader {
		@Override
		public Triple readTriple(ByteBuffer b, ValueIO.Reader reader) {
			Resource s = (Resource) readValue(b, reader, SHORT_SIZE);
			IRI p = (IRI) readValue(b, reader, SHORT_SIZE);
			Value o = readValue(b, reader, INT_SIZE);
			return reader.getValueFactory().createTriple(s, p, o);
		}
	}
}
