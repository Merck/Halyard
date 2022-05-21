package com.msd.gin.halyard.common;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
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
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.BiFunction;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeConstants;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.transform.TransformerException;

import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
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
import org.locationtech.jts.io.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

public class ValueIO {
	public static final int DEFAULT_BUFFER_SIZE = 128;

	private static final Logger LOGGER = LoggerFactory.getLogger(ValueIO.class);
	private static final int IRI_HASH_SIZE = 4;
	private static final int NAMESPACE_HASH_SIZE = 2;
	private static final int LANG_HASH_SIZE = 2;

	private static List<Class<?>> getVocabularies() {
		List<Class<?>> vocabs = new ArrayList<>(25);

		LOGGER.info("Loading default vocabularies...");
		Class<?>[] defaultVocabClasses = { RDF.class, RDFS.class, XSD.class, SD.class, VOID.class, FOAF.class,
				OWL.class, DC.class, DCTERMS.class, SKOS.class, SKOSXL.class, ORG.class, GEO.class,
				WGS84.class, PROV.class, ROV.class };
		for (Class<?> vocabClass : defaultVocabClasses) {
			LOGGER.debug("Loading vocabulary {}", vocabClass.getName());
			vocabs.add(vocabClass);
		}

		LOGGER.info("Searching for additional vocabularies...");
		for (Vocabulary vocab : ServiceLoader.load(Vocabulary.class)) {
			LOGGER.info("Loading vocabulary {}", vocab.getClass().getName());
			vocabs.add(vocab.getClass());
		}

		return Collections.unmodifiableList(vocabs);
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

	static final byte IRI_TYPE = '<';
	static final byte COMPRESSED_IRI_TYPE = 'w';
	static final byte IRI_HASH_TYPE = '#';
	static final byte NAMESPACE_HASH_TYPE = ':';
	static final byte ENCODED_IRI_TYPE = '{';
	static final byte END_SLASH_ENCODED_IRI_TYPE = '}';
	static final byte BNODE_TYPE = '_';
	static final byte DATATYPE_LITERAL_TYPE = '\"';
	static final byte LANGUAGE_LITERAL_TYPE = '@';
	static final byte TRIPLE_TYPE = '*';
	static final byte FALSE_TYPE = '0';
	static final byte TRUE_TYPE = '1';
	static final byte LANGUAGE_HASH_LITERAL_TYPE = 'a';
	static final byte BYTE_TYPE = 'b';
	static final byte SHORT_TYPE = 's';
	static final byte INT_TYPE = 'i';
	static final byte LONG_TYPE = 'l';
	static final byte FLOAT_TYPE = 'f';
	static final byte DOUBLE_TYPE = 'd';
	static final byte COMPRESSED_STRING_TYPE = 'z';
	static final byte UNCOMPRESSED_STRING_TYPE = 'Z';
	static final byte TIME_TYPE = 't';
	static final byte DATE_TYPE = 'D';
	static final byte BIG_FLOAT_TYPE = 'F';
	static final byte BIG_INT_TYPE = 'I';
	static final byte LONG_COMPRESSED_BIG_INT_TYPE = '8';
	static final byte INT_COMPRESSED_BIG_INT_TYPE = '4';
	static final byte SHORT_COMPRESSED_BIG_INT_TYPE = '2';
	static final byte DATETIME_TYPE = 'T';
	static final byte WKT_LITERAL_TYPE = 'W';
	static final byte XML_TYPE = 'x';

	// compressed IRI types
	private static final byte HTTP_SCHEME = 'h';
	private static final byte HTTPS_SCHEME = 's';
	private static final byte DOI_HTTP_SCHEME = 'd';
	private static final byte DOI_HTTPS_SCHEME = 'D';

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
		b = ensureCapacity(b, Integer.BYTES + compressed.remaining());
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

	private static final int MAX_LONG_STRING_LENGTH = Long.toString(Long.MAX_VALUE).length();

	public static ByteBuffer writeCompressedInteger(String s, ByteBuffer b) {
		BigInteger bigInt;
		long x;
		if (s.length() < MAX_LONG_STRING_LENGTH) {
			x = XMLDatatypeUtil.parseLong(s);
			bigInt = null;
		} else {
			bigInt = XMLDatatypeUtil.parseInteger(s);
			double u = bigInt.doubleValue();
			if (u >= Long.MIN_VALUE && u <= Long.MAX_VALUE) {
				x = bigInt.longValueExact();
				bigInt = null;
			} else {
				x = 0;
			}
		}
		return writeCompressedInteger(bigInt, x, b);
	}

	private static ByteBuffer writeCompressedInteger(BigInteger bigInt, long x, ByteBuffer b) {
		if (bigInt != null) {
			byte[] bytes = bigInt.toByteArray();
			b = ensureCapacity(b, 1 + bytes.length);
			return b.put(BIG_INT_TYPE).put(bytes);
		} else if (x >= Short.MIN_VALUE && x <= Short.MAX_VALUE) {
			b = ensureCapacity(b, 1 + Short.BYTES);
			return b.put(SHORT_COMPRESSED_BIG_INT_TYPE).putShort((short) x);
		} else if (x >= Integer.MIN_VALUE && x <= Integer.MAX_VALUE) {
			b = ensureCapacity(b, 1 + Integer.BYTES);
			return b.put(INT_COMPRESSED_BIG_INT_TYPE).putInt((int) x);
		} else {
			b = ensureCapacity(b, 1 + Long.BYTES);
			return b.put(LONG_COMPRESSED_BIG_INT_TYPE).putLong(x);
		}
	}

	public static String readCompressedInteger(ByteBuffer b) {
		int type = b.get();
		switch (type) {
			case SHORT_COMPRESSED_BIG_INT_TYPE:
				return Short.toString(b.getShort());
			case INT_COMPRESSED_BIG_INT_TYPE:
				return Integer.toString(b.getInt());
			case LONG_COMPRESSED_BIG_INT_TYPE:
				return Long.toString(b.getLong());
			case BIG_INT_TYPE:
				byte[] bytes = new byte[b.remaining()];
				b.get(bytes);
				return new BigInteger(bytes).toString();
			default:
				throw new AssertionError(String.format("Unrecognized compressed integer type: %d", type));
		}
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
		if (sizeBytes == Short.BYTES) {
			buf.putShort((short) len);
		} else if (sizeBytes == Integer.BYTES) {
			buf.putInt(len);
		} else {
			throw new AssertionError();
		}
		buf.position(endPos);
		return buf;
	}

	public static Value readValue(ByteBuffer buf, ValueIO.Reader reader, int sizeBytes) {
		int len;
		if (sizeBytes == Short.BYTES) {
			len = buf.getShort();
		} else if (sizeBytes == Integer.BYTES) {
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
		boolean loadVocabularies = Config.getBoolean("halyard.vocabularies", true);
		boolean loadLanguages = Config.getBoolean("halyard.languages", true);
		int stringCompressionThreshold = Config.getInteger("halyard.string.compressionThreshold", 200);
		return new ValueIO(loadVocabularies, loadLanguages, stringCompressionThreshold);
	}

	private final BiMap<Short, String> wellKnownNamespaces = HashBiMap.create(256);
	private final Map<String,Namespace> wellKnownNamespacePrefixes = new HashMap<>(256);
	private final BiMap<Short, String> wellKnownLangs = HashBiMap.create(256);
	protected final BiMap<Integer, IRI> wellKnownIris = HashBiMap.create(1024);
	private final Map<IRI, ByteWriter> byteWriters = new HashMap<>(32);
	private final Map<Integer, ByteReader> byteReaders = new HashMap<>(32);
	private final Map<Short, IRIEncodingNamespace> iriEncoders = new HashMap<>();
	private final int stringCompressionThreshold;

	public ValueIO(boolean loadVocabularies, boolean loadLanguages, int stringCompressionThreshold) {
		this.stringCompressionThreshold = stringCompressionThreshold;

		if (loadVocabularies) {
			for(Class<?> vocab : getVocabularies()) {
				loadNamespaces(vocab);
				loadIRIs(vocab);
			}
		}
		if (loadLanguages) {
			try {
				loadLanguageTags();
			} catch (IOException ioe) {
				throw new UncheckedIOException(ioe);
			}
		}

		addByteReaderWriters();
	}

	public Collection<Namespace> getWellKnownNamespaces() {
		return wellKnownNamespacePrefixes.values();
	}

	private void loadNamespaces(Class<?> vocab) {
		Collection<Namespace> namespaces = getNamespace(vocab);
		addNamespaces(namespaces);
	}

	private Set<Namespace> getNamespace(Class<?> vocabulary) {
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

	private void addNamespaces(Collection<Namespace> namespaces) {
		for (Namespace namespace : namespaces) {
			String name = namespace.getName();
			Short hash = Hashes.hash16(Bytes.toBytes(name));
			if (wellKnownNamespaces.putIfAbsent(hash, name) != null) {
				throw new AssertionError(String.format("Hash collision between %s and %s",
						wellKnownNamespaces.get(hash), name));
			}
			wellKnownNamespacePrefixes.put(namespace.getPrefix(), namespace);
			if (namespace instanceof IRIEncodingNamespace) {
				iriEncoders.put(hash, (IRIEncodingNamespace) namespace);
			}
		}
	}

	private void loadLanguageTags() throws IOException {
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("languageTags"), "US-ASCII"))) {
			reader.lines().forEach(langTag -> {
				LOGGER.debug("Loading language {}", langTag);
				addLanguageTag(langTag);
				// add lowercase alternative
				String lcLangTag = langTag.toLowerCase();
				if (!lcLangTag.equals(langTag)) {
					addLanguageTag(lcLangTag);
				}
			});
		}
	}

	private void addLanguageTag(String langTag) {
		Short hash = Hashes.hash16(Bytes.toBytes(langTag));
		if (wellKnownLangs.putIfAbsent(hash, langTag) != null) {
			throw new AssertionError(String.format("Hash collision between %s and %s",
					wellKnownLangs.get(hash), langTag));
		}
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
			Integer hash = Hashes.hash32(Bytes.toBytes(iri.stringValue()));
			if (wellKnownIris.putIfAbsent(hash, iri) != null) {
				throw new AssertionError(String.format("Hash collision between %s and %s",
						wellKnownIris.get(hash), iri));
			}
		}
	}

	private void addByteWriter(IRI datatype, ByteWriter bw) {
		if (byteWriters.putIfAbsent(datatype, bw) != null) {
			throw new AssertionError(String.format("%s already exists for %s", ByteWriter.class.getSimpleName(), datatype));
		}
	}

	private void addByteReader(int valueType, ByteReader br) {
		if (byteReaders.putIfAbsent(valueType, br) != null) {
			throw new AssertionError(String.format("%s already exists for %s", ByteReader.class.getSimpleName(), (char)valueType));
		}
	}

	private void addByteReaderWriters() {
		addByteWriter(XSD.BOOLEAN, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				boolean v = l.booleanValue();
				b = ensureCapacity(b, 1);
				return b.put(v ? TRUE_TYPE : FALSE_TYPE);
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
				byte v = l.byteValue();
				return ensureCapacity(b, 1 + Byte.BYTES).put(BYTE_TYPE).put(v);
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
				short v = l.shortValue();
				return ensureCapacity(b, 1 + Short.BYTES).put(SHORT_TYPE).putShort(v);
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
				int v = l.intValue();
				return ensureCapacity(b, 1 + Integer.BYTES).put(INT_TYPE).putInt(v);
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
				long v = l.longValue();
				return ensureCapacity(b, 1 + Long.BYTES).put(LONG_TYPE).putLong(v);
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
				float v = l.floatValue();
				return ensureCapacity(b, 1 + Float.BYTES).put(FLOAT_TYPE).putFloat(v);
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
				double v = l.doubleValue();
				return ensureCapacity(b, 1 + Double.BYTES).put(DOUBLE_TYPE).putDouble(v);
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
				long x;
				if (l.getLabel().length() < MAX_LONG_STRING_LENGTH) {
					x = l.longValue();
					bigInt = null;
				} else {
					bigInt = l.integerValue();
					double u = bigInt.doubleValue();
					if (u >= Long.MIN_VALUE && u <= Long.MAX_VALUE) {
						x = bigInt.longValueExact();
						bigInt = null;
					} else {
						x = 0;
					}
				}
				return writeCompressedInteger(bigInt, x, b);
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
		addByteReader(INT_COMPRESSED_BIG_INT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return new IntLiteral(b.getInt(), XSD.INTEGER);
			}
		});
		addByteReader(SHORT_COMPRESSED_BIG_INT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return new IntLiteral(b.getShort(), XSD.INTEGER);
			}
		});

		addByteWriter(XSD.DECIMAL, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				BigDecimal x = l.decimalValue();
				byte[] bytes = x.unscaledValue().toByteArray();
				int scale = x.scale();
				b = ensureCapacity(b, 1 + Integer.BYTES + bytes.length);
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

		addByteWriter(GEO.WKT_LITERAL, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				try {
					byte[] wkb = WKTLiteral.writeWKB(l.getLabel());
					b = ensureCapacity(b, 2 + wkb.length);
					b.put(WKT_LITERAL_TYPE);
					b.put(WKT_LITERAL_TYPE); // mark as valid
					return b.put(wkb);
				} catch (ParseException | IOException e) {
					ByteBuffer wktBytes = writeUncompressedString(l.getLabel());
					b = ensureCapacity(b, 2 + wktBytes.remaining());
					b.put(WKT_LITERAL_TYPE);
					b.put(UNCOMPRESSED_STRING_TYPE); // mark as invalid
					return b.put(wktBytes);
				}
			}
		});
		addByteReader(WKT_LITERAL_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				int wktType = b.get();
				switch (wktType) {
					case WKT_LITERAL_TYPE:
						// valid wkt
						byte[] wkbBytes = new byte[b.remaining()];
						b.get(wkbBytes);
						return new WKTLiteral(wkbBytes);
					case UNCOMPRESSED_STRING_TYPE:
						// invalid xml
						return vf.createLiteral(readUncompressedString(b), GEO.WKT_LITERAL);
					default:
						throw new AssertionError(String.format("Unrecognized WKT type: %d", wktType));
				}
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
		if (compressed != null && Integer.BYTES + compressed.remaining() < uncompressedLen) {
			b = ensureCapacity(b, 1 + Integer.BYTES + compressed.remaining());
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

		private ByteBuffer writeIRI(IRI iri, ByteBuffer b) {
			Integer irihash = wellKnownIris.inverse().get(iri);
			if (irihash != null) {
				b = ensureCapacity(b, 1 + IRI_HASH_SIZE);
				b.put(IRI_HASH_TYPE);
				b.putInt(irihash);
				return b;
			} else {
				String ns = iri.getNamespace();
				String localName = iri.getLocalName();
				boolean endSlashEncodable = false;
				Short nshash = wellKnownNamespaces.inverse().get(ns);
				if (nshash == null) {
					// is it end-slash encodable?
					String s = iri.stringValue();
					int iriLen = s.length();
					// has to be at least x//
					if (iriLen >= 3 && s.charAt(iriLen-1) == '/') {
						int sepPos = s.lastIndexOf('/', iriLen-2);
						if (sepPos > 0) {
							ns = s.substring(0, sepPos+1);
							localName = s.substring(sepPos+1, iriLen-1);
							nshash = wellKnownNamespaces.inverse().get(ns);
							endSlashEncodable = true;
						}
					}
				}
				if (nshash != null) {
					IRIEncodingNamespace iriEncoder = !localName.isEmpty() ? iriEncoders.get(nshash) : null;
					if (iriEncoder != null) {
						b = ensureCapacity(b, 1 + NAMESPACE_HASH_SIZE);
						final int failsafeMark = b.position();
						b.put(endSlashEncodable ? END_SLASH_ENCODED_IRI_TYPE : ENCODED_IRI_TYPE);
						b.putShort(nshash);
						try {
							return iriEncoder.writeBytes(localName, b);
						} catch (Exception e) {
							LOGGER.warn("Possibly invalid IRI for namespace {}: {} ({})", ns, iri, e.getMessage());
							LOGGER.debug("{} for {} failed", IRIEncodingNamespace.class.getSimpleName(), ns, e);
							// if the dedicated namespace encoder fails then fallback to the generic encoder
							b.position(failsafeMark);
						}
					}
					ByteBuffer localBytes = writeUncompressedString(localName);
					b = ensureCapacity(b, 1 + NAMESPACE_HASH_SIZE + localBytes.remaining());
					b.put(NAMESPACE_HASH_TYPE);
					b.putShort(nshash);
					b.put(localBytes);
					return b;
				} else {
					byte schemeType;
					int prefixLen;
					String s = iri.stringValue();
					if (s.startsWith("http")) {
						prefixLen = 4;
						if (s.startsWith("://", prefixLen)) {
							prefixLen += 3;
							if (s.startsWith("dx.doi.org/", prefixLen)) {
								schemeType = DOI_HTTP_SCHEME;
								prefixLen += 11;
							} else {
								schemeType = HTTP_SCHEME;
							}
						} else if (s.startsWith("s://", prefixLen)) {
							prefixLen += 4;
							if (s.startsWith("dx.doi.org/", prefixLen)) {
								schemeType = DOI_HTTPS_SCHEME;
								prefixLen += 11;
							} else {
								schemeType = HTTPS_SCHEME;
							}
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
					return b;
				}
			}
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
				b = writeLanguagePrefix(langTag, b);
				return writeString(l.getLabel(), b);
			} else {
				ByteWriter writer = byteWriters.get(l.getDatatype());
				if (writer != null) {
					final int failsafeMark = b.position();
					try {
						return writer.writeBytes(l, b);
					} catch (Exception e) {
						LOGGER.warn("Possibly invalid literal: {} ({})", l, e.toString());
						LOGGER.debug("{} for {} failed", ByteWriter.class.getSimpleName(), l.getDatatype(), e);
						// if the dedicated writer fails then fallback to the generic writer
						b.position(failsafeMark);
					}
				}
				b = ensureCapacity(b, 1 + Short.BYTES);
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

		private ByteBuffer writeLanguagePrefix(String langTag, ByteBuffer b) {
			Short hash = wellKnownLangs.inverse().get(langTag);
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
						case DOI_HTTP_SCHEME:
							prefix = "http://dx.doi.org/";
							break;
						case DOI_HTTPS_SCHEME:
							prefix = "https://dx.doi.org/";
							break;
						default:
							throw new AssertionError(String.format("Unexpected scheme type: %d", schemeType));
					}
					String s = readUncompressedString(b);
					return vf.createIRI(prefix + s);
				case IRI_HASH_TYPE:
					b.mark();
					Integer irihash = b.getInt(); // 32-bit hash
					IRI iri = wellKnownIris.get(irihash);
					if (iri == null) {
						b.limit(b.position()).reset();
						throw new IllegalStateException(String.format("Unknown IRI hash: %s", Hashes.encode(b)));
					}
					return iri;
				case NAMESPACE_HASH_TYPE:
					b.mark();
					Short nshash = b.getShort(); // 16-bit hash
					String namespace = wellKnownNamespaces.get(nshash);
					if (namespace == null) {
						b.limit(b.position()).reset();
						throw new IllegalStateException(String.format("Unknown namespace hash: %s", Hashes.encode(b)));
					}
					return vf.createIRI(namespace, readUncompressedString(b));
				case ENCODED_IRI_TYPE:
					b.mark();
					nshash = b.getShort(); // 16-bit hash
					IRIEncodingNamespace iriEncoder = iriEncoders.get(nshash);
					if (iriEncoder == null) {
						b.limit(b.position()).reset();
						throw new IllegalStateException(String.format("Unknown IRI encoder hash: %s", Hashes.encode(b)));
					}
					return vf.createIRI(iriEncoder.getName(), iriEncoder.readBytes(b));
				case END_SLASH_ENCODED_IRI_TYPE:
					b.mark();
					nshash = b.getShort(); // 16-bit hash
					iriEncoder = iriEncoders.get(nshash);
					if (iriEncoder == null) {
						b.limit(b.position()).reset();
						throw new IllegalStateException(String.format("Unknown IRI encoder hash: %s", Hashes.encode(b)));
					}
					return vf.createIRI(iriEncoder.getName()+iriEncoder.readBytes(b)+'/');
				case BNODE_TYPE:
					return bnodeTransformer.apply(readUncompressedString(b), vf);
				case LANGUAGE_HASH_LITERAL_TYPE:
					b.mark();
					Short langHash = b.getShort(); // 16-bit hash
					String lang = wellKnownLangs.get(langHash);
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
					ByteReader reader = byteReaders.get(type);
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
			buf = writeValue(subj, writer, buf, Short.BYTES);
			buf = writeValue(pred, writer, buf, Short.BYTES);
			buf = writeValue(obj, writer, buf, Integer.BYTES);
			return buf;
		}
	}


	public static final class StreamTripleReader implements TripleReader {
		@Override
		public Triple readTriple(ByteBuffer b, ValueIO.Reader reader) {
			Resource s = (Resource) readValue(b, reader, Short.BYTES);
			IRI p = (IRI) readValue(b, reader, Short.BYTES);
			Value o = readValue(b, reader, Integer.BYTES);
			return reader.getValueFactory().createTriple(s, p, o);
		}
	}
}
