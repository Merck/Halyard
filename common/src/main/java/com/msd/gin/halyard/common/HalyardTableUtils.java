/*
 * Copyright 2016 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
 * Inc., Kenilworth, NJ, USA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.msd.gin.halyard.common;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.hash.Hashing;
import com.msd.gin.halyard.vocab.HALYARD;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeConstants;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
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

/**
 * Core Halyard utility class performing RDF to HBase mappings and base HBase table and key management. The methods of this class define how
 * Halyard stores and finds data in HBase. This class also provides several constants that define the key encoding.
 *
 * @author Adam Sotona (MSD)
 */
public final class HalyardTableUtils {

    private static final byte[] CF_NAME = "e".getBytes(StandardCharsets.UTF_8);
    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

    /*
     * Triples/ quads are stored in multiple regions as different permutations.
     * These values define the prefixes of the regions.
     */

    /**
     * HBase key prefix for SPO regions
     */
    public static final byte SPO_PREFIX = 0;

    /**
     * HBase key prefix for POS regions
     */
    public static final byte POS_PREFIX = 1;

    /**
     * HBase key prefix for OSP regions
     */
    public static final byte OSP_PREFIX = 2;

    /**
     * HBase key prefix for CSPO regions
     */
    public static final byte CSPO_PREFIX = 3;

    /**
     * HBase key prefix for CPOS regions
     */
    public static final byte CPOS_PREFIX = 4;

    /**
     * HBase key prefix for COSP regions
     */
    public static final byte COSP_PREFIX = 5;

	private static final int PREFIXES = 3;
	public static final int ID_SIZE = 20;

	static final byte[] STOP_KEY_16 = new byte[2];
	static final byte[] STOP_KEY_32 = new byte[4];
	static final byte[] STOP_KEY_48 = new byte[6];
	static final byte[] STOP_KEY_64 = new byte[8];
	static final byte[] STOP_KEY_128 = new byte[16];

	static {
		Arrays.fill(STOP_KEY_16, (byte) 0xff); /* 0xff is 255 in decimal */
		Arrays.fill(STOP_KEY_32, (byte) 0xff); /* 0xff is 255 in decimal */
		Arrays.fill(STOP_KEY_48, (byte) 0xff); /* 0xff is 255 in decimal */
		Arrays.fill(STOP_KEY_64, (byte) 0xff); /* 0xff is 255 in decimal */
		Arrays.fill(STOP_KEY_128, (byte) 0xff); /* 0xff is 255 in decimal */
    }

	private static final byte NON_LITERAL_FLAG = (byte) 0x80;
	public static final byte[] LITERAL_STOP_KEY = new byte[] { NON_LITERAL_FLAG };

	private static final int READ_VERSIONS = 1;
	private static final Compression.Algorithm DEFAULT_COMPRESSION_ALGORITHM = Compression.Algorithm.GZ;
    private static final DataBlockEncoding DEFAULT_DATABLOCK_ENCODING = DataBlockEncoding.PREFIX;
	private static final long REGION_MAX_FILESIZE = 10000000000l;
    private static final String REGION_SPLIT_POLICY = "org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy";

    private static final ThreadLocal<MessageDigest> MD = new ThreadLocal<MessageDigest>() {
        @Override
		protected MessageDigest initialValue() {
			return getMessageDigest("SHA-1");
        }
    };

    private static final byte[] PEARSON_HASH_TABLE = {
		// 0-255 shuffled in any (random) order suffices
		39,(byte)158,(byte)178,(byte)187,(byte)131,(byte)136,1,49,50,17,(byte)141,91,47,(byte)129,60,99,
		(byte)237,18,(byte)253,(byte)225,8,(byte)208,(byte)172,(byte)244,(byte)255,126,101,79,(byte)145,(byte)235,(byte)228,121,
		123,(byte)251,67,(byte)250,(byte)161,0,107,97,(byte)241,111,(byte)181,82,(byte)249,33,69,55,
		(byte)197,96,(byte)210,45,16,(byte)227,(byte)248,(byte)202,51,(byte)152,(byte)252,125,81,(byte)206,(byte)215,(byte)186,
		90,(byte)168,(byte)156,(byte)203,(byte)177,120,2,(byte)190,(byte)188,7,100,(byte)185,(byte)174,(byte)243,(byte)162,10,
		(byte)154,35,86,(byte)171,105,34,38,(byte)200,(byte)147,58,77,118,(byte)173,(byte)246, 76,(byte)254,
		3,14,(byte)204,72,21,41,56,66,28,(byte)193,40,(byte)217,25,54,(byte)179,117,
		(byte)189,(byte)205,(byte)199,(byte)128,(byte)176,19,(byte)211,(byte)236,127,(byte)192,(byte)231,70,(byte)233,88,(byte)146,44,
		98,6,85,(byte)150,36,23,112,(byte)164,(byte)135,(byte)207,(byte)169,5,26,64,(byte)165,(byte)219,
		(byte)183,(byte)201,22,83,13,(byte)214,116,109,(byte)159,32,95,(byte)226,(byte)140,(byte)220, 57, 12,
		59,(byte)153,29,9,(byte)213,(byte)167,84,93,30,46,94,75,(byte)151,114,73,(byte)222,
		(byte)238,87,(byte)240,(byte)155,(byte)180,(byte)170,(byte)242,(byte)212,(byte)191,(byte)163,78,(byte)218,(byte)137,(byte)194,(byte)175,110,
		61,20,68,89,(byte)130,63,52,102,24,(byte)229,(byte)132,(byte)245,80,(byte)216,(byte)195,115,
		(byte)133,(byte)232,(byte)196,(byte)144,(byte)198,124,53,4,108,74,(byte)223,(byte)234,(byte)134,(byte)230,(byte)157,(byte)139,
		43,119,(byte)224,71,122,(byte)142,42,(byte)160,104,48,(byte)247,103,15,11,(byte)138,(byte)239,
		(byte)221, 31,(byte)209,(byte)182,(byte)143,92,(byte)149,(byte)184,(byte)148,62,113,65,37,27,106,(byte)166
	};

	private static byte[] hash16(byte[] key) {
		byte h1 = PEARSON_HASH_TABLE[(key[0] & 0xFF) % 256];
		byte h2 = PEARSON_HASH_TABLE[(key[key.length-1] & 0xFF) % 256];
		for(int j = 1; j < key.length; j++) {
			h1 = PEARSON_HASH_TABLE[(h1 & 0xFF) ^ (key[j] & 0xFF)];
			h2 = PEARSON_HASH_TABLE[(h2 & 0xFF) ^ (key[key.length - 1 - j] & 0xFF)];
		}
		return new byte[] {h1, h2};
    }

    private static byte[] hash32(byte[] key) {
    	return Hashing.murmur3_32().hashBytes(key).asBytes();
    }

    public static byte[] hashUnique(byte[] key) {
		MessageDigest md = MD.get();
        try {
            md.update(key);
            return md.digest();
        } finally {
            md.reset();
        }
    }

	private static final BiMap<ByteBuffer, IRI> WELL_KNOWN_IRIS = HashBiMap.create(256);
	private static final BiMap<ByteBuffer, IRI> WELL_KNOWN_IRI_IDS = HashBiMap.create(256);
	private static final BiMap<ByteBuffer, String> WELL_KNOWN_NAMESPACES = HashBiMap.create(256);
	private static final byte WELL_KNOWN_IRI_MARKER = (byte) ('#' | 0x80);  // marker must be negative (msb set) so it is distinguishable from a length (>=0)

	private static void loadNamespacesAndIRIs(Class<?> vocab) {
		Set<Namespace> namespaces = getNamespace(vocab);
		for (Namespace namespace : namespaces) {
			String name = namespace.getName();
			ByteBuffer hash = ByteBuffer.wrap(hash16(name.getBytes(StandardCharsets.UTF_8)));
			if (WELL_KNOWN_NAMESPACES.putIfAbsent(hash, name) != null) {
				throw new AssertionError(String.format("Hash collision between %s and %s",
						WELL_KNOWN_NAMESPACES.get(hash), name));
			}
		}

		Set<IRI> iris = Vocabularies.getIRIs(vocab);
		for (IRI iri : iris) {
			byte[] id = id(iri);
			IdentifiableIRI idIri = new IdentifiableIRI(id, iri);

			ByteBuffer idbb = ByteBuffer.wrap(id).asReadOnlyBuffer();
			if (WELL_KNOWN_IRI_IDS.putIfAbsent(idbb, idIri) != null) {
				throw new AssertionError(String.format("Hash collision between %s and %s",
						WELL_KNOWN_IRI_IDS.get(idbb), idIri));
			}

			ByteBuffer hash = ByteBuffer.wrap(hash32(idIri.toString().getBytes(StandardCharsets.UTF_8))).asReadOnlyBuffer();
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

	private static final DatatypeFactory DATATYPE_FACTORY;
	static {
		try {
			DATATYPE_FACTORY = DatatypeFactory.newInstance();
		}
		catch (DatatypeConfigurationException e) {
			throw new AssertionError(e);
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
				return new StringBuilder().append((char)STRING_TYPE).append(l.getLabel()).toString().getBytes(StandardCharsets.UTF_8);
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

	static MessageDigest getMessageDigest(String algorithm) {
        try {
            return MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
	 * Helper method which locates or creates and returns the specified Table used for triple/ quad storage. The table may be pre-split into regions (rather than HBase's default of
	 * starting with 1). For a discussion of pre-splits take a look at <a href= "https://hortonworks.com/blog/apache-hbase-region-splitting-and-merging/">this article</a>
	 * 
	 * @param config Hadoop Configuration of the cluster running HBase
	 * @param tableName String table name
	 * @param create boolean option to create the table if does not exist
	 * @param splitBits int number of bits used for calculation of Table region pre-splits (applies for new tables only). Must be between 0 and 16. Higher values generate more
	 * splits.
	 * @throws IOException throws IOException in case of any HBase IO problems
	 * @return the org.apache.hadoop.hbase.client.Table
	 */
	public static Table getTable(Configuration config, String tableName, boolean create, int splitBits) throws IOException {
		return getTable(config, tableName, create, splitBits, true);
	}
	public static Table getTable(Configuration config, String tableName, boolean create, int splitBits, boolean quads)
			throws IOException {
		return getTable(getConnection(config), tableName, create, splitBits, quads);
	}

	/**
	 * Helper method which locates or creates and returns the specified Table used for triple/ quad storage. The table may be pre-split into regions (rather than HBase's default of
	 * starting with 1). For a discussion of pre-splits take a look at <a href= "https://hortonworks.com/blog/apache-hbase-region-splitting-and-merging/">this article</a>
	 * 
	 * @param conn Connection to the cluster running HBase
	 * @param tableName String table name
	 * @param create boolean option to create the table if does not exist
	 * @param splitBits int number of bits used for calculation of Table region pre-splits (applies for new tables only). Must be between 0 and 16. Higher values generate more
	 * splits.
	 * @throws IOException throws IOException in case of any HBase IO problems
	 * @return the org.apache.hadoop.hbase.client.Table
	 */
	public static Table getTable(Connection conn, String tableName, boolean create, int splitBits) throws IOException {
		return getTable(conn, tableName, create, splitBits, true);
	}
	public static Table getTable(Connection conn, String tableName, boolean create, int splitBits, boolean quads) throws IOException {
		return getTable(conn, tableName, create, splitBits < 0 ? null : calculateSplits(splitBits, quads));
    }

    /**
     * Helper method which locates or creates and returns the specified HTable used for triple/ quad storage
     * @param config Hadoop Configuration of the cluster running HBase
     * @param tableName String table name
     * @param create boolean option to create the table if does not exists
     * @param splits array of keys used to pre-split new table, may be null
     * @return HTable
     * @throws IOException throws IOException in case of any HBase IO problems
     */
	public static Table getTable(Configuration config, String tableName, boolean create, byte[][] splits)
			throws IOException {
		return getTable(getConnection(config), tableName, create, splits);
	}

	/**
	 * Helper method which locates or creates and returns the specified Table used for triple/ quad storage
	 * 
	 * @param conn Connection to the cluster running HBase
	 * @param tableName String table name
	 * @param create boolean option to create the table if does not exists
	 * @param splits array of keys used to pre-split new table, may be null
	 * @return Table
	 * @throws IOException throws IOException in case of any HBase IO problems
	 */
	public static Table getTable(Connection conn, String tableName, boolean create, byte[][] splits)
			throws IOException {
		TableName htableName = TableName.valueOf(tableName);
        if (create) {
			try (Admin admin = conn.getAdmin()) {
				// check if the table exists and if it doesn't, make it
				if (!admin.tableExists(htableName)) {
					TableDescriptor td = TableDescriptorBuilder.newBuilder(htableName).setColumnFamily(createColumnFamily()).setMaxFileSize(REGION_MAX_FILESIZE).setRegionSplitPolicyClassName(REGION_SPLIT_POLICY).build();
					admin.createTable(td, splits);
                }
            }
        }
		return conn.getTable(htableName);
	}

	public static Connection getConnection(Configuration config) throws IOException {
		Configuration cfg = HBaseConfiguration.create(config);
		cfg.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 3600000l);
		return ConnectionFactory.createConnection(cfg);
	}

	/**
	 * Truncates Table while preserving the region pre-splits
	 * 
	 * @param conn connection to cluster
	 * @param table Table to truncate
	 * @throws IOException throws IOException in case of any HBase IO problems
	 */
	public static void truncateTable(Connection conn, Table table) throws IOException {
		try (Admin admin = conn.getAdmin()) {
			admin.disableTable(table.getName());
			admin.truncateTable(table.getName(), true);
		}
    }

    /**
	 * Calculates the split keys (one for each permutation of the CSPO HBase Key prefix).
	 * 
	 * @param splitBits must be between 0 and 15, larger values result in more keys.
	 * @return An array of keys represented as {@code byte[]}s
	 */
	static byte[][] calculateSplits(final int splitBits, boolean quads) {
		return calculateSplits(splitBits, quads, null, 0.5f);
	}
	static byte[][] calculateSplits(final int splitBits, boolean quads, Map<IRI,Float> predicateRatios, float literalRatio) {
        TreeSet<byte[]> splitKeys = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        //basic presplits
        splitKeys.add(new byte[]{POS_PREFIX});
        splitKeys.add(new byte[]{OSP_PREFIX});
		if (quads) {
			splitKeys.add(new byte[] { CSPO_PREFIX });
			splitKeys.add(new byte[] { CPOS_PREFIX });
			splitKeys.add(new byte[] { COSP_PREFIX });
		}
        //common presplits
		addSplitsNoLiterals(splitKeys, new byte[] { SPO_PREFIX }, splitBits, null);
		addSplitsNoLiterals(splitKeys, new byte[] { POS_PREFIX }, splitBits, transformKeys(predicateRatios, iri -> RDFPredicate.create(iri)));
        addSplits(splitKeys, new byte[]{OSP_PREFIX}, splitBits, literalRatio);
        if (quads) {
			addSplitsNoLiterals(splitKeys, new byte[] { CSPO_PREFIX }, splitBits/2, null);
			addSplitsNoLiterals(splitKeys, new byte[] { CPOS_PREFIX }, splitBits/2, null);
			addSplitsNoLiterals(splitKeys, new byte[] { COSP_PREFIX }, splitBits/2, null);
        }
        return splitKeys.toArray(new byte[splitKeys.size()][]);
    }

	private static <K1,K2,V> Map<K2,V> transformKeys(Map<K1,V> map, Function<K1,K2> f) {
		if (map == null) {
			return null;
		}
		Map<K2,V> newMap = new HashMap<>(map.size()+1);
		for (Map.Entry<K1,V> entry : map.entrySet()) {
			newMap.put(f.apply(entry.getKey()), entry.getValue());
		}
		return newMap;
	}

	/**
	 * Generate the split key and add it to the collection.
	 * 
	 * @param splitKeys the {@code TreeSet} to add the collection to.
	 * @param prefix the prefix to calculate the key for
	 * @param splitBits between 0 and 16, larger values generate smaller split steps
	 */
	private static void addSplits(TreeSet<byte[]> splitKeys, byte[] prefix, final int splitBits, float literalRatio) {
		int noLitSplitBits = (int)Math.round((1.0f-literalRatio)*splitBits);
		int litSplitBits = (int)Math.round(literalRatio*splitBits);
		boolean needDivider = (noLitSplitBits > 0) && (litSplitBits > 0);
		// adjust for rounding errors
		if (noLitSplitBits + litSplitBits > splitBits) {
			float scale = (float)splitBits/(float)(noLitSplitBits + litSplitBits);
			noLitSplitBits *= scale;
			litSplitBits *= scale;
		}
		addSplitsNoLiterals(splitKeys, prefix, noLitSplitBits, null);
		addSplitsLiterals(splitKeys, prefix, litSplitBits);
		if (needDivider) {
			byte[] prefixLiteralSplit = Arrays.copyOf(prefix, prefix.length + 2);
			prefixLiteralSplit[prefix.length] = NON_LITERAL_FLAG;
			splitKeys.add(prefixLiteralSplit);
		}
	}

	/**
	 * Generate the split key (for non-literals) and add it to the collection.
	 * 
	 * @param splitKeys the {@code TreeSet} to add the collection to.
	 * @param prefix the prefix to calculate the key for
	 * @param splitBits between 0 and 15, larger values generate smaller split steps
	 */
	private static void addSplitsNoLiterals(TreeSet<byte[]> splitKeys, byte[] prefix, final int splitBits, Map<? extends RDFIdentifier,Float> keyFractions) {
        if (splitBits == 0) return;
		if (splitBits < 0 || splitBits > 15) {
			throw new IllegalArgumentException("Illegal nunmber of split bits");
		}

		int actualSplitBits = 0;
		int nonZeroSplitCount = 0;
		float fractionSum = 0.0f;
		if (keyFractions != null && !keyFractions.isEmpty()) {
			for (Float f : keyFractions.values()) {
				actualSplitBits += (int)Math.round(f*splitBits);
				if (actualSplitBits > 0) {
					nonZeroSplitCount++;
				}
				fractionSum += f;
			}
		}
		int otherSplitBits = (int)Math.round((1.0f - fractionSum)*splitBits);
		actualSplitBits += otherSplitBits;
		if (otherSplitBits > 0) {
			nonZeroSplitCount++;
		}
		float scale = (float)splitBits/(float)actualSplitBits;

		fractionSum = 0.0f;
		if (keyFractions != null && !keyFractions.isEmpty()) {
			for (Map.Entry<? extends RDFIdentifier, Float> entry : keyFractions.entrySet()) {
				byte[] keyHash = entry.getKey().getKeyHash(prefix[0]);
				byte[] keyPrefix = new byte[1+keyHash.length];
				keyPrefix[0] = prefix[0];
				System.arraycopy(keyHash, 0, keyPrefix, 1, keyHash.length);
				if (nonZeroSplitCount > 1) {
					// add divider
					splitKeys.add(keyPrefix);
				}
				float fraction = entry.getValue();
				int keySplitBits = (int)(scale*Math.round(fraction*splitBits));
				final int splitStep = 1 << (16 - keySplitBits);
				for (int i = splitStep; i <= 0xFFFF; i += splitStep) {
		            byte bb[] = Arrays.copyOf(keyPrefix, keyPrefix.length + 2);
					bb[keyPrefix.length] = (byte) ((i >> 8) & 0xff); // 0xff = 255.
		            bb[keyPrefix.length + 1] = (byte)(i & 0xff);
		            splitKeys.add(bb);
				}
				fractionSum += fraction;
			}
		}

		otherSplitBits *= scale;
		final int splitStep = 1 << (15 - otherSplitBits); // 1 splitBit gives a split step of 16384, 8 splitBits gives a split step of 128
		for (int i = splitStep; i <= 0x7FFF; i += splitStep) { // 0x7FFF is 32767 so a split step of 16384 will give 2 iterations, larger split bits give more iterations
            byte bb[] = Arrays.copyOf(prefix, prefix.length + 2);
			bb[prefix.length] = (byte) (0x80 + ((i >> 8) & 0xff)); // 0xff = 255.
            bb[prefix.length + 1] = (byte)(i & 0xff);
            splitKeys.add(bb);
        }
    }

	private static void addSplitsLiterals(TreeSet<byte[]> splitKeys, byte[] prefix, final int splitBits) {
        if (splitBits == 0) return;
		if (splitBits < 0 || splitBits > 15) {
			throw new IllegalArgumentException("Illegal nunmber of split bits");
		}

		final int splitStep = 1 << (15 - splitBits); // 1 splitBit gives a split step of 16384, 8 splitBits gives a split step of 128
		for (int i = splitStep; i <= 0x7FFF; i += splitStep) { // 0x7FFF is 32767 so a split step of 16384 will give 2 iterations, larger split bits give more iterations
            byte bb[] = Arrays.copyOf(prefix, prefix.length + 2);
			bb[prefix.length] = (byte) ((i >> 8) & 0xff); // 0xff = 255.
            bb[prefix.length + 1] = (byte)(i & 0xff);
            splitKeys.add(bb);
        }
    }

	/**
     * Conversion method from Subj, Pred, Obj and optional Context into an array of HBase keys
     * @param subj subject Resource
     * @param pred predicate IRI
     * @param obj object Value
     * @param context optional context Resource
     * @param delete boolean switch whether to get KeyValues for deletion instead of for insertion
     * @param timestamp long timestamp value for time-ordering purposes
     * @return List of KeyValues
     */
	public static List<? extends KeyValue> toKeyValues(Resource subj, IRI pred, Value obj, Resource context, boolean delete, long timestamp) {
		List<KeyValue> kvs =  new ArrayList<KeyValue>(context == null ? PREFIXES : 2 * PREFIXES);
        KeyValue.Type type = delete ? KeyValue.Type.DeleteColumn : KeyValue.Type.Put;
		timestamp = toHalyardTimestamp(timestamp, !delete);
		appendKeyValues(subj, pred, obj, context, type, timestamp, kvs);
		return kvs;
	}

    private static void appendKeyValues(Resource subj, IRI pred, Value obj, Resource context, KeyValue.Type type, long timestamp, List<KeyValue> kvs) {
    	if(subj == null || pred == null || obj == null) {
    		throw new NullPointerException();
    	}
    	if(context instanceof Triple) {
    		throw new UnsupportedOperationException("Context cannot be a triple value");
    	}

    	RDFSubject sb = RDFSubject.create(subj); // subject bytes
		RDFPredicate pb = RDFPredicate.create(pred); // predicate bytes
		RDFObject ob = RDFObject.create(obj); // object bytes
		RDFContext cb = RDFContext.create(context); // context (graph) bytes

		// generate HBase key value pairs from: row, family, qualifier, value. Permutations of SPO (and if needed CSPO) are all stored.
		kvs.add(new KeyValue(row(SPO_PREFIX, sb, pb, ob, cb), CF_NAME, qualifier(SPO_PREFIX, sb, pb, ob, cb), timestamp, type, value(SPO_PREFIX, sb, pb, ob, cb)));
		kvs.add(new KeyValue(row(POS_PREFIX, pb, ob, sb, cb), CF_NAME, qualifier(POS_PREFIX, pb, ob, sb, cb), timestamp, type, value(POS_PREFIX, pb, ob, sb, cb)));
		kvs.add(new KeyValue(row(OSP_PREFIX, ob, sb, pb, cb), CF_NAME, qualifier(OSP_PREFIX, ob, sb, pb, cb), timestamp, type, value(OSP_PREFIX, ob, sb, pb, cb)));
        if (context != null) {
        	kvs.add(new KeyValue(row(CSPO_PREFIX, cb, sb, pb, ob), CF_NAME, qualifier(CSPO_PREFIX, cb, sb, pb, ob), timestamp, type, value(CSPO_PREFIX, cb, sb, pb, ob)));
        	kvs.add(new KeyValue(row(CPOS_PREFIX, cb, pb, ob, sb), CF_NAME, qualifier(CPOS_PREFIX, cb, pb, ob, sb), timestamp, type, value(CPOS_PREFIX, cb, pb, ob, sb)));
        	kvs.add(new KeyValue(row(COSP_PREFIX, cb, ob, sb, pb), CF_NAME, qualifier(COSP_PREFIX, cb, ob, sb, pb), timestamp, type, value(COSP_PREFIX, cb, ob, sb, pb)));
        }

		if (subj instanceof Triple) {
			Triple t = (Triple) subj;
			appendKeyValues(t.getSubject(), t.getPredicate(), t.getObject(), HALYARD.TRIPLE_GRAPH_CONTEXT, type, timestamp, kvs);
		}

		if (obj instanceof Triple) {
			Triple t = (Triple) obj;
			appendKeyValues(t.getSubject(), t.getPredicate(), t.getObject(), HALYARD.TRIPLE_GRAPH_CONTEXT, type, timestamp, kvs);
		}
    }

    private static byte[] row(byte prefix, RDFIdentifier v1, RDFIdentifier v2, RDFIdentifier v3, RDFIdentifier v4) {
    	ByteBuffer r;
        switch(prefix) {
        	case SPO_PREFIX:
        	case POS_PREFIX:
        	case OSP_PREFIX:
				r = ByteBuffer.allocate(1 + v1.keyHashSize() + v2.keyHashSize() + v3.endKeyHashSize() + (v4 != null ? v4.keyHashSize() : 0));
				r.put(prefix);
				r.put(v1.getKeyHash(prefix));
				r.put(v2.getKeyHash(prefix));
				r.put(v3.getEndKeyHash(prefix));
        		if(v4 != null) {
					r.put(v4.getKeyHash(prefix));
        		}
                break;
        		
        	case CSPO_PREFIX:
        	case CPOS_PREFIX:
        	case COSP_PREFIX:
        		r = ByteBuffer.allocate(1 + v1.keyHashSize() + v2.keyHashSize() + v3.keyHashSize() + v4.endKeyHashSize());
				r.put(prefix);
        		r.put(v1.getKeyHash(prefix));
        		r.put(v2.getKeyHash(prefix));
        		r.put(v3.getKeyHash(prefix));
       			r.put(v4.getEndKeyHash(prefix));
                break;
            default:
            	throw new AssertionError(String.format("Invalid prefix: %s", prefix));
        }
        return r.array();
    }

	private static byte[] qualifier(byte prefix, RDFIdentifier v1, RDFIdentifier v2, RDFIdentifier v3, RDFIdentifier v4) {
    	ByteBuffer cq;
        switch(prefix) {
        	case SPO_PREFIX:
        	case POS_PREFIX:
        	case OSP_PREFIX:
				cq = ByteBuffer.allocate(v1.qualifierHashSize() + (v2 != null ? v2.qualifierHashSize() : 0) + (v3 != null ? v3.endQualifierHashSize() : 0) + (v4 != null ? v4.qualifierHashSize() : 0));
				cq.put(v1.getQualifierHash());
        		if(v2 != null) {
					cq.put(v2.getQualifierHash());
	        		if(v3 != null) {
						cq.put(v3.getEndQualifierHash());
		        		if(v4 != null) {
							cq.put(v4.getQualifierHash());
		        		}
	        		}
        		}
                break;
        		
        	case CSPO_PREFIX:
        	case CPOS_PREFIX:
        	case COSP_PREFIX:
        		cq = ByteBuffer.allocate(v1.qualifierHashSize() + (v2 != null ? v2.qualifierHashSize() : 0) + (v3 != null ? v3.qualifierHashSize() : 0) + (v4 != null ? v4.endQualifierHashSize() : 0));
        		cq.put(v1.getQualifierHash());
        		if(v2 != null) {
	        		cq.put(v2.getQualifierHash());
	        		if(v3 != null) {
		        		cq.put(v3.getQualifierHash());
		        		if(v4 != null) {
		        			cq.put(v4.getEndQualifierHash());
		        		}
	        		}
        		}
                break;
            default:
            	throw new AssertionError(String.format("Invalid prefix: %s", prefix));
        }
        return cq.array();
    }

	private static byte[] value(byte prefix, RDFValue<?> v1, RDFValue<?> v2, RDFValue<?> v3, RDFValue<?> v4) {
		ByteBuffer cv;
		switch (prefix) {
			case SPO_PREFIX:
				cv = ByteBuffer.allocate(len(v1, 2) + len(v2, 2) + len(v3, 4) + (v4 != null ? len(v4, 0) : 0));
				putShortRDFValue(cv, v1);
				putShortRDFValue(cv, v2);
				putIntRDFValue(cv, v3);
				if (v4 != null) {
					putLastRDFValue(cv, v4);
				}
				break;
			case POS_PREFIX:
				cv = ByteBuffer.allocate(len(v1, 2) + len(v2, 4) + len(v3, 2) + (v4 != null ? len(v4, 0) : 0));
				putShortRDFValue(cv, v1);
				putIntRDFValue(cv, v2);
				putShortRDFValue(cv, v3);
				if (v4 != null) {
					putLastRDFValue(cv, v4);
				}
				break;
			case OSP_PREFIX:
				cv = ByteBuffer.allocate(len(v1, 4) + len(v2, 2) + len(v3, 2) + (v4 != null ? len(v4, 0) : 0));
				putIntRDFValue(cv, v1);
				putShortRDFValue(cv, v2);
				putShortRDFValue(cv, v3);
				if (v4 != null) {
					putLastRDFValue(cv, v4);
				}
				break;

			case CSPO_PREFIX:
				cv = ByteBuffer.allocate(len(v1, 2) + len(v2, 2) + len(v3, 2) + len(v4, 0));
				putShortRDFValue(cv, v1);
				putShortRDFValue(cv, v2);
				putShortRDFValue(cv, v3);
				putLastRDFValue(cv, v4);
				break;
			case CPOS_PREFIX:
				cv = ByteBuffer.allocate(len(v1, 2) + len(v2, 2) + len(v3, 4) + len(v4, 0));
				putShortRDFValue(cv, v1);
				putShortRDFValue(cv, v2);
				putIntRDFValue(cv, v3);
				putLastRDFValue(cv, v4);
				break;
			case COSP_PREFIX:
				cv = ByteBuffer.allocate(len(v1, 2) + len(v2, 4) + len(v3, 2) + len(v4, 0));
				putShortRDFValue(cv, v1);
				putIntRDFValue(cv, v2);
				putShortRDFValue(cv, v3);
				putLastRDFValue(cv, v4);
				break;
			default:
				throw new AssertionError(String.format("Invalid prefix: %s", prefix));
		}
		return cv.array();
	}

	private static int len(RDFValue<?> v, int sizeLen) {
		if (WELL_KNOWN_IRIS.containsValue(v.val)) {
			return 1;
		} else {
			return sizeLen + v.getSerializedForm().length;
		}
	}

	private static void putShortRDFValue(ByteBuffer cv, RDFValue<?> v) {
		if (WELL_KNOWN_IRIS.containsValue(v.val)) {
			cv.put(WELL_KNOWN_IRI_MARKER);
		} else {
			byte[] ser = v.getSerializedForm();
			cv.putShort((short) ser.length).put(ser);
		}
	}

	private static void putIntRDFValue(ByteBuffer cv, RDFValue<?> v) {
		if (WELL_KNOWN_IRIS.containsValue(v.val)) {
			cv.put(WELL_KNOWN_IRI_MARKER);
		} else {
			byte[] ser = v.getSerializedForm();
			cv.putInt(ser.length).put(ser);
		}
	}

	private static void putLastRDFValue(ByteBuffer cv, RDFValue<?> v) {
		if (WELL_KNOWN_IRIS.containsValue(v.val)) {
			cv.put(WELL_KNOWN_IRI_MARKER);
		} else {
			byte[] ser = v.getSerializedForm();
			cv.put(ser);
		}
	}

	/**
	 * Timestamp is shifted one bit left and the last bit is used to prioritize
	 * between inserts and deletes of the same time to avoid HBase ambiguity inserts
	 * are considered always later after deletes on a timeline.
	 * @param ts timestamp
	 * @param insert true if timestamp of an 'insert'
	 * @return Halyard internal timestamp value
	 */
	public static long toHalyardTimestamp(long ts, boolean insert) {
		// use arithmetic operations instead of bit-twiddling to correctly handle
		// negative timestamps
		long hts = 2 * ts;
		if (insert) {
			hts += 1;
		}
		return hts;
	}

	public static long fromHalyardTimestamp(long hts) {
		return hts >> 1; // NB: preserve sign
	}

    /**
     * Method constructing HBase Scan from a Statement pattern hashes, any of the arguments can be null
     * @param subj optional subject Resource
     * @param pred optional predicate IRI
     * @param obj optional object Value
     * @param ctx optional context Resource
     * @return HBase Scan instance to retrieve all data potentially matching the Statement pattern
     */
	public static Scan scan(RDFSubject subj, RDFPredicate pred, RDFObject obj, RDFContext ctx) {
		if (ctx == null) {
			if (subj == null) {
				if (pred == null) {
					if (obj == null) {
						return scan3_0(SPO_PREFIX, RDFSubject.STOP_KEY, RDFPredicate.STOP_KEY, RDFObject.END_STOP_KEY);
                    } else {
						return scan3_1(OSP_PREFIX, obj, RDFSubject.STOP_KEY, RDFPredicate.END_STOP_KEY);
                    }
                } else {
					if (obj == null) {
						return scan3_1(POS_PREFIX, pred, RDFObject.STOP_KEY, RDFSubject.END_STOP_KEY);
                    } else {
						return scan3_2(POS_PREFIX, pred, obj, RDFSubject.END_STOP_KEY);
                    }
                }
            } else {
				if (pred == null) {
					if (obj == null) {
						return scan3_1(SPO_PREFIX, subj, RDFPredicate.STOP_KEY, RDFObject.END_STOP_KEY);
                    } else {
						return scan3_2(OSP_PREFIX, obj, subj, RDFPredicate.END_STOP_KEY);
                    }
                } else {
					if (obj == null) {
						return scan3_2(SPO_PREFIX, subj, pred, RDFObject.END_STOP_KEY);
                    } else {
						return scan3_3(SPO_PREFIX, subj, pred, obj);
                    }
                }
            }
        } else {
			if (subj == null) {
				if (pred == null) {
					if (obj == null) {
						return scan4_1(CSPO_PREFIX, ctx, RDFSubject.STOP_KEY, RDFPredicate.STOP_KEY, RDFObject.END_STOP_KEY);
                    } else {
						return scan4_2(COSP_PREFIX, ctx, obj, RDFSubject.STOP_KEY, RDFPredicate.END_STOP_KEY);
                    }
                } else {
					if (obj == null) {
						return scan4_2(CPOS_PREFIX, ctx, pred, RDFObject.STOP_KEY, RDFSubject.END_STOP_KEY);
                    } else {
						return scan4_3(CPOS_PREFIX, ctx, pred, obj, RDFSubject.END_STOP_KEY);
                    }
                }
            } else {
				if (pred == null) {
					if (obj == null) {
						return scan4_2(CSPO_PREFIX, ctx, subj, RDFPredicate.STOP_KEY, RDFObject.END_STOP_KEY);
                    } else {
						return scan4_3(COSP_PREFIX, ctx, obj, subj, RDFPredicate.END_STOP_KEY);
                    }
                } else {
					if (obj == null) {
						return scan4_3(CSPO_PREFIX, ctx, subj, pred, RDFObject.END_STOP_KEY);
                    } else {
						return scan4_4(CSPO_PREFIX, ctx, subj, pred, obj);
                    }
                }
            }
        }
    }

	public static Resource getSubject(Table table, byte[] id, ValueFactory vf) throws IOException {
		TripleFactory tf = new TripleFactory(table);
		Scan scan = scan(RDFRole.SUBJECT, SPO_PREFIX, id, RDFPredicate.STOP_KEY, RDFObject.END_STOP_KEY);
		for(Result result : table.getScanner(scan)) {
			Cell[] cells = result.rawCells();
			if(cells != null && cells.length > 0) {
				Statement stmt = parseStatement(null, null, null, null, cells[0], vf, tf);
				return stmt.getSubject();
			}
		}
		return null;
	}

	public static IRI getPredicate(Table table, byte[] id, ValueFactory vf) throws IOException {
		Scan scan = scan(RDFRole.PREDICATE, POS_PREFIX, id, RDFObject.STOP_KEY, RDFSubject.END_STOP_KEY);
		for(Result result : table.getScanner(scan)) {
			Cell[] cells = result.rawCells();
			if(cells != null && cells.length > 0) {
				Statement stmt = parseStatement(null, null, null, null, cells[0], vf, null);
				return stmt.getPredicate();
			}
		}
		return null;
	}

	public static Value getObject(Table table, byte[] id, ValueFactory vf) throws IOException {
		TripleFactory tf = new TripleFactory(table);
		Scan scan = scan(RDFRole.OBJECT, OSP_PREFIX, id, RDFSubject.STOP_KEY, RDFPredicate.END_STOP_KEY);
		for(Result result : table.getScanner(scan)) {
			Cell[] cells = result.rawCells();
			if(cells != null && cells.length > 0) {
				Statement stmt = parseStatement(null, null, null, null, cells[0], vf, tf);
				return stmt.getObject();
			}
		}
		return null;
	}

	private static Scan scan(RDFRole role, byte prefix, byte[] id, byte[] stopKey2, byte[] stopKey3) {
		byte[] keyHash = role.keyHash(prefix, id);
		byte[] qualifierHash = role.qualifierHash(id);
		return scan(concat(prefix, false, keyHash), concat(prefix, true, keyHash, stopKey2, stopKey3, RDFContext.STOP_KEY))
			.setFilter(new FilterList(new ColumnPrefixFilter(qualifierHash), new FirstKeyOnlyFilter())).setOneRowLimit();
	}

	/**
	 * Parser method returning all Statements from a single HBase Scan Result
	 * 
     * @param subj subject if known
     * @param pred predicate if known
     * @param obj object if known
     * @param ctx context if known
	 * @param res HBase Scan Result
	 * @param vf ValueFactory
	 * @return List of Statements
	 */
    public static List<Statement> parseStatements(RDFSubject subj, RDFPredicate pred, RDFObject obj, RDFContext ctx, Result res, ValueFactory vf, TripleFactory tf) throws IOException {
    	// multiple triples may have the same hash (i.e. row key)
		List<Statement> st;
		Cell[] cells = res.rawCells();
		if (cells != null && cells.length > 0) {
			if (cells.length == 1) {
				st = Collections.singletonList(parseStatement(subj, pred, obj, ctx, cells[0], vf, tf));
			} else {
				st = new ArrayList<>(cells.length);
				for (Cell c : cells) {
					st.add(parseStatement(subj, pred, obj, ctx, c, vf, tf));
				}
			}
		} else {
			st = Collections.emptyList();
		}
		return st;
    }

    /**
	 * Parser method returning Statement from a single HBase Result Cell
	 * 
     * @param subj subject if known
     * @param pred predicate if known
     * @param obj object if known
     * @param ctx context if known
	 * @param cell HBase Result Cell
	 * @param vf ValueFactory
	 * @return Statements
	 */
    public static Statement parseStatement(RDFSubject subj, RDFPredicate pred, RDFObject obj, RDFContext ctx, Cell cell, ValueFactory vf, TripleFactory tf) throws IOException {
    	ByteBuffer key = ByteBuffer.wrap(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
        ByteBuffer cn = ByteBuffer.wrap(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
        ByteBuffer cv = ByteBuffer.wrap(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
        Resource s;
        IRI p;
        Value o;
        Resource c;
    	byte prefix = key.get();
        switch(prefix) {
        	case SPO_PREFIX:
        		s = parseShortRDFValue(prefix, RDFRole.SUBJECT, subj, key, cn, cv, RDFSubject.KEY_SIZE, vf, tf);
        		p = parseShortRDFValue(prefix, RDFRole.PREDICATE, pred, key, cn, cv, RDFPredicate.KEY_SIZE, vf, null);
        		o = parseIntRDFValue(prefix, RDFRole.OBJECT, obj, key, cn, cv, RDFObject.END_KEY_SIZE, vf, tf);
        		c = parseLastRDFValue(prefix, RDFRole.CONTEXT, ctx, key, cn, cv, RDFContext.KEY_SIZE, vf, null);
                break;
        	case POS_PREFIX:
        		p = parseShortRDFValue(prefix, RDFRole.PREDICATE, pred, key, cn, cv, RDFPredicate.KEY_SIZE, vf, null);
        		o = parseIntRDFValue(prefix, RDFRole.OBJECT, obj, key, cn, cv, RDFObject.KEY_SIZE, vf, tf);
        		s = parseShortRDFValue(prefix, RDFRole.SUBJECT, subj, key, cn, cv, RDFSubject.END_KEY_SIZE, vf, tf);
        		c = parseLastRDFValue(prefix, RDFRole.CONTEXT, ctx, key, cn, cv, RDFContext.KEY_SIZE, vf, null);
                break;
        	case OSP_PREFIX:
        		o = parseIntRDFValue(prefix, RDFRole.OBJECT, obj, key, cn, cv, RDFObject.KEY_SIZE, vf, tf);
        		s = parseShortRDFValue(prefix, RDFRole.SUBJECT, subj, key, cn, cv, RDFSubject.KEY_SIZE, vf, tf);
        		p = parseShortRDFValue(prefix, RDFRole.PREDICATE, pred, key, cn, cv, RDFPredicate.END_KEY_SIZE, vf, null);
        		c = parseLastRDFValue(prefix, RDFRole.CONTEXT, ctx, key, cn, cv, RDFContext.KEY_SIZE, vf, null);
                break;
        	case CSPO_PREFIX:
        		c = parseShortRDFValue(prefix, RDFRole.CONTEXT, ctx, key, cn, cv, RDFContext.KEY_SIZE, vf, null);
        		s = parseShortRDFValue(prefix, RDFRole.SUBJECT, subj, key, cn, cv, RDFSubject.KEY_SIZE, vf, tf);
        		p = parseShortRDFValue(prefix, RDFRole.PREDICATE, pred, key, cn, cv, RDFPredicate.KEY_SIZE, vf, null);
        		o = parseLastRDFValue(prefix, RDFRole.OBJECT, obj, key, cn, cv, RDFObject.END_KEY_SIZE, vf, tf);
                break;
        	case CPOS_PREFIX:
        		c = parseShortRDFValue(prefix, RDFRole.CONTEXT, ctx, key, cn, cv, RDFContext.KEY_SIZE, vf, null);
        		p = parseShortRDFValue(prefix, RDFRole.PREDICATE, pred, key, cn, cv, RDFPredicate.KEY_SIZE, vf, null);
        		o = parseIntRDFValue(prefix, RDFRole.OBJECT, obj, key, cn, cv, RDFObject.KEY_SIZE, vf, tf);
        		s = parseLastRDFValue(prefix, RDFRole.SUBJECT, subj, key, cn, cv, RDFSubject.END_KEY_SIZE, vf, tf);
                break;
        	case COSP_PREFIX:
        		c = parseShortRDFValue(prefix, RDFRole.CONTEXT, ctx, key, cn, cv, RDFContext.KEY_SIZE, vf, null);
        		o = parseIntRDFValue(prefix, RDFRole.OBJECT, obj, key, cn, cv, RDFObject.KEY_SIZE, vf, tf);
        		s = parseShortRDFValue(prefix, RDFRole.SUBJECT, subj, key, cn, cv, RDFSubject.KEY_SIZE, vf, tf);
        		p = parseLastRDFValue(prefix, RDFRole.PREDICATE, pred, key, cn, cv, RDFPredicate.END_KEY_SIZE, vf, null);
                break;
            default:
            	throw new AssertionError(String.format("Invalid prefix: %s", prefix));
        }

        Statement stmt;
		if (c == null) {
			stmt = vf.createStatement(s, p, o);
		} else {
			stmt = vf.createStatement(s, p, o, c);
		}
		if (stmt instanceof Timestamped) {
			((Timestamped) stmt).setTimestamp(fromHalyardTimestamp(cell.getTimestamp()));
        }
		return stmt;
    }

    private static <V extends Value> V parseShortRDFValue(byte prefix, RDFRole role, RDFValue<V> pattern, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, int keySize, ValueFactory vf, TripleFactory tf) throws IOException {
    	byte marker = cv.get(cv.position()); // peek
    	int len;
    	if (marker == WELL_KNOWN_IRI_MARKER) {
    		len = cv.get();
    	} else {
    		len = cv.getShort();
    	}
   		return parseRDFValue(prefix, role, pattern, key, cn, cv, keySize, ID_SIZE, len, vf, tf);
    }

    private static <V extends Value> V parseIntRDFValue(byte prefix, RDFRole role, RDFValue<V> pattern, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, int keySize, ValueFactory vf, TripleFactory tf) throws IOException {
    	byte marker = cv.get(cv.position()); // peek
    	int len;
    	if (marker == WELL_KNOWN_IRI_MARKER) {
    		len = cv.get();
    	} else {
    		len = cv.getInt();
    	}
   		return parseRDFValue(prefix, role, pattern, key, cn, cv, keySize, ID_SIZE, len, vf, tf);
    }

    private static <V extends Value> V parseLastRDFValue(byte prefix, RDFRole role, RDFValue<V> pattern, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, int keySize, ValueFactory vf, TripleFactory tf) throws IOException {
    	byte marker = cv.hasRemaining() ? cv.get(cv.position()) : 0; // peek
    	int len;
    	if (marker == WELL_KNOWN_IRI_MARKER) {
    		len = cv.get();
    	} else {
    		len = cv.remaining();
    	}
   		return parseRDFValue(prefix, role, pattern, key, cn, cv, keySize, ID_SIZE, len, vf, tf);
    }

    @SuppressWarnings("unchecked")
	private static <V extends Value> V parseRDFValue(byte prefix, RDFRole role, RDFValue<V> pattern, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, int keySize, int idSize, int len, ValueFactory vf, TripleFactory tf) throws IOException {
    	if(pattern != null) {
    		// if we have been given the value then don't bother to read it and skip to the next
    		skipId(key, cn, keySize, idSize);
    		if (len > 0) {
    			cv.position(cv.position() + len);
    		}
			return pattern.val;
    	} else if(len == WELL_KNOWN_IRI_MARKER) {
			ByteBuffer id = parseId(prefix, role, key, cn, keySize, idSize);
			IRI iri = WELL_KNOWN_IRI_IDS.get(id);
			if (iri == null) {
				throw new IllegalStateException(String.format("Unknown IRI hash: %s", encode(id)));
			}
			return (V) iri;
		} else if(len > 0) {
			ByteBuffer id = parseId(prefix, role, key, cn, keySize, idSize);
			int limit = cv.limit();
			cv.limit(cv.position() + len);
			V value = (V) readValue(cv, vf, tf);
			cv.limit(limit);
			if (value instanceof Identifiable) {
				((Identifiable)value).setId(id.array());
			}
			return value;
		} else if(len == 0) {
			return null;
		} else {
			throw new AssertionError(String.format("Invalid RDF value length: %d", len));
		}
    }

	private static ByteBuffer parseId(byte prefix, RDFRole role, ByteBuffer key, ByteBuffer cn, int keySize, int idSize) {
		ByteBuffer id = ByteBuffer.allocate(idSize);
		role.rotateLeft(key.array(), key.arrayOffset()+key.position(), keySize, prefix, id.array());
		key.position(key.position()+keySize);
		int cnLimit = cn.limit();
		cn.limit(cn.position() + idSize - keySize);
		id.position(keySize);
		id.put(cn).flip();
		cn.limit(cnLimit);
		return id;
	}

	private static void skipId(ByteBuffer key, ByteBuffer cn, int keySize, int idSize) {
		key.position(key.position() + keySize);
		cn.position(cn.position() + idSize - keySize);
	}

	/**
     * Helper method constructing a custom HBase Scan from given arguments
     * @param startRow start row key byte array
     * @param stopRow stop row key byte array
     * @return HBase Scan instance
     */
	public static Scan scan(byte[] startRow, byte[] stopRow) {
        Scan scan = new Scan();
        scan.addFamily(CF_NAME);
		scan.readVersions(READ_VERSIONS);
        scan.setAllowPartialResults(true);
        scan.setBatch(10);
        if(startRow != null) {
			scan.withStartRow(startRow);
        }
        if(stopRow != null) {
			scan.withStopRow(stopRow);
        }
        return scan;
    }

    /**
     * Helper method concatenating keys
     * @param prefix key prefix byte
     * @param trailingZero boolean switch adding trailing zero to the resulting key
     * @param fragments variable number of the key fragments as byte arrays
     * @return concatenated key as byte array
     */
    public static byte[] concat(byte prefix, boolean trailingZero, byte[]... fragments) {
        int i = 1;
        for (byte[] fr : fragments) {
            i += fr.length;
        }
        byte[] res = new byte[trailingZero ? i + 1 : i];
        res[0] = prefix;
        i = 1;
        for (byte[] fr : fragments) {
            System.arraycopy(fr, 0, res, i, fr.length);
            i += fr.length;
        }
        if (trailingZero) {
            res[res.length - 1] = 0;
        }
        return res;
    }

// private methods

	private static ColumnFamilyDescriptor createColumnFamily() {
		return ColumnFamilyDescriptorBuilder.newBuilder(CF_NAME)
                .setMaxVersions(1)
                .setBlockCacheEnabled(true)
                .setBloomFilterType(BloomType.ROW)
                .setCompressionType(DEFAULT_COMPRESSION_ALGORITHM)
                .setDataBlockEncoding(DEFAULT_DATABLOCK_ENCODING)
                .setCacheBloomsOnWrite(true)
                .setCacheDataOnWrite(true)
                .setCacheIndexesOnWrite(true)
                .setKeepDeletedCells(KeepDeletedCells.FALSE)
				.build();
    }

	public static byte[] id(Value v) {
		byte[] hash;
		Identifiable idValue;

		if (v instanceof Identifiable) {
			idValue = (Identifiable) v;
			hash = idValue.getId();
		} else {
			idValue = null;
			ByteBuffer id = WELL_KNOWN_IRI_IDS.inverse().get(v);
			if (id != null) {
				hash = new byte[ID_SIZE];
    			// NB: do not alter original hash buffer which is shared across threads
				id.duplicate().get(hash);
			} else {
				hash = null;
			}
		}

		boolean alreadyHasHash = (hash != null);
		if (!alreadyHasHash) {
			hash = hashUnique(v.toString().getBytes(StandardCharsets.UTF_8));
			// literal prefix
			if (v instanceof Literal) {
				hash[0] &= 0x7F; // 0 msb
			} else {
				hash[0] |= NON_LITERAL_FLAG; // 1 msb
			}
		}

		if (idValue != null && !alreadyHasHash) {
			idValue.setId(hash);
		}
		return hash;
	}

	static boolean isLiteral(byte[] hash) {
		return (hash[0] & NON_LITERAL_FLAG) == 0;
	}

    public static String encode(byte b[]) {
        return ENCODER.encodeToString(b);
    }

    public static byte[] decode(String s) {
    	return DECODER.decode(s);
    }

    /**
	 * NB: this alters the buffer.
	 */
	private static CharSequence encode(ByteBuffer b) {
		return StandardCharsets.UTF_8.decode(ENCODER.encode(b));
	}

	private static Scan scan3_0(byte prefix, byte[] stopKey1, byte[] stopKey2, byte[] stopKey3) {
		return scan(concat(prefix, false), concat(prefix, true, stopKey1, stopKey2, stopKey3, RDFContext.STOP_KEY));
	}

	private static Scan scan3_1(byte prefix, RDFIdentifier key1, byte[] stopKey2, byte[] stopKey3) {
		return scan(concat(prefix, false, key1.getKeyHash(prefix)), concat(prefix, true, key1.getKeyHash(prefix), stopKey2, stopKey3, RDFContext.STOP_KEY))
			.setFilter(new ColumnPrefixFilter(qualifier(prefix, key1, null, null, null)));
	}

	private static Scan scan3_2(byte prefix, RDFIdentifier key1, RDFIdentifier key2, byte[] stopKey3) {
		return scan(concat(prefix, false, key1.getKeyHash(prefix), key2.getKeyHash(prefix)), concat(prefix, true, key1.getKeyHash(prefix), key2.getKeyHash(prefix), stopKey3, RDFContext.STOP_KEY))
			.setFilter(new ColumnPrefixFilter(qualifier(prefix, key1, key2, null, null)));
	}

	private static Scan scan3_3(byte prefix, RDFIdentifier key1, RDFIdentifier key2, RDFIdentifier key3) {
		return scan(concat(prefix, false, key1.getKeyHash(prefix), key2.getKeyHash(prefix), key3.getEndKeyHash(prefix)), concat(prefix, true, key1.getKeyHash(prefix), key2.getKeyHash(prefix), key3.getEndKeyHash(prefix), RDFContext.STOP_KEY))
			.setFilter(new ColumnPrefixFilter(qualifier(prefix, key1, key2, key3, null)));
	}

	private static Scan scan4_1(byte prefix, RDFIdentifier key1, byte[] stopKey2, byte[] stopKey3, byte[] stopKey4) {
		return scan(concat(prefix, false, key1.getKeyHash(prefix)), concat(prefix, true, key1.getKeyHash(prefix), stopKey2, stopKey3, stopKey4)).setFilter(new ColumnPrefixFilter(qualifier(prefix, key1, null, null, null)));
    }

	private static Scan scan4_2(byte prefix, RDFIdentifier key1, RDFIdentifier key2, byte[] stopKey3, byte[] stopKey4) {
		return scan(concat(prefix, false, key1.getKeyHash(prefix), key2.getKeyHash(prefix)), concat(prefix, true, key1.getKeyHash(prefix), key2.getKeyHash(prefix), stopKey3, stopKey4)).setFilter(new ColumnPrefixFilter(qualifier(prefix, key1, key2, null, null)));
    }

	private static Scan scan4_3(byte prefix, RDFIdentifier key1, RDFIdentifier key2, RDFIdentifier key3, byte[] stopKey4) {
		return scan(concat(prefix, false, key1.getKeyHash(prefix), key2.getKeyHash(prefix), key3.getKeyHash(prefix)), concat(prefix, true, key1.getKeyHash(prefix), key2.getKeyHash(prefix), key3.getKeyHash(prefix), stopKey4)).setFilter(new ColumnPrefixFilter(qualifier(prefix, key1, key2, key3, null)));
    }

	private static Scan scan4_4(byte prefix, RDFIdentifier key1, RDFIdentifier key2, RDFIdentifier key3, RDFIdentifier key4) {
		return scan(concat(prefix, false, key1.getKeyHash(prefix), key2.getKeyHash(prefix), key3.getKeyHash(prefix), key4.getEndKeyHash(prefix)), concat(prefix, true, key1.getKeyHash(prefix), key2.getKeyHash(prefix), key3.getKeyHash(prefix), key4.getEndKeyHash(prefix)))
			.setFilter(new ColumnPrefixFilter(qualifier(prefix, key1, key2, key3, key4)));
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
    		byte[] b = new byte[1+3*HalyardTableUtils.ID_SIZE];
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
		byte[] sid = id(subj);
		System.arraycopy(sid, 0, b, i, ID_SIZE);
		i += ID_SIZE;
		byte[] pid = id(pred);
		System.arraycopy(pid, 0, b, i, ID_SIZE);
		i += ID_SIZE;
		byte[] oid = id(obj);
		System.arraycopy(oid, 0, b, i, ID_SIZE);
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
					throw new IllegalStateException(String.format("Unknown IRI hash: %s", encode(b)));
				}
				b.position(originalLimit);
				return iri;
			case NAMESPACE_HASH_TYPE:
				b.limit(b.position()+2); // 16-byte hash
				String namespace = WELL_KNOWN_NAMESPACES.get(b);
				if (namespace == null) {
					throw new IllegalStateException(String.format("Unknown namespace hash: %s", encode(b)));
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
				byte[] sid = new byte[ID_SIZE];
				byte[] pid = new byte[ID_SIZE];
				byte[] oid = new byte[ID_SIZE];
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

	public static class TripleFactory {
		private final Table table;

		public TripleFactory(Table table) {
			this.table = table;
		}

		public Triple readTriple(byte[] sid, byte[] pid, byte[] oid, ValueFactory vf) throws IOException {
			byte prefix = CSPO_PREFIX;
			RDFContext ckey = RDFContext.create(HALYARD.TRIPLE_GRAPH_CONTEXT);
			RDFIdentifier skey = new RDFIdentifier(RDFRole.SUBJECT, sid);
			RDFIdentifier pkey = new RDFIdentifier(RDFRole.PREDICATE, pid);
			RDFIdentifier okey = new RDFIdentifier(RDFRole.OBJECT, oid);
			Get scan = new Get(concat(CSPO_PREFIX, false, ckey.getKeyHash(prefix), skey.getKeyHash(prefix), pkey.getKeyHash(prefix), okey.getEndKeyHash(prefix)))
				.setFilter(new FilterList(new ColumnPrefixFilter(qualifier(prefix, ckey, skey, pkey, okey)), new FirstKeyOnlyFilter()));
	        scan.addFamily(CF_NAME);
			scan.readVersions(READ_VERSIONS);
			Result result = table.get(scan);
			assert result.rawCells().length == 1;
			Statement stmt = parseStatement(null, null, null, ckey, result.rawCells()[0], vf, this);
			return vf.createTriple(stmt.getSubject(), stmt.getPredicate(), stmt.getObject());
		}
	}
}
