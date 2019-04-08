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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeConstants;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;

import com.google.common.hash.Hashing;

/**
 * Core Halyard utility class performing RDF to HBase mappings and base HBase table and key management. The methods of this class define how
 * Halyard stores and finds data in HBase. This class also provides several constants that define the key encoding.
 *
 * @author Adam Sotona (MSD)
 */
public final class HalyardTableUtils {

    private static final Charset UTF8 = StandardCharsets.UTF_8;
    private static final byte[] EMPTY = new byte[0];
    private static final byte[] CF_NAME = "e".getBytes(UTF8);
    private static final Base64.Encoder ENC = Base64.getUrlEncoder().withoutPadding();

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

    /**
     * Key hash size in bytes
     */
	public static final byte S_KEY_SIZE = 16;
	public static final byte P_KEY_SIZE = 4;
	public static final byte O_KEY_SIZE = 16;
	public static final byte C_KEY_SIZE = 8;

    private static final int PREFIXES = 3;
	private static final byte[] S_START_KEY = new byte[S_KEY_SIZE];
	public static final byte[] S_STOP_KEY = new byte[S_KEY_SIZE];
	private static final byte[] P_START_KEY = new byte[P_KEY_SIZE];
	public static final byte[] P_STOP_KEY = new byte[P_KEY_SIZE];
	private static final byte[] O_START_KEY = new byte[O_KEY_SIZE];
	public static final byte[] O_STOP_KEY = new byte[O_KEY_SIZE];
	private static final byte[] C_START_KEY = new byte[C_KEY_SIZE];
	public static final byte[] C_STOP_KEY = new byte[C_KEY_SIZE];
    static {
		Arrays.fill(S_START_KEY, (byte) 0);
		Arrays.fill(S_STOP_KEY, (byte) 0xff); /* 0xff is 255 in decimal */
		Arrays.fill(P_START_KEY, (byte) 0);
		Arrays.fill(P_STOP_KEY, (byte) 0xff); /* 0xff is 255 in decimal */
		Arrays.fill(O_START_KEY, (byte) 0);
		Arrays.fill(O_STOP_KEY, (byte) 0xff); /* 0xff is 255 in decimal */
		Arrays.fill(C_START_KEY, (byte) 0);
		Arrays.fill(C_STOP_KEY, (byte) 0xff); /* 0xff is 255 in decimal */
    }
    private static final Compression.Algorithm DEFAULT_COMPRESSION_ALGORITHM = Compression.Algorithm.GZ;
    private static final DataBlockEncoding DEFAULT_DATABLOCK_ENCODING = DataBlockEncoding.PREFIX;
    private static final String REGION_MAX_FILESIZE = "10000000000";
    private static final String REGION_SPLIT_POLICY = "org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy";

    private static final ThreadLocal<Map<Integer,MessageDigest>> MDS = new ThreadLocal<Map<Integer,MessageDigest>>(){
        @Override
        protected Map<Integer,MessageDigest> initialValue() {
        	Map<Integer,MessageDigest> mds = new HashMap<>();
        	mds.put(128, getMessageDigest("Skein-256-128"));
        	mds.put(256, getMessageDigest("Skein-256-256"));
        	return Collections.unmodifiableMap(mds);
        }
    };

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
		Literal readBytes(byte[] b, ValueFactory vf);
	}

	private static final Map<IRI, ByteWriter> BYTE_WRITERS = new HashMap<>();
	private static final Map<Byte, ByteReader> BYTE_READERS = new HashMap<>();

	private static final byte IRI_TYPE = '<';
	private static final byte BNODE_TYPE = '_';
	private static final byte FULL_LITERAL_TYPE = '\"';
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
		BYTE_WRITERS.put(XMLSchema.BOOLEAN, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				return new byte[] { l.booleanValue() ? TRUE_TYPE : FALSE_TYPE };
			}
		});
		BYTE_READERS.put(FALSE_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(byte[] b, ValueFactory vf) {
				return vf.createLiteral(false);
			}
		});
		BYTE_READERS.put(TRUE_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(byte[] b, ValueFactory vf) {
				return vf.createLiteral(true);
			}
		});

		BYTE_WRITERS.put(XMLSchema.BYTE, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				return new byte[] { BYTE_TYPE, l.byteValue() };
			}
		});
		BYTE_READERS.put(BYTE_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(byte[] b, ValueFactory vf) {
				return vf.createLiteral(b[1]);
			}
		});

		BYTE_WRITERS.put(XMLSchema.SHORT, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				byte[] b = new byte[3];
				ByteBuffer.wrap(b).put(SHORT_TYPE).putShort(l.shortValue());
				return b;
			}
		});
		BYTE_READERS.put(SHORT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(byte[] b, ValueFactory vf) {
				return vf.createLiteral(ByteBuffer.wrap(b, 1, 2).getShort());
			}
		});

		BYTE_WRITERS.put(XMLSchema.INT, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				byte[] b = new byte[5];
				ByteBuffer.wrap(b).put(INT_TYPE).putInt(l.intValue());
				return b;
			}
		});
		BYTE_READERS.put(INT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(byte[] b, ValueFactory vf) {
				return vf.createLiteral(ByteBuffer.wrap(b, 1, 4).getInt());
			}
		});

		BYTE_WRITERS.put(XMLSchema.LONG, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				byte[] b = new byte[9];
				ByteBuffer.wrap(b).put(LONG_TYPE).putLong(l.longValue());
				return b;
			}
		});
		BYTE_READERS.put(LONG_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(byte[] b, ValueFactory vf) {
				return vf.createLiteral(ByteBuffer.wrap(b, 1, 8).getLong());
			}
		});

		BYTE_WRITERS.put(XMLSchema.FLOAT, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				byte[] b = new byte[5];
				ByteBuffer.wrap(b).put(FLOAT_TYPE).putFloat(l.floatValue());
				return b;
			}
		});
		BYTE_READERS.put(FLOAT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(byte[] b, ValueFactory vf) {
				return vf.createLiteral(ByteBuffer.wrap(b, 1, 4).getFloat());
			}
		});

		BYTE_WRITERS.put(XMLSchema.DOUBLE, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				byte[] b = new byte[9];
				ByteBuffer.wrap(b).put(DOUBLE_TYPE).putDouble(l.doubleValue());
				return b;
			}
		});
		BYTE_READERS.put(DOUBLE_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(byte[] b, ValueFactory vf) {
				return vf.createLiteral(ByteBuffer.wrap(b, 1, 8).getDouble());
			}
		});

		BYTE_WRITERS.put(XMLSchema.STRING, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				return new StringBuilder().append((char)STRING_TYPE).append(l.getLabel()).toString().getBytes(UTF8);
			}
		});
		BYTE_READERS.put(STRING_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(byte[] b, ValueFactory vf) {
				return vf.createLiteral(new String(b, 1, b.length-1, UTF8));
			}
		});

		BYTE_WRITERS.put(XMLSchema.TIME, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				return calendarTypeToBytes(TIME_TYPE, l.calendarValue());
			}
		});
		BYTE_READERS.put(TIME_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(byte[] b, ValueFactory vf) {
				long millis = ByteBuffer.wrap(b, 1, 8).getLong();
				GregorianCalendar c = new GregorianCalendar();
				c.setTimeInMillis(millis);
				XMLGregorianCalendar cal = DATATYPE_FACTORY.newXMLGregorianCalendar(c);
				cal.setYear(null);
				cal.setMonth(DatatypeConstants.FIELD_UNDEFINED);
				cal.setDay(DatatypeConstants.FIELD_UNDEFINED);
				int tz = ByteBuffer.wrap(b, 9, 2).getShort();
				if(tz == Short.MIN_VALUE) {
					tz = DatatypeConstants.FIELD_UNDEFINED;
				}
				cal.setTimezone(tz);
				return vf.createLiteral(cal);
			}
		});

		BYTE_WRITERS.put(XMLSchema.DATE, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				return calendarTypeToBytes(DATE_TYPE, l.calendarValue());
			}
		});
		BYTE_READERS.put(DATE_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(byte[] b, ValueFactory vf) {
				long millis = ByteBuffer.wrap(b, 1, 8).getLong();
				GregorianCalendar c = new GregorianCalendar();
				c.setTimeInMillis(millis);
				XMLGregorianCalendar cal = DATATYPE_FACTORY.newXMLGregorianCalendar(c);
				cal.setTime(DatatypeConstants.FIELD_UNDEFINED, DatatypeConstants.FIELD_UNDEFINED, DatatypeConstants.FIELD_UNDEFINED, null);
				int tz = ByteBuffer.wrap(b, 9, 2).getShort();
				if(tz == Short.MIN_VALUE) {
					tz = DatatypeConstants.FIELD_UNDEFINED;
				}
				cal.setTimezone(tz);
				return vf.createLiteral(cal);
			}
		});

		BYTE_WRITERS.put(XMLSchema.DATETIME, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				return calendarTypeToBytes(DATETIME_TYPE, l.calendarValue());
			}
		});
		BYTE_READERS.put(DATETIME_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(byte[] b, ValueFactory vf) {
				long millis = ByteBuffer.wrap(b, 1, 8).getLong();
				GregorianCalendar c = new GregorianCalendar();
				c.setTimeInMillis(millis);
				XMLGregorianCalendar cal = DATATYPE_FACTORY.newXMLGregorianCalendar(c);
				int tz = ByteBuffer.wrap(b, 9, 2).getShort();
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

	static {
		System.setProperty("org.bouncycastle.ec.disable_mqv", "true");
		Security.addProvider(new BouncyCastleProvider());
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
	public static Table getTable(Configuration config, String tableName, boolean create, int splitBits)
			throws IOException {
		return getTable(getConnection(config), tableName, create, splitBits);
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
		return getTable(conn, tableName, create, splitBits < 0 ? null : calculateSplits(splitBits));
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
					HTableDescriptor td = new HTableDescriptor(htableName);
					td.addFamily(createColumnFamily());
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
     * Calculates the split keys (one for each purmutation of the CSPO HBase Key prefix).
     * @param splitBits must be between 0 and 16, larger values result in more keys.
     * @return An array of keys represented as {@code byte[]}s
     */
    static byte[][] calculateSplits(int splitBits) {
        TreeSet<byte[]> splitKeys = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        //basic presplits
        splitKeys.add(new byte[]{POS_PREFIX});
        splitKeys.add(new byte[]{OSP_PREFIX});
        splitKeys.add(new byte[]{CSPO_PREFIX});
        splitKeys.add(new byte[]{CPOS_PREFIX});
        splitKeys.add(new byte[]{COSP_PREFIX});
        //common presplits
        addSplits(splitKeys, new byte[]{SPO_PREFIX}, splitBits);
        addSplits(splitKeys, new byte[]{POS_PREFIX}, splitBits);
        addSplits(splitKeys, new byte[]{OSP_PREFIX}, splitBits);
        return splitKeys.toArray(new byte[splitKeys.size()][]);
    }

    /**
     * Generate the split key and add it to the collection
     * @param splitKeys the {@code TreeSet} to add the collection to.
     * @param prefix the prefix to calculate the key for
     * @param splitBits between 0 and 16, larger values generate smaller split steps
     */
    private static void addSplits(TreeSet<byte[]> splitKeys, byte[] prefix, int splitBits) {
        if (splitBits == 0) return;
        if (splitBits < 0 || splitBits > 16) throw new IllegalArgumentException("Illegal nunmber of split bits");

        final int splitStep = 1 << (16 - splitBits); //1 splitBit gives a split step of 32768, 8 splitBits gives a split step of 256
        for (int i = splitStep; i <= 0xFFFF; i += splitStep) { // 0xFFFF is 65535 so a split step of 32768 will give 2 iterations, larger split bits give more iterations
            byte bb[] = Arrays.copyOf(prefix, prefix.length + 2);
            bb[prefix.length] = (byte)((i >> 8) & 0xff); //0xff = 255.
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
     * @return array of KeyValues
     */
    public static KeyValue[] toKeyValues(Resource subj, IRI pred, Value obj, Resource context, boolean delete, long timestamp) {
		byte[] sb = writeBytes(subj); // subject bytes
		byte[] pb = writeBytes(pred); // predicate bytes
		byte[] ob = writeBytes(obj); // object bytes
		byte[] cb = context == null ? new byte[0] : writeBytes(context); // context (graph) bytes
        byte[] sKey = hashSubject(sb);  //subject key
        byte[] pKey = hashPredicate(pb);  //predicate key
        byte[] oKey = hashObject(ob);  //object key

        //bytes to be used for the HBase column qualifier
		byte[] cq = ByteBuffer.allocate(sb.length + pb.length + ob.length + cb.length + 8).putShort((short) sb.length).putShort((short) pb.length).putInt(ob.length).put(sb).put(pb).put(ob).put(cb).array();

        KeyValue kv[] =  new KeyValue[context == null ? PREFIXES : 2 * PREFIXES];

        KeyValue.Type type = delete ? KeyValue.Type.DeleteColumn : KeyValue.Type.Put;

		timestamp = toHalyardTimestamp(timestamp, !delete);

        //generate HBase key value pairs from: row, family, qualifier, value. Permutations of SPO (and if needed CSPO) are all stored. Values are actually empty.
        kv[0] = new KeyValue(concat(SPO_PREFIX, false, sKey, pKey, oKey), CF_NAME, cq, timestamp, type, EMPTY);
        kv[1] = new KeyValue(concat(POS_PREFIX, false, pKey, oKey, sKey), CF_NAME, cq, timestamp, type, EMPTY);
        kv[2] = new KeyValue(concat(OSP_PREFIX, false, oKey, sKey, pKey), CF_NAME, cq, timestamp, type, EMPTY);
        if (context != null) {
            byte[] cKey = hashContext(cb);
            kv[3] = new KeyValue(concat(CSPO_PREFIX, false, cKey, sKey, pKey, oKey), CF_NAME, cq, timestamp, type, EMPTY);
            kv[4] = new KeyValue(concat(CPOS_PREFIX, false, cKey, pKey, oKey, sKey), CF_NAME, cq, timestamp, type, EMPTY);
            kv[5] = new KeyValue(concat(COSP_PREFIX, false, cKey, oKey, sKey, pKey), CF_NAME, cq, timestamp, type, EMPTY);
        }
        return kv;
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
     * Method constructing HBase Scan from a Statement pattern, any of the arguments can be null
     * @param subj optional subject Resource
     * @param pred optional predicate IRI
     * @param obj optional object Value
     * @param ctx optional context Resource
     * @return HBase Scan instance to retrieve all data potentially matching the Statement pattern
     */
    public static Scan scan(Resource subj, IRI pred, Value obj, Resource ctx) {
        return scan(hashSubject(subj), hashPredicate(pred), hashObject(obj), hashContext(ctx));
    }
    /**
     * Method constructing HBase Scan from a Statement pattern hashes, any of the arguments can be null
     * @param subjHash optional subject Resource hash
     * @param predHash optional predicate IRI hash
     * @param objHash optional object Value hash
     * @param ctxHash optional context Resource hash
     * @return HBase Scan instance to retrieve all data potentially matching the Statement pattern
     */
    public static Scan scan(byte[] subjHash, byte[] predHash, byte[] objHash, byte[] ctxHash) {
        if (ctxHash == null) {
            if (subjHash == null) {
                if (predHash == null) {
                    if (objHash == null) {
						return scan3_0(SPO_PREFIX, S_STOP_KEY, P_STOP_KEY, O_STOP_KEY);
                    } else {
						return scan3_1(OSP_PREFIX, objHash, S_STOP_KEY, P_STOP_KEY);
                    }
                } else {
                    if (objHash == null) {
						return scan3_1(POS_PREFIX, predHash, O_STOP_KEY, S_STOP_KEY);
                    } else {
						return scan3_2(POS_PREFIX, predHash, objHash, S_STOP_KEY);
                    }
                }
            } else {
                if (predHash == null) {
                    if (objHash == null) {
						return scan3_1(SPO_PREFIX, subjHash, P_STOP_KEY, O_STOP_KEY);
                    } else {
						return scan3_2(OSP_PREFIX, objHash, subjHash, P_STOP_KEY);
                    }
                } else {
                    if (objHash == null) {
						return scan3_2(SPO_PREFIX, subjHash, predHash, O_STOP_KEY);
                    } else {
						return scan3_3(SPO_PREFIX, subjHash, predHash, objHash);
                    }
                }
            }
        } else {
            if (subjHash == null) {
                if (predHash == null) {
                    if (objHash == null) {
						return scan4_1(CSPO_PREFIX, ctxHash, S_STOP_KEY, P_STOP_KEY, O_STOP_KEY);
                    } else {
						return scan4_2(COSP_PREFIX, ctxHash, objHash, S_STOP_KEY, P_STOP_KEY);
                    }
                } else {
                    if (objHash == null) {
						return scan4_2(CPOS_PREFIX, ctxHash, predHash, O_STOP_KEY, S_STOP_KEY);
                    } else {
						return scan4_3(CPOS_PREFIX, ctxHash, predHash, objHash, S_STOP_KEY);
                    }
                }
            } else {
                if (predHash == null) {
                    if (objHash == null) {
						return scan4_2(CSPO_PREFIX, ctxHash, subjHash, P_STOP_KEY, O_STOP_KEY);
                    } else {
						return scan4_3(COSP_PREFIX, ctxHash, objHash, subjHash, P_STOP_KEY);
                    }
                } else {
                    if (objHash == null) {
						return scan4_3(CSPO_PREFIX, ctxHash, subjHash, predHash, O_STOP_KEY);
                    } else {
						return scan4_4(CSPO_PREFIX, ctxHash, subjHash, predHash, objHash);
                    }
                }
            }
        }
    }

    /**
	 * Parser method returning all Statements from a single HBase Scan Result
	 * 
	 * @param res HBase Scan Result
	 * @param vf ValueFactory
	 * @return List of Statements
	 */
    public static List<Statement> parseStatements(Result res, ValueFactory vf) {
    	// multiple triples may have the same hash (i.e. row key)
        ArrayList<Statement> st = new ArrayList<>();
		if (res.rawCells() != null) {
			for (Cell c : res.rawCells()) {
				st.add(parseStatement(c, vf));
			}
		}
        return st;
    }

    /**
	 * Parser method returning Statement from a single HBase Result Cell
	 * 
	 * @param c HBase Result Cell
	 * @param vf ValueFactory
	 * @return Statements
	 */
    public static Statement parseStatement(Cell c, ValueFactory vf) {
        ByteBuffer bb = ByteBuffer.wrap(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength());
		byte[] sb = new byte[bb.getShort()];
		byte[] pb = new byte[bb.getShort()];
        byte[] ob = new byte[bb.getInt()];
        bb.get(sb);
        bb.get(pb);
        bb.get(ob);
        byte[] cb = new byte[bb.remaining()];
        bb.get(cb);
        Resource subj = (Resource) readValue(sb, vf);
        IRI pred = (IRI) readValue(pb, vf);
        Value value = readValue(ob, vf);
		Statement stmt;
		if (cb.length == 0) {
			stmt = vf.createStatement(subj, pred, value);
		} else {
			Resource context = (Resource) readValue(cb, vf);
			stmt = vf.createStatement(subj, pred, value, context);
		}
		if (stmt instanceof Timestamped) {
			((Timestamped) stmt).setTimestamp(fromHalyardTimestamp(c.getTimestamp()));
        }
		return stmt;
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
        scan.setMaxVersions(1);
        scan.setAllowPartialResults(true);
        scan.setBatch(10);
        if(startRow != null) {
        	scan.setStartRow(startRow);
        }
        if(stopRow != null) {
        	scan.setStopRow(stopRow);
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
    public static byte[] concat(byte prefix, boolean trailingZero, byte[]...fragments) {
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

    private static HColumnDescriptor createColumnFamily() {
        return new HColumnDescriptor(CF_NAME)
                .setMaxVersions(1)
                .setBlockCacheEnabled(true)
                .setBloomFilterType(BloomType.ROW)
                .setCompressionType(DEFAULT_COMPRESSION_ALGORITHM)
                .setDataBlockEncoding(DEFAULT_DATABLOCK_ENCODING)
                .setCacheBloomsOnWrite(true)
                .setCacheDataOnWrite(true)
                .setCacheIndexesOnWrite(true)
                .setKeepDeletedCells(KeepDeletedCells.FALSE)
                .setValue(HTableDescriptor.MAX_FILESIZE, REGION_MAX_FILESIZE)
                .setValue(HTableDescriptor.SPLIT_POLICY, REGION_SPLIT_POLICY);
    }

    public static byte[] hashUnique(byte[] key) {
        MessageDigest md = MDS.get().get(256);
        try {
            md.update(key);
            return md.digest();
        } finally {
            md.reset();
        }
    }

    public static byte[] hashSubject(byte[] key) {
    	return digest(key, MDS.get().get(128), S_KEY_SIZE);
    }

    public static byte[] hashPredicate(byte[] key) {
    	return Hashing.murmur3_32().hashBytes(key).asBytes();
    }

    public static byte[] hashObject(byte[] key) {
    	return digest(key, MDS.get().get(128), O_KEY_SIZE);
    }

    public static byte[] hashContext(byte[] key) {
    	return digest(key, MDS.get().get(128), C_KEY_SIZE);
    }

    private static byte[] digest(byte[] key, MessageDigest md, int hashLen) {
    	byte[] hash;
        try {
            md.update(key);
            byte[] digest = md.digest();
            if(hashLen < digest.length) {
            	hash = new byte[hashLen];
            	System.arraycopy(digest, 0, hash, 0, hashLen);
            } else {
            	hash = digest;
            }
            return hash;
		} finally {
            md.reset();
        }
    }

    public static byte[] hashSubject(Resource v) {
		return v == null ? null : hashSubject(writeBytes(v));
    }

    public static byte[] hashPredicate(IRI v) {
		return v == null ? null : hashPredicate(writeBytes(v));
    }

    public static byte[] hashObject(Value v) {
		return v == null ? null : hashObject(writeBytes(v));
    }

    public static byte[] hashContext(Resource v) {
		return v == null ? null : hashContext(writeBytes(v));
    }

    public static String encode(byte b[]) {
        return ENC.encodeToString(b);
    }

	private static Scan scan3_0(byte prefix, byte[] stopKey1, byte[] stopKey2, byte[] stopKey3) {
		return scan(concat(prefix, false), concat(prefix, true, stopKey1, stopKey2, stopKey3));
	}

	private static Scan scan3_1(byte prefix, byte[] key1, byte[] stopKey2, byte[] stopKey3) {
		return scan(concat(prefix, false, key1), concat(prefix, true, key1, stopKey2, stopKey3));
	}

	private static Scan scan3_2(byte prefix, byte[] key1, byte[] key2, byte[] stopKey3) {
		return scan(concat(prefix, false, key1, key2), concat(prefix, true, key1, key2, stopKey3));
	}

	private static Scan scan3_3(byte prefix, byte[] key1, byte[] key2, byte[] key3) {
		return scan(concat(prefix, false, key1, key2, key3), concat(prefix, true, key1, key2, key3));
	}

	private static Scan scan4_1(byte prefix, byte[] key1, byte[] stopKey2, byte[] stopKey3, byte[] stopKey4) {
		return scan(concat(prefix, false, key1), concat(prefix, true, key1, stopKey2, stopKey3, stopKey4));
    }

	private static Scan scan4_2(byte prefix, byte[] key1, byte[] key2, byte[] stopKey3, byte[] stopKey4) {
		return scan(concat(prefix, false, key1, key2), concat(prefix, true, key1, key2, stopKey3, stopKey4));
    }

	private static Scan scan4_3(byte prefix, byte[] key1, byte[] key2, byte[] key3, byte[] stopKey4) {
		return scan(concat(prefix, false, key1, key2, key3), concat(prefix, true, key1, key2, key3, stopKey4));
    }

	private static Scan scan4_4(byte prefix, byte[] key1, byte[] key2, byte[] key3, byte[] key4) {
        return scan(concat(prefix, false, key1, key2, key3, key4), concat(prefix, true, key1, key2, key3, key4));
    }

    public static byte[] writeBytes(Value v) {
    	if (v instanceof IRI) {
    		return ("<"+v.stringValue()+">").getBytes(UTF8);
    	} else if (v instanceof BNode) {
    		return v.toString().getBytes(UTF8);
    	} else if (v instanceof Literal) {
			Literal l = (Literal) v;
			ByteWriter writer = BYTE_WRITERS.get(l.getDatatype());
			if (writer != null) {
				return writer.writeBytes(l);
			} else {
				return l.toString().getBytes(UTF8);
			}
		} else {
			throw new AssertionError(v);
		}
    }

    public static Value readValue(byte[] b, ValueFactory vf) {
		byte type = b[0];
		switch(type) {
			case IRI_TYPE:
				return vf.createIRI(new String(b, 1, b.length-2, UTF8));
			case BNODE_TYPE:
				return vf.createBNode(new String(b, 2, b.length-2, UTF8));
			case FULL_LITERAL_TYPE:
				String s = new String(b, UTF8);
				int endOfLabel = s.lastIndexOf('\"');
				String label = s.substring(1, endOfLabel);
				int startOfLang = s.indexOf('@', endOfLabel+1);
				if(startOfLang != -1) {
					return vf.createLiteral(label, s.substring(startOfLang+1));
				} else {
					int startOfDatatype = s.indexOf("^^<", endOfLabel+1);
					return vf.createLiteral(label, vf.createIRI(s.substring(startOfDatatype+3, s.length()-1)));
				}
			default:
				ByteReader reader = BYTE_READERS.get(type);
				return reader.readBytes(b, vf);
		}
    }
}
