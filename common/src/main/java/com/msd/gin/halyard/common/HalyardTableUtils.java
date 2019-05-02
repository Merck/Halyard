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
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
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

    private static final byte[] EMPTY = new byte[0];
    private static final byte[] CF_NAME = "e".getBytes(StandardCharsets.UTF_8);
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

	private static class MDHolder {
		final MessageDigest md128;
		final MessageDigest md256;

		MDHolder() {
			this.md128 = getMessageDigest("Skein-256-128");
			this.md256 = getMessageDigest("Skein-256-256");
		}
	}

	private static final ThreadLocal<MDHolder> MDS = new ThreadLocal<MDHolder>() {
        @Override
		protected MDHolder initialValue() {
			return new MDHolder();
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
				return new StringBuilder().append((char)STRING_TYPE).append(l.getLabel()).toString().getBytes(StandardCharsets.UTF_8);
			}
		});
		BYTE_READERS.put(STRING_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(byte[] b, ValueFactory vf) {
				return vf.createLiteral(new String(b, 1, b.length-1, StandardCharsets.UTF_8));
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
				ByteBuffer buf = ByteBuffer.wrap(b, 1, 10);
				long millis = buf.getLong();
				int tz = buf.getShort();
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

		BYTE_WRITERS.put(XMLSchema.DATE, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				return calendarTypeToBytes(DATE_TYPE, l.calendarValue());
			}
		});
		BYTE_READERS.put(DATE_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(byte[] b, ValueFactory vf) {
				ByteBuffer buf = ByteBuffer.wrap(b, 1, 10);
				long millis = buf.getLong();
				int tz = buf.getShort();
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

		BYTE_WRITERS.put(XMLSchema.DATETIME, new ByteWriter() {
			@Override
			public byte[] writeBytes(Literal l) {
				return calendarTypeToBytes(DATETIME_TYPE, l.calendarValue());
			}
		});
		BYTE_READERS.put(DATETIME_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(byte[] b, ValueFactory vf) {
				ByteBuffer buf = ByteBuffer.wrap(b, 1, 10);
				long millis = buf.getLong();
				int tz = buf.getShort();
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
    	if(subj == null || pred == null || obj == null) {
    		throw new NullPointerException();
    	}

    	RDFValue<Resource> sb = RDFValue.createSubject(subj); // subject bytes
		RDFValue<IRI> pb = RDFValue.createPredicate(pred); // predicate bytes
		RDFValue<Value> ob = RDFValue.createObject(obj); // object bytes
		RDFValue<Resource> cb = RDFValue.createContext(context); // context (graph) bytes

        KeyValue kv[] =  new KeyValue[context == null ? PREFIXES : 2 * PREFIXES];

        KeyValue.Type type = delete ? KeyValue.Type.DeleteColumn : KeyValue.Type.Put;

		timestamp = toHalyardTimestamp(timestamp, !delete);

        //generate HBase key value pairs from: row, family, qualifier, value. Permutations of SPO (and if needed CSPO) are all stored. Values are actually empty.
        kv[0] = new KeyValue(concat(SPO_PREFIX, false, sb.hash, pb.hash, ob.hash), CF_NAME, qualifier(SPO_PREFIX, sb.ser, pb.ser, ob.ser, cb != null ? cb.ser : null), timestamp, type, EMPTY);
        kv[1] = new KeyValue(concat(POS_PREFIX, false, pb.hash, ob.hash, sb.hash), CF_NAME, qualifier(POS_PREFIX, pb.ser, ob.ser, sb.ser, cb != null ? cb.ser : null), timestamp, type, EMPTY);
        kv[2] = new KeyValue(concat(OSP_PREFIX, false, ob.hash, sb.hash, pb.hash), CF_NAME, qualifier(OSP_PREFIX, ob.ser, sb.ser, pb.ser, cb != null ? cb.ser : null), timestamp, type, EMPTY);
        if (context != null) {
            kv[3] = new KeyValue(concat(CSPO_PREFIX, false, cb.hash, sb.hash, pb.hash, ob.hash), CF_NAME, qualifier(CSPO_PREFIX, cb.ser, sb.ser, pb.ser, ob.ser), timestamp, type, EMPTY);
            kv[4] = new KeyValue(concat(CPOS_PREFIX, false, cb.hash, pb.hash, ob.hash, sb.hash), CF_NAME, qualifier(CPOS_PREFIX, cb.ser, pb.ser, ob.ser, sb.ser), timestamp, type, EMPTY);
            kv[5] = new KeyValue(concat(COSP_PREFIX, false, cb.hash, ob.hash, sb.hash, pb.hash), CF_NAME, qualifier(COSP_PREFIX, cb.ser, ob.ser, sb.ser, pb.ser), timestamp, type, EMPTY);
        }
        return kv;
    }

    private static byte[] qualifier(byte prefix, byte[] v1, byte[] v2, byte[] v3, byte[] v4) {
    	ByteBuffer cq;
        switch(prefix) {
        	case SPO_PREFIX:
        		cq = ByteBuffer.allocate(2+v1.length + (v2 != null ? 2+v2.length : 0) + (v3 != null ? 4+v3.length : 0) + (v4 != null ? v4.length : 0));
        		cq.putShort((short) v1.length).put(v1);
        		if(v2 != null) {
        			cq.putShort((short) v2.length).put(v2);
        			if(v3 != null) {
        				cq.putInt(v3.length).put(v3);
        				if(v4 != null) {
        					cq.put(v4);
        				}
        			}
        		}
                break;
        	case POS_PREFIX:
        		cq = ByteBuffer.allocate(2+v1.length + (v2 != null ? 4+v2.length : 0) + (v3 != null ? 2+v3.length : 0) + (v4 != null ? v4.length : 0));
        		cq.putShort((short) v1.length).put(v1);
        		if(v2 != null) {
	        		cq.putInt(v2.length).put(v2);
	        		if(v3 != null) {
		        		cq.putShort((short) v3.length).put(v3);
		        		if(v4 != null) {
		        			cq.put(v4);
		        		}
	        		}
        		}
                break;
        	case OSP_PREFIX:
        		cq = ByteBuffer.allocate(4+v1.length + (v2 != null ? 2+v2.length : 0) + (v3 != null ? 2+v3.length : 0) + (v4 != null ? v4.length : 0));
        		cq.putInt(v1.length).put(v1);
        		if(v2 != null) {
	        		cq.putShort((short) v2.length).put(v2);
	        		if(v3 != null) {
		        		cq.putShort((short) v3.length).put(v3);
		        		if(v4 != null) {
		        			cq.put(v4);
		        		}
	        		}
        		}
                break;
        		
        	case CSPO_PREFIX:
        		cq = ByteBuffer.allocate(2+v1.length + (v2 != null ? 2+v2.length : 0) + (v3 != null ? 2+v3.length : 0) + (v4 != null ? v4.length : 0));
        		cq.putShort((short) v1.length).put(v1);
        		if(v2 != null) {
	        		cq.putShort((short) v2.length).put(v2);
	        		if(v3 != null) {
		        		cq.putShort((short) v3.length).put(v3);
		        		if(v4 != null) {
		        			cq.put(v4);
		        		}
	        		}
        		}
                break;
        	case CPOS_PREFIX:
        		cq = ByteBuffer.allocate(2+v1.length + (v2 != null ? 2+v2.length : 0) + (v3 != null ? 4+v3.length : 0) + (v4 != null ? v4.length : 0));
        		cq.putShort((short) v1.length).put(v1);
        		if(v2 != null) {
	        		cq.putShort((short) v2.length).put(v2);
	        		if(v3 != null) {
	        			cq.putInt(v3.length).put(v3);
	        			if(v4 != null) {
	        				cq.put(v4);
	        			}
	        		}
        		}
                break;
        	case COSP_PREFIX:
        		cq = ByteBuffer.allocate(2+v1.length + (v2 != null ? 4+v2.length : 0) + (v3 != null ? 2+v3.length : 0) + (v4 != null ? v4.length : 0));
        		cq.putShort((short) v1.length).put(v1);
        		if(v2 != null) {
	        		cq.putInt(v2.length).put(v2);
	        		if(v3 != null) {
		        		cq.putShort((short) v3.length).put(v3);
		        		if(v4 != null) {
		        			cq.put(v4);
		        		}
	        		}
        		}
                break;
            default:
            	throw new AssertionError("Invalid prefix: "+prefix);
        }
        return cq.array();
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
	public static Scan scan(RDFValue<Resource> subj, RDFValue<IRI> pred, RDFValue<Value> obj, RDFValue<Resource> ctx) {
		if (ctx == null) {
			if (subj == null) {
				if (pred == null) {
					if (obj == null) {
						return scan3_0(SPO_PREFIX, S_STOP_KEY, P_STOP_KEY, O_STOP_KEY);
                    } else {
						return scan3_1(OSP_PREFIX, obj, S_STOP_KEY, P_STOP_KEY);
                    }
                } else {
					if (obj == null) {
						return scan3_1(POS_PREFIX, pred, O_STOP_KEY, S_STOP_KEY);
                    } else {
						return scan3_2(POS_PREFIX, pred, obj, S_STOP_KEY);
                    }
                }
            } else {
				if (pred == null) {
					if (obj == null) {
						return scan3_1(SPO_PREFIX, subj, P_STOP_KEY, O_STOP_KEY);
                    } else {
						return scan3_2(OSP_PREFIX, obj, subj, P_STOP_KEY);
                    }
                } else {
					if (obj == null) {
						return scan3_2(SPO_PREFIX, subj, pred, O_STOP_KEY);
                    } else {
						return scan3_3(SPO_PREFIX, subj, pred, obj);
                    }
                }
            }
        } else {
			if (subj == null) {
				if (pred == null) {
					if (obj == null) {
						return scan4_1(CSPO_PREFIX, ctx, S_STOP_KEY, P_STOP_KEY, O_STOP_KEY);
                    } else {
						return scan4_2(COSP_PREFIX, ctx, obj, S_STOP_KEY, P_STOP_KEY);
                    }
                } else {
					if (obj == null) {
						return scan4_2(CPOS_PREFIX, ctx, pred, O_STOP_KEY, S_STOP_KEY);
                    } else {
						return scan4_3(CPOS_PREFIX, ctx, pred, obj, S_STOP_KEY);
                    }
                }
            } else {
				if (pred == null) {
					if (obj == null) {
						return scan4_2(CSPO_PREFIX, ctx, subj, P_STOP_KEY, O_STOP_KEY);
                    } else {
						return scan4_3(COSP_PREFIX, ctx, obj, subj, P_STOP_KEY);
                    }
                } else {
					if (obj == null) {
						return scan4_3(CSPO_PREFIX, ctx, subj, pred, O_STOP_KEY);
                    } else {
						return scan4_4(CSPO_PREFIX, ctx, subj, pred, obj);
                    }
                }
            }
        }
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
    public static List<Statement> parseStatements(RDFValue<Resource> subj, RDFValue<IRI> pred, RDFValue<Value> obj, RDFValue<Resource> ctx, Result res, ValueFactory vf) {
    	// multiple triples may have the same hash (i.e. row key)
		List<Statement> st;
		Cell[] cells = res.rawCells();
		if (cells != null && cells.length > 0) {
			if (cells.length == 1) {
				st = Collections.singletonList(parseStatement(subj, pred, obj, ctx, cells[0], vf));
			} else {
				st = new ArrayList<>(cells.length);
				for (Cell c : cells) {
					st.add(parseStatement(subj, pred, obj, ctx, c, vf));
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
    public static Statement parseStatement(RDFValue<Resource> subj, RDFValue<IRI> pred, RDFValue<Value> obj, RDFValue<Resource> ctx, Cell cell, ValueFactory vf) {
    	ByteBuffer key = ByteBuffer.wrap(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
    	byte prefix = key.get();
        ByteBuffer cq = ByteBuffer.wrap(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
        Resource s;
        IRI p;
        Value o;
        Resource c;
        switch(prefix) {
        	case SPO_PREFIX:
        		s = parseValue(subj, cq, cq.getShort(), vf);
        		p = parseValue(pred, cq, cq.getShort(), vf);
        		o = parseValue(obj, cq, cq.getInt(), vf);
        		c = parseValue(ctx, cq, cq.remaining(), vf);
                break;
        	case POS_PREFIX:
        		p = parseValue(pred, cq, cq.getShort(), vf);
        		o = parseValue(obj, cq, cq.getInt(), vf);
        		s = parseValue(subj, cq, cq.getShort(), vf);
        		c = parseValue(ctx, cq, cq.remaining(), vf);
                break;
        	case OSP_PREFIX:
        		o = parseValue(obj, cq, cq.getInt(), vf);
        		s = parseValue(subj, cq, cq.getShort(), vf);
        		p = parseValue(pred, cq, cq.getShort(), vf);
        		c = parseValue(ctx, cq, cq.remaining(), vf);
                break;
        	case CSPO_PREFIX:
        		c = parseValue(ctx, cq, cq.getShort(), vf);
        		s = parseValue(subj, cq, cq.getShort(), vf);
        		p = parseValue(pred, cq, cq.getShort(), vf);
        		o = parseValue(obj, cq, cq.remaining(), vf);
                break;
        	case CPOS_PREFIX:
        		c = parseValue(ctx, cq, cq.getShort(), vf);
        		p = parseValue(pred, cq, cq.getShort(), vf);
        		o = parseValue(obj, cq, cq.getInt(), vf);
        		s = parseValue(subj, cq, cq.remaining(), vf);
                break;
        	case COSP_PREFIX:
        		c = parseValue(ctx, cq, cq.getShort(), vf);
        		o = parseValue(obj, cq, cq.getInt(), vf);
        		s = parseValue(subj, cq, cq.getShort(), vf);
        		p = parseValue(pred, cq, cq.remaining(), vf);
                break;
            default:
            	throw new AssertionError("Invalid prefix: "+prefix);
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

    private static <V extends Value> V parseValue(RDFValue<V> pattern, ByteBuffer cq, int len, ValueFactory vf) {
    	if(pattern != null) {
			cq.position(cq.position()+len);
			return pattern.val;
		} else if(len > 0) {
    		byte[] b = new byte[len];
            cq.get(b);
            return (V) readValue(b, vf);
		} else {
			return null;
		}
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
		MessageDigest md = MDS.get().md256;
        try {
            md.update(key);
            return md.digest();
        } finally {
            md.reset();
        }
    }

    static byte[] hashSubject(byte[] key) {
		return digest(key, MDS.get().md128, S_KEY_SIZE);
    }

    static byte[] hashPredicate(byte[] key) {
    	return Hashing.murmur3_32().hashBytes(key).asBytes();
    }

    static byte[] hashObject(byte[] key) {
		return digest(key, MDS.get().md128, O_KEY_SIZE);
    }

    static byte[] hashContext(byte[] key) {
		return digest(key, MDS.get().md128, C_KEY_SIZE);
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

	public static byte[] id(Value v) {
		return hashUnique(writeBytes(v));
	}

    public static byte[] hashSubject(Resource v) {
		return hashSubject(writeBytes(v));
    }

    public static byte[] hashPredicate(IRI v) {
		return hashPredicate(writeBytes(v));
    }

    public static byte[] hashObject(Value v) {
		return hashObject(writeBytes(v));
    }

    public static byte[] hashContext(Resource v) {
		return hashContext(writeBytes(v));
    }

    public static String encode(byte b[]) {
        return ENC.encodeToString(b);
    }

	private static Scan scan3_0(byte prefix, byte[] stopKey1, byte[] stopKey2, byte[] stopKey3) {
		return scan(concat(prefix, false), concat(prefix, true, stopKey1, stopKey2, stopKey3));
	}

	private static Scan scan3_1(byte prefix, RDFValue<?> key1, byte[] stopKey2, byte[] stopKey3) {
		return scan(concat(prefix, false, key1.hash), concat(prefix, true, key1.hash, stopKey2, stopKey3))
				.setFilter(new ColumnPrefixFilter(qualifier(prefix, key1.ser, null, null, null)));
	}

	private static Scan scan3_2(byte prefix, RDFValue<?> key1, RDFValue<?> key2, byte[] stopKey3) {
		return scan(concat(prefix, false, key1.hash, key2.hash), concat(prefix, true, key1.hash, key2.hash, stopKey3))
				.setFilter(new ColumnPrefixFilter(qualifier(prefix, key1.ser, key2.ser, null, null)));
	}

	private static Scan scan3_3(byte prefix, RDFValue<?> key1, RDFValue<?> key2, RDFValue<?> key3) {
		return scan(concat(prefix, false, key1.hash, key2.hash, key3.hash), concat(prefix, true, key1.hash, key2.hash, key3.hash))
				.setFilter(new ColumnPrefixFilter(qualifier(prefix, key1.ser, key2.ser, key3.ser, null)));
	}

	private static Scan scan4_1(byte prefix, RDFValue<?> key1, byte[] stopKey2, byte[] stopKey3, byte[] stopKey4) {
		return scan(concat(prefix, false, key1.hash), concat(prefix, true, key1.hash, stopKey2, stopKey3, stopKey4)).setFilter(new ColumnPrefixFilter(qualifier(prefix, key1.ser, null, null, null)));
    }

	private static Scan scan4_2(byte prefix, RDFValue<?> key1, RDFValue<?> key2, byte[] stopKey3, byte[] stopKey4) {
		return scan(concat(prefix, false, key1.hash, key2.hash), concat(prefix, true, key1.hash, key2.hash, stopKey3, stopKey4)).setFilter(new ColumnPrefixFilter(qualifier(prefix, key1.ser, key2.ser, null, null)));
    }

	private static Scan scan4_3(byte prefix, RDFValue<?> key1, RDFValue<?> key2, RDFValue<?> key3, byte[] stopKey4) {
		return scan(concat(prefix, false, key1.hash, key2.hash, key3.hash), concat(prefix, true, key1.hash, key2.hash, key3.hash, stopKey4)).setFilter(new ColumnPrefixFilter(qualifier(prefix, key1.ser, key2.ser, key3.ser, null)));
    }

	private static Scan scan4_4(byte prefix, RDFValue<?> key1, RDFValue<?> key2, RDFValue<?> key3, RDFValue<?> key4) {
		return scan(concat(prefix, false, key1.hash, key2.hash, key3.hash, key4.hash), concat(prefix, true, key1.hash, key2.hash, key3.hash, key4.hash))
				.setFilter(new ColumnPrefixFilter(qualifier(prefix, key1.ser, key2.ser, key3.ser, key4.ser)));
    }

    public static byte[] writeBytes(Value v) {
    	if (v instanceof IRI) {
    		return ("<"+v.stringValue()+">").getBytes(StandardCharsets.UTF_8);
    	} else if (v instanceof BNode) {
    		return v.toString().getBytes(StandardCharsets.UTF_8);
    	} else if (v instanceof Literal) {
			Literal l = (Literal) v;
			ByteWriter writer = BYTE_WRITERS.get(l.getDatatype());
			if (writer != null) {
				return writer.writeBytes(l);
			} else {
				return l.toString().getBytes(StandardCharsets.UTF_8);
			}
		} else {
			throw new AssertionError(v);
		}
    }

    public static Value readValue(byte[] b, ValueFactory vf) {
		byte type = b[0];
		switch(type) {
			case IRI_TYPE:
				return vf.createIRI(new String(b, 1, b.length-2, StandardCharsets.UTF_8));
			case BNODE_TYPE:
				return vf.createBNode(new String(b, 2, b.length-2, StandardCharsets.UTF_8));
			case FULL_LITERAL_TYPE:
				String s = new String(b, StandardCharsets.UTF_8);
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
