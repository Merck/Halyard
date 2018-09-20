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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.TreeSet;
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
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;

/**
 * Core Halyard utility class performing RDF to HBase mappings and base HBase table and key management. The methods of this class define how
 * Halyard stores and finds data in HBase. This class also provides several constants that define the key encoding.
 *
 * @author Adam Sotona (MSD)
 */
public final class HalyardTableUtils {

    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final byte[] EMPTY = new byte[0];
    private static final byte[] CF_NAME = "e".getBytes(UTF8);
    private static final String MD_ALGORITHM = "SHA1";
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

    private static final int PREFIXES = 3;
    private static final byte[] START_KEY = new byte[20];
    public static final byte[] STOP_KEY = new byte[20];
    static {
        Arrays.fill(START_KEY, (byte)0);
        Arrays.fill(STOP_KEY, (byte)0xff); /* 0xff is 255 in decimal */
    }
    private static final Compression.Algorithm DEFAULT_COMPRESSION_ALGORITHM = Compression.Algorithm.GZ;
    private static final DataBlockEncoding DEFAULT_DATABLOCK_ENCODING = DataBlockEncoding.PREFIX;
    private static final String REGION_MAX_FILESIZE = "10000000000";
    private static final String REGION_SPLIT_POLICY = "org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy";

    private static final ThreadLocal<MessageDigest> MD = new ThreadLocal<MessageDigest>(){
        @Override
        protected MessageDigest initialValue() {
            return getMessageDigest(MD_ALGORITHM);
        }
    };

    static MessageDigest getMessageDigest(String algorithm) {
        try {
            return MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Helper method which locates or creates and returns the specified HTable used for triple/ quad storage. The table may be pre-split into regions (rather than HBase's default
     * of starting with 1). For a discussion of pre-splits take a look at <a href="https://hortonworks.com/blog/apache-hbase-region-splitting-and-merging/">this article</a>
     * @param config Hadoop Configuration of the cluster running HBase
     * @param tableName String table name
     * @param create boolean option to create the table if does not exist
     * @param splitBits int number of bits used for calculation of HTable region pre-splits (applies for new tables only). Must be between 0 and 16. Higher values generate more
     * splits.
     * @throws IOException throws IOException in case of any HBase IO problems
     * @return the org.apache.hadoop.hbase.client.HTable
     */
    public static HTable getTable(Configuration config, String tableName, boolean create, int splitBits) throws IOException {
        return getTable(config, tableName, create, splitBits < 0 ? null : calculateSplits(splitBits));
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
    public static HTable getTable(Configuration config, String tableName, boolean create, byte[][] splits) throws IOException {
        Configuration cfg = HBaseConfiguration.create(config);
        cfg.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 3600000l);
        if (create) {
            try (Connection con = ConnectionFactory.createConnection(config)) {
                try (Admin admin = con.getAdmin()) {
                	    //check if the table exists and if it doesn't, make it
                    if (!admin.tableExists(TableName.valueOf(tableName))) {
                        HTableDescriptor td = new HTableDescriptor(TableName.valueOf(tableName));
                        td.addFamily(createColumnFamily());
                        admin.createTable(td, splits);
                    }
                }
            }
        }

        //this is deprecated, the recommendation now is to use connection.getTable()
        HTable table = new HTable(cfg, tableName);
        table.setAutoFlushTo(false);
        return table;
    }

    /**
     * Truncates HTable while preserving the region pre-splits
     * @param table HTable to truncate
     * @return new instance of the truncated HTable
     * @throws IOException throws IOException in case of any HBase IO problems
     */
    public static HTable truncateTable(HTable table) throws IOException {
        Configuration conf = table.getConfiguration();
        byte[][] presplits = table.getRegionLocator().getStartKeys();
        if (presplits.length > 0 && presplits[0].length == 0) {
            presplits = Arrays.copyOfRange(presplits, 1, presplits.length);
        }
        HTableDescriptor desc = table.getTableDescriptor();
        table.close();
        try (Connection con = ConnectionFactory.createConnection(conf)) {
            try (Admin admin = con.getAdmin()) {
                admin.disableTable(desc.getTableName());
                admin.deleteTable(desc.getTableName());
                admin.createTable(desc, presplits);
            }
        }
        return HalyardTableUtils.getTable(conf, desc.getTableName().getNameAsString(), false, 0);
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
        byte[] sb = NTriplesUtil.toNTriplesString(subj).getBytes(UTF8); //subject bytes
        byte[] pb = NTriplesUtil.toNTriplesString(pred).getBytes(UTF8); //predicate bytes
        byte[] ob = NTriplesUtil.toNTriplesString(obj).getBytes(UTF8);  //object bytes
        byte[] cb = context == null ? new byte[0] : NTriplesUtil.toNTriplesString(context).getBytes(UTF8); // context (graph) bytes
        byte[] sKey = hashKey(sb);  //subject key
        byte[] pKey = hashKey(pb);  //predicate key
        byte[] oKey = hashKey(ob);  //object key

        //bytes to be used for the HBase column qualifier
        byte[] cq = ByteBuffer.allocate(sb.length + pb.length + ob.length + cb.length + 12).putInt(sb.length).putInt(pb.length).putInt(ob.length).put(sb).put(pb).put(ob).put(cb).array();

        KeyValue kv[] =  new KeyValue[context == null ? PREFIXES : 2 * PREFIXES];

        KeyValue.Type type = delete ? KeyValue.Type.DeleteColumn : KeyValue.Type.Put;

        //timestamp is shifted one bit left and the last bit is used to prioritize between inserts and deletes of the same time to avoid HBase ambiguity
        //inserts are considered always later after deletes on a timeline
        timestamp = timestamp << 1;
        if (!delete) {
            timestamp |= 1;
        }

        //generate HBase key value pairs from: row, family, qualifier, value. Permutations of SPO (and if needed CSPO) are all stored. Values are actually empty.
        kv[0] = new KeyValue(concat(SPO_PREFIX, false, sKey, pKey, oKey), CF_NAME, cq, timestamp, type, EMPTY);
        kv[1] = new KeyValue(concat(POS_PREFIX, false, pKey, oKey, sKey), CF_NAME, cq, timestamp, type, EMPTY);
        kv[2] = new KeyValue(concat(OSP_PREFIX, false, oKey, sKey, pKey), CF_NAME, cq, timestamp, type, EMPTY);
        if (context != null) {
            byte[] cKey = hashKey(cb);
            kv[3] = new KeyValue(concat(CSPO_PREFIX, false, cKey, sKey, pKey, oKey), CF_NAME, cq, timestamp, type, EMPTY);
            kv[4] = new KeyValue(concat(CPOS_PREFIX, false, cKey, pKey, oKey, sKey), CF_NAME, cq, timestamp, type, EMPTY);
            kv[5] = new KeyValue(concat(COSP_PREFIX, false, cKey, oKey, sKey, pKey), CF_NAME, cq, timestamp, type, EMPTY);
        }
        return kv;
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
        return scan(hashKey(subj), hashKey(pred), hashKey(obj), hashKey(ctx));
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
                        return scan(concat(SPO_PREFIX, false), concat(SPO_PREFIX, true, STOP_KEY, STOP_KEY, STOP_KEY));
                    } else {
                        return scan(OSP_PREFIX, objHash);
                    }
                } else {
                    if (objHash == null) {
                        return scan(POS_PREFIX, predHash);
                    } else {
                        return scan(POS_PREFIX, predHash, objHash);
                    }
                }
            } else {
                if (predHash == null) {
                    if (objHash == null) {
                        return scan(SPO_PREFIX, subjHash);
                    } else {
                        return scan(OSP_PREFIX, objHash, subjHash);
                    }
                } else {
                    if (objHash == null) {
                        return scan(SPO_PREFIX, subjHash, predHash);
                    } else {
                        return scan(SPO_PREFIX, subjHash, predHash, objHash);
                    }
                }
            }
        } else {
            if (subjHash == null) {
                if (predHash == null) {
                    if (objHash == null) {
                        return scan(CSPO_PREFIX, ctxHash);
                    } else {
                        return scan(COSP_PREFIX, ctxHash, objHash);
                    }
                } else {
                    if (objHash == null) {
                        return scan(CPOS_PREFIX, ctxHash, predHash);
                    } else {
                        return scan(CPOS_PREFIX, ctxHash, predHash, objHash);
                    }
                }
            } else {
                if (predHash == null) {
                    if (objHash == null) {
                        return scan(CSPO_PREFIX, ctxHash, subjHash);
                    } else {
                        return scan(COSP_PREFIX, ctxHash, objHash, subjHash);
                    }
                } else {
                    if (objHash == null) {
                        return scan(CSPO_PREFIX, ctxHash, subjHash, predHash);
                    } else {
                        return scan(CSPO_PREFIX, ctxHash, subjHash, predHash, objHash);
                    }
                }
            }
        }
    }

    /**
     * Parser method returning all Statements from a single HBase Scan Result
     * @param res HBase Scan Result
     * @return List of Statements
     */
    public static List<Statement> parseStatements(Result res) {
        ArrayList<Statement> st = new ArrayList<>();
        if (res.rawCells() != null) for (Cell c : res.rawCells()) {
            st.add(parseStatement(c));
        }
        return st;
    }

    /**
     * Parser method returning Statement from a single HBase Result Cell
     * @param c HBase Result Cell
     * @return Statements
     */
    public static Statement parseStatement(Cell c) {
        ByteBuffer bb = ByteBuffer.wrap(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength());
        byte[] sb = new byte[bb.getInt()];
        byte[] pb = new byte[bb.getInt()];
        byte[] ob = new byte[bb.getInt()];
        bb.get(sb);
        bb.get(pb);
        bb.get(ob);
        byte[] cb = new byte[bb.remaining()];
        bb.get(cb);
        ValueFactory vf = SimpleValueFactory.getInstance();
        return vf.createStatement(NTriplesUtil.parseResource(new String(sb, UTF8), vf), NTriplesUtil.parseURI(new String(pb, UTF8), vf), NTriplesUtil.parseValue(new String(ob,UTF8), vf), cb.length == 0 ? null : NTriplesUtil.parseResource(new String(cb,UTF8), vf));
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
        scan.setStartRow(startRow);
        scan.setStopRow(stopRow);
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

    public static byte[] hashKey(byte[] key) {
        MessageDigest md = MD.get();
        try {
            md.update(key);
            return md.digest();
        } finally {
            md.reset();
        }
    }

    public static byte[] hashKey(Value v) {
        return v == null ? null : hashKey(NTriplesUtil.toNTriplesString(v).getBytes(UTF8));
    }

    public static String encode(byte b[]) {
        return ENC.encodeToString(b);
    }

    private static Scan scan(byte prefix, byte[] key1) {
        return scan(concat(prefix, false, key1), concat(prefix, true, key1, STOP_KEY, STOP_KEY, STOP_KEY));
    }

    private static Scan scan(byte prefix, byte[] key1, byte[] key2) {
        return scan(concat(prefix, false, key1, key2), concat(prefix, true, key1, key2, STOP_KEY, STOP_KEY));
    }

    private static Scan scan(byte prefix, byte[] key1, byte[] key2, byte[] key3) {
        return scan(concat(prefix, false, key1, key2, key3), concat(prefix, true, key1, key2, key3, STOP_KEY));
    }

    private static Scan scan(byte prefix, byte[] key1, byte[] key2, byte[] key3, byte[] key4) {
        return scan(concat(prefix, false, key1, key2, key3, key4), concat(prefix, true, key1, key2, key3, key4));
    }
}
