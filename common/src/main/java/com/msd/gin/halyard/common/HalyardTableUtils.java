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

import com.msd.gin.halyard.vocab.HALYARD;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.Function;

import javax.annotation.Nullable;

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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;

/**
 * Core Halyard utility class performing RDF to HBase mappings and base HBase table and key management. The methods of this class define how
 * Halyard stores and finds data in HBase. This class also provides several constants that define the key encoding.
 *
 * @author Adam Sotona (MSD)
 */
public final class HalyardTableUtils {

    static final byte[] CF_NAME = Bytes.toBytes("e");
    public static final byte[] CONFIG_ROW_KEY = new byte[] {(byte) 0xff};
    static final byte[] CONFIG_COL = Bytes.toBytes("config");

	private static final int PREFIXES = 3;

	static final int DEFAULT_MAX_VERSIONS = 1;
	static final int READ_VERSIONS = 1;
	private static final Compression.Algorithm DEFAULT_COMPRESSION_ALGORITHM = Compression.Algorithm.GZ;
    private static final DataBlockEncoding DEFAULT_DATABLOCK_ENCODING = DataBlockEncoding.PREFIX;
	private static final long REGION_MAX_FILESIZE = 10000000000l;
    private static final String REGION_SPLIT_POLICY = "org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy";

    private HalyardTableUtils() {}

    static byte[] createStopKey(int size) {
    	byte[] stopKey = new byte[size];
    	Arrays.fill(stopKey, (byte) 0xff);
    	return stopKey;
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
		TableName htableName = TableName.valueOf(tableName);
        if (create && !tableExists(conn, htableName)) {
    		RDFFactory rdfFactory = RDFFactory.create(conn.getConfiguration());
            return createTable(conn, htableName, splitBits < 0 ? null : calculateSplits(splitBits, quads, rdfFactory), DEFAULT_MAX_VERSIONS);
        } else {
        	return conn.getTable(htableName);
        }
    }

    /**
     * Helper method which locates or creates and returns the specified HTable used for triple/ quad storage
     * @param config Hadoop Configuration of the cluster running HBase
     * @param tableName String table name
     * @param splits array of keys used to pre-split new table, may be null
	 * @return the org.apache.hadoop.hbase.client.Table
     * @throws IOException throws IOException in case of any HBase IO problems
     */
	public static Table createTableIfNotExists(Configuration config, String tableName, byte[][] splits)
			throws IOException {
		TableName htableName = TableName.valueOf(tableName);
		Connection conn = getConnection(config);
		if (!tableExists(conn, htableName)) {
			return createTable(conn, htableName, splits, DEFAULT_MAX_VERSIONS);
		} else {
			return conn.getTable(htableName);
		}
	}

	private static boolean tableExists(Connection conn, TableName htableName) throws IOException {
		try (Admin admin = conn.getAdmin()) {
			return admin.tableExists(htableName);
		}
	}

	public static Table createTable(Connection conn, TableName htableName, byte[][] splits, int maxVersions) throws IOException {
		try (Admin admin = conn.getAdmin()) {
			TableDescriptor td = TableDescriptorBuilder.newBuilder(htableName)
				.setColumnFamily(createColumnFamily(maxVersions))
				.setMaxFileSize(REGION_MAX_FILESIZE)
				.setRegionSplitPolicyClassName(REGION_SPLIT_POLICY)
				.build();
			admin.createTable(td, splits);
		}
		Table table = conn.getTable(htableName);
		Configuration config = conn.getConfiguration();
		Configuration halyardConf = new Configuration(false);
		for (Map.Entry<String, String> entry : config.getPropsWithPrefix("halyard.").entrySet()) {
			halyardConf.set("halyard." + entry.getKey(), entry.getValue());
		}
		ByteArrayOutputStream bout = new ByteArrayOutputStream(1024);
		halyardConf.writeXml(bout);
		Put configPut = new Put(CONFIG_ROW_KEY)
			.addColumn(CF_NAME, CONFIG_COL, bout.toByteArray());
		table.put(configPut);
		return table;
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
	static byte[][] calculateSplits(final int splitBits, boolean quads, RDFFactory rdfFactory) {
		return calculateSplits(splitBits, quads, null, rdfFactory);
	}
	static byte[][] calculateSplits(final int splitBits, boolean quads, Map<IRI,Float> predicateRatios, RDFFactory rdfFactory) {
        StatementIndex<SPOC.S,SPOC.P,SPOC.O,SPOC.C> spo = rdfFactory.getSPOIndex();
        StatementIndex<SPOC.P,SPOC.O,SPOC.S,SPOC.C> pos = rdfFactory.getPOSIndex();
        StatementIndex<SPOC.O,SPOC.S,SPOC.P,SPOC.C> osp = rdfFactory.getOSPIndex();
        StatementIndex<SPOC.C,SPOC.S,SPOC.P,SPOC.O> cspo = rdfFactory.getCSPOIndex();
        StatementIndex<SPOC.C,SPOC.P,SPOC.O,SPOC.S> cpos = rdfFactory.getCPOSIndex();
        StatementIndex<SPOC.C,SPOC.O,SPOC.S,SPOC.P> cosp = rdfFactory.getCOSPIndex();
        TreeSet<byte[]> splitKeys = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        //basic presplits
        splitKeys.add(new byte[]{ pos.prefix });
        splitKeys.add(new byte[]{ osp.prefix });
		if (quads) {
			splitKeys.add(new byte[] { cspo.prefix });
			splitKeys.add(new byte[] { cpos.prefix });
			splitKeys.add(new byte[] { cosp.prefix });
		}
        //common presplits
		addSplits(splitKeys, spo.prefix, splitBits, null, rdfFactory);
		addSplits(splitKeys, pos.prefix, splitBits, transformKeys(predicateRatios, iri -> rdfFactory.createPredicate(iri)), rdfFactory);
        addSplits(splitKeys, osp.prefix, splitBits, null, rdfFactory);
        if (quads) {
			addSplits(splitKeys, cspo.prefix, splitBits/2, null, rdfFactory);
			addSplits(splitKeys, cpos.prefix, splitBits/2, null, rdfFactory);
			addSplits(splitKeys, cosp.prefix, splitBits/2, null, rdfFactory);
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
	 * Generate the split keys and add it to the collection.
	 * 
	 * @param splitKeys the {@code TreeSet} to add the collection to.
	 * @param prefix the prefix to calculate the key for
	 * @param splitBits between 0 and 15, larger values generate smaller split steps
	 * @param rdfFactory RDFFactory
	 */
	private static void addSplits(TreeSet<byte[]> splitKeys, byte prefix, final int splitBits, Map<? extends RDFIdentifier<?>,Float> keyFractions, RDFFactory rdfFactory) {
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
			for (Map.Entry<? extends RDFIdentifier<?>, Float> entry : keyFractions.entrySet()) {
				byte[] keyHash = entry.getKey().getKeyHash(StatementIndex.toIndex(prefix, rdfFactory));
				byte[] keyPrefix = new byte[1+keyHash.length];
				keyPrefix[0] = prefix;
				System.arraycopy(keyHash, 0, keyPrefix, 1, keyHash.length);
				if (nonZeroSplitCount > 1) {
					// add divider
					splitKeys.add(keyPrefix);
				}
				float fraction = entry.getValue();
				int keySplitBits = (int)(scale*Math.round(fraction*splitBits));
				splitKey(splitKeys, keyPrefix, keySplitBits);
				fractionSum += fraction;
			}
		}

		otherSplitBits *= scale;
		splitKey(splitKeys, new byte[] {prefix}, otherSplitBits);
    }

	private static void splitKey(TreeSet<byte[]> splitKeys, byte[] prefix, final int splitBits) {
		final int splitStep = 1 << (16 - splitBits);
		for (int i = splitStep; i <= 0xFFFF; i += splitStep) {
            byte bb[] = Arrays.copyOf(prefix, prefix.length + 2);
            // write unsigned short
			bb[prefix.length] = (byte) ((i >> 8) & 0xFF);
            bb[prefix.length + 1] = (byte) (i & 0xFF);
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
     * @param rdfFactory RDFFactory
     * @return List of KeyValues
     */
	public static List<? extends KeyValue> toKeyValues(Resource subj, IRI pred, Value obj, Resource context, boolean delete, long timestamp, RDFFactory rdfFactory) {
		List<KeyValue> kvs =  new ArrayList<KeyValue>(context == null ? PREFIXES : 2 * PREFIXES);
        KeyValue.Type type = delete ? KeyValue.Type.DeleteColumn : KeyValue.Type.Put;
		timestamp = toHalyardTimestamp(timestamp, !delete);
		appendKeyValues(subj, pred, obj, context, type, timestamp, kvs, rdfFactory);
		return kvs;
	}

    private static void appendKeyValues(Resource subj, IRI pred, Value obj, Resource context, KeyValue.Type type, long timestamp, List<KeyValue> kvs, RDFFactory rdfFactory) {
    	if(subj == null || pred == null || obj == null) {
    		throw new NullPointerException();
    	}
    	if(context != null && context.isTriple()) {
    		throw new UnsupportedOperationException("Context cannot be a triple value");
    	}

    	RDFSubject sb = rdfFactory.createSubject(subj); // subject bytes
		RDFPredicate pb = rdfFactory.createPredicate(pred); // predicate bytes
		RDFObject ob = rdfFactory.createObject(obj); // object bytes
		RDFContext cb = rdfFactory.createContext(context); // context (graph) bytes

        StatementIndex<SPOC.S,SPOC.P,SPOC.O,SPOC.C> spo = rdfFactory.getSPOIndex();
        StatementIndex<SPOC.P,SPOC.O,SPOC.S,SPOC.C> pos = rdfFactory.getPOSIndex();
        StatementIndex<SPOC.O,SPOC.S,SPOC.P,SPOC.C> osp = rdfFactory.getOSPIndex();
        StatementIndex<SPOC.C,SPOC.S,SPOC.P,SPOC.O> cspo = rdfFactory.getCSPOIndex();
        StatementIndex<SPOC.C,SPOC.P,SPOC.O,SPOC.S> cpos = rdfFactory.getCPOSIndex();
        StatementIndex<SPOC.C,SPOC.O,SPOC.S,SPOC.P> cosp = rdfFactory.getCOSPIndex();

        // generate HBase key value pairs from: row, family, qualifier, value. Permutations of SPO (and if needed CSPO) are all stored.
		kvs.add(new KeyValue(spo.row(sb, pb, ob, cb), CF_NAME, spo.qualifier(sb, pb, ob, cb), timestamp, type, spo.value(sb, pb, ob, cb)));
		kvs.add(new KeyValue(pos.row(pb, ob, sb, cb), CF_NAME, pos.qualifier(pb, ob, sb, cb), timestamp, type, pos.value(pb, ob, sb, cb)));
		kvs.add(new KeyValue(osp.row(ob, sb, pb, cb), CF_NAME, osp.qualifier(ob, sb, pb, cb), timestamp, type, osp.value(ob, sb, pb, cb)));
        if (context != null) {
        	kvs.add(new KeyValue(cspo.row(cb, sb, pb, ob), CF_NAME, cspo.qualifier(cb, sb, pb, ob), timestamp, type, cspo.value(cb, sb, pb, ob)));
        	kvs.add(new KeyValue(cpos.row(cb, pb, ob, sb), CF_NAME, cpos.qualifier(cb, pb, ob, sb), timestamp, type, cpos.value(cb, pb, ob, sb)));
        	kvs.add(new KeyValue(cosp.row(cb, ob, sb, pb), CF_NAME, cosp.qualifier(cb, ob, sb, pb), timestamp, type, cosp.value(cb, ob, sb, pb)));
        }

		if (subj.isTriple()) {
			Triple t = (Triple) subj;
			appendKeyValues(t.getSubject(), t.getPredicate(), t.getObject(), HALYARD.TRIPLE_GRAPH_CONTEXT, type, timestamp, kvs, rdfFactory);
		}

		if (obj.isTriple()) {
			Triple t = (Triple) obj;
			appendKeyValues(t.getSubject(), t.getPredicate(), t.getObject(), HALYARD.TRIPLE_GRAPH_CONTEXT, type, timestamp, kvs, rdfFactory);
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
	public static Scan scan(RDFSubject subj, RDFPredicate pred, RDFObject obj, RDFContext ctx, RDFFactory rdfFactory) {
		if (ctx == null) {
			if (subj == null) {
				if (pred == null) {
					if (obj == null) {
						return rdfFactory.getSPOIndex().scan();
                    } else {
						return rdfFactory.getOSPIndex().scan(obj);
                    }
                } else {
					if (obj == null) {
						return rdfFactory.getPOSIndex().scan(pred);
                    } else {
						return rdfFactory.getPOSIndex().scan(pred, obj);
                    }
                }
            } else {
				if (pred == null) {
					if (obj == null) {
						return rdfFactory.getSPOIndex().scan(subj);
                    } else {
						return rdfFactory.getOSPIndex().scan(obj, subj);
                    }
                } else {
					if (obj == null) {
						return rdfFactory.getSPOIndex().scan(subj, pred);
                    } else {
						return rdfFactory.getSPOIndex().scan(subj, pred, obj);
                    }
                }
            }
        } else {
			if (subj == null) {
				if (pred == null) {
					if (obj == null) {
						return rdfFactory.getCSPOIndex().scan(ctx);
                    } else {
						return rdfFactory.getCOSPIndex().scan(ctx, obj);
                    }
                } else {
					if (obj == null) {
						return rdfFactory.getCPOSIndex().scan(ctx, pred);
                    } else {
						return rdfFactory.getCPOSIndex().scan(ctx, pred, obj);
                    }
                }
            } else {
				if (pred == null) {
					if (obj == null) {
						return rdfFactory.getCSPOIndex().scan(ctx, subj);
                    } else {
						return rdfFactory.getCOSPIndex().scan(ctx, obj, subj);
                    }
                } else {
					if (obj == null) {
						return rdfFactory.getCSPOIndex().scan(ctx, subj, pred);
                    } else {
						return rdfFactory.getCSPOIndex().scan(ctx, subj, pred, obj);
                    }
                }
            }
        }
    }

	public static Scan scanWithConstraints(RDFSubject subj, RDFPredicate pred, LiteralConstraints constraints, RDFContext ctx, RDFFactory rdfFactory) {
		if (ctx == null) {
			if (subj == null) {
				if (pred == null) {
					return rdfFactory.getOSPIndex().scanWithConstraints(constraints);
                } else {
					return rdfFactory.getPOSIndex().scanWithConstraints(pred, constraints);
                }
            } else {
				if (pred == null) {
					return rdfFactory.getSPOIndex().scanWithConstraints(subj, null, constraints);
                } else {
					return rdfFactory.getSPOIndex().scanWithConstraints(subj, pred, constraints);
                }
            }
        } else {
			if (subj == null) {
				if (pred == null) {
					return rdfFactory.getCOSPIndex().scanWithConstraints(ctx, constraints);
                } else {
					return rdfFactory.getCPOSIndex().scanWithConstraints(ctx, pred, constraints);
                }
            } else {
				if (pred == null) {
					return rdfFactory.getCSPOIndex().scanWithConstraints(ctx, subj, null, constraints);
                } else {
					return rdfFactory.getCSPOIndex().scanWithConstraints(ctx, subj, pred, constraints);
                }
            }
        }
    }

	public static Resource getSubject(Table table, Identifier id, ValueFactory vf, RDFFactory rdfFactory) throws IOException {
		ValueIO.Reader valueReader = rdfFactory.createTableReader(vf, table);
		Scan scan = scan(rdfFactory.getSPOIndex(), id);
		try (ResultScanner scanner = table.getScanner(scan)) {
			for (Result result : scanner) {
				Cell[] cells = result.rawCells();
				if(cells != null && cells.length > 0) {
					Statement stmt = parseStatement(null, null, null, null, cells[0], valueReader, rdfFactory);
					return stmt.getSubject();
				}
			}
		}
		return null;
	}

	public static IRI getPredicate(Table table, Identifier id, ValueFactory vf, RDFFactory rdfFactory) throws IOException {
		ValueIO.Reader valueReader = rdfFactory.createTableReader(vf, table);
		Scan scan = scan(rdfFactory.getPOSIndex(), id);
		try (ResultScanner scanner = table.getScanner(scan)) {
			for (Result result : scanner) {
				Cell[] cells = result.rawCells();
				if(cells != null && cells.length > 0) {
					Statement stmt = parseStatement(null, null, null, null, cells[0], valueReader, rdfFactory);
					return stmt.getPredicate();
				}
			}
		}
		return null;
	}

	public static Value getObject(Table table, Identifier id, ValueFactory vf, RDFFactory rdfFactory) throws IOException {
		ValueIO.Reader valueReader = rdfFactory.createTableReader(vf, table);
		Scan scan = scan(rdfFactory.getOSPIndex(), id);
		try (ResultScanner scanner = table.getScanner(scan)) {
			for (Result result : scanner) {
				Cell[] cells = result.rawCells();
				if(cells != null && cells.length > 0) {
					Statement stmt = parseStatement(null, null, null, null, cells[0], valueReader, rdfFactory);
					return stmt.getObject();
				}
			}
		}
		return null;
	}

	private static Scan scan(StatementIndex<?,?,?,?> index, Identifier id) {
		Scan scanAll = index.scan(id);
		return scan(scanAll.getStartRow(), scanAll.getStopRow())
			.setFilter(new FilterList(scanAll.getFilter(), new FirstKeyOnlyFilter())).setOneRowLimit();
	}

	/**
	 * Parser method returning all Statements from a single HBase Scan Result
	 * 
     * @param subj subject if known
     * @param pred predicate if known
     * @param obj object if known
     * @param ctx context if known
	 * @param res HBase Scan Result
	 * @param valueReader ValueIO.Reader
	 * @return List of Statements
	 */
    public static List<Statement> parseStatements(@Nullable RDFSubject subj, @Nullable RDFPredicate pred, @Nullable RDFObject obj, @Nullable RDFContext ctx, Result res, ValueIO.Reader valueReader, RDFFactory rdfFactory) {
    	// multiple triples may have the same hash (i.e. row key)
		List<Statement> st;
		Cell[] cells = res.rawCells();
		if (cells != null && cells.length > 0) {
			if (cells.length == 1) {
				st = Collections.singletonList(parseStatement(subj, pred, obj, ctx, cells[0], valueReader, rdfFactory));
			} else {
				st = new ArrayList<>(cells.length);
				for (Cell c : cells) {
					st.add(parseStatement(subj, pred, obj, ctx, c, valueReader, rdfFactory));
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
	 * @param valueReader ValueIO.Reader
	 * @return Statements
	 */
    public static Statement parseStatement(@Nullable RDFSubject subj, @Nullable RDFPredicate pred, @Nullable RDFObject obj, @Nullable RDFContext ctx, Cell cell, ValueIO.Reader valueReader, RDFFactory rdfFactory) {
    	ByteBuffer key = ByteBuffer.wrap(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
        ByteBuffer cn = ByteBuffer.wrap(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
        ByteBuffer cv = ByteBuffer.wrap(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    	StatementIndex<?,?,?,?> index = StatementIndex.toIndex(key.get(), rdfFactory);
        Statement stmt = index.parseStatement(subj, pred, obj, ctx, key, cn, cv, valueReader);
		if (stmt instanceof Timestamped) {
			((Timestamped) stmt).setTimestamp(fromHalyardTimestamp(cell.getTimestamp()));
        }
		return stmt;
    }

	/**
     * Helper method constructing a custom HBase Scan from given arguments
     * @param startRow start row key byte array (inclusive)
     * @param stopRow stop row key byte array (exclusive)
     * @return HBase Scan instance
     */
	static Scan scan(byte[] startRow, byte[] stopRow) {
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

	private static ColumnFamilyDescriptor createColumnFamily(int maxVersions) {
		return ColumnFamilyDescriptorBuilder.newBuilder(CF_NAME)
                .setMaxVersions(maxVersions)
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

	static final class ByteBufferInputStream extends InputStream {
		private final ByteBuffer buf;

		ByteBufferInputStream(ByteBuffer b) {
			this.buf = b;
		}

		@Override
		public int read() {
			return buf.hasRemaining() ? (buf.get() & 0xff) : -1;
		}

		@Override
		public int read(byte[] b, int off, int len) {
			int remaining = buf.remaining();
			if (remaining == 0) {
				return -1;
			}
			len = Math.min(len, remaining);
			buf.get(b, off, len);
			return len;
		}

		@Override
		public long skip(long n) {
			n = Math.max(n, -buf.position());
			n = Math.min(n, buf.remaining());
			buf.position((int)(buf.position() + n));
			return n;
		}

		@Override
		public int available() {
			return buf.remaining();
		}
	}
}
