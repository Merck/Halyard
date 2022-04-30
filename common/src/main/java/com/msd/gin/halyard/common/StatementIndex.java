package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;

/**
 * Triples/quads are stored in multiple indices as different permutations.
 */
public final class StatementIndex<T1 extends SPOC<?>,T2 extends SPOC<?>,T3 extends SPOC<?>,T4 extends SPOC<?>> {
	public enum Name {SPO, POS, OSP, CSPO, CPOS, COSP};
	private static final byte[] EMPTY_1D = new byte[0];
	private static final byte[][] EMPTY_2D = new byte[0][];
	private static final byte WELL_KNOWN_IRI_MARKER = (byte) ('#' | 0x80);  // marker must be negative (msb set) so it is distinguishable from a length (>=0)

	public static StatementIndex<?,?,?,?> toIndex(byte prefix, RDFFactory rdfFactory) {
		switch(prefix) {
			case 0: return rdfFactory.getSPOIndex();
			case 1: return rdfFactory.getPOSIndex();
			case 2: return rdfFactory.getOSPIndex();
			case 3: return rdfFactory.getCSPOIndex();
			case 4: return rdfFactory.getCPOSIndex();
			case 5: return rdfFactory.getCOSPIndex();
			default: throw new AssertionError(String.format("Invalid prefix: %s", prefix));
		}
	}

	public static final Scan scanAll(RDFFactory rdfFactory) {
		StatementIndex<SPOC.S,SPOC.P,SPOC.O,SPOC.C> spo = rdfFactory.getSPOIndex();
		StatementIndex<SPOC.C,SPOC.O,SPOC.S,SPOC.P> cosp = rdfFactory.getCOSPIndex();
		return HalyardTableUtils.scan(spo.concat(false), cosp.concat(true, cosp.newStopKeys()));
	}

	public static final Scan scanLiterals(RDFFactory rdfFactory) {
		StatementIndex<SPOC.O,SPOC.S,SPOC.P,SPOC.C> index = rdfFactory.getOSPIndex();
		int typeSaltSize = rdfFactory.getTypeSaltSize();
		if (typeSaltSize == 1) {
			byte[] startRow = index.concat(false, rdfFactory.createTypeSalt(0, (byte)0)); // inclusive
			byte[] stopRow = index.concat(false, rdfFactory.createTypeSalt(0, Identifier.LITERAL_STOP_BITS)); // exclusive
			return HalyardTableUtils.scan(startRow, stopRow);
		} else {
			return index.scanWithConstraints(new LiteralConstraints());
		}
	}

	public static final Scan scanLiterals(Resource graph, RDFFactory rdfFactory) {
		RDFContext ctx = rdfFactory.createContext(graph);
		StatementIndex<SPOC.C,SPOC.O,SPOC.S,SPOC.P> index = rdfFactory.getCOSPIndex();
		int typeSaltSize = rdfFactory.getTypeSaltSize();
		if (typeSaltSize == 1) {
			byte[] ctxb = ctx.getKeyHash(index);
			byte[] startRow = index.concat(false, ctxb, rdfFactory.createTypeSalt(0, (byte)0)); // inclusive
			byte[] stopRow = index.concat(false, ctxb, rdfFactory.createTypeSalt(0, Identifier.LITERAL_STOP_BITS)); // exclusive
			return HalyardTableUtils.scan(startRow, stopRow);
		} else {
			return index.scanWithConstraints(ctx, new LiteralConstraints());
		}
	}

	/**
	 * @param sizeLen length of size field, 2 for short, 4 for int.
	 */
	private static int valueSize(RDFValue<?,?> v, int sizeLen) {
		if (v.isWellKnownIRI()) {
			return 1;
		} else {
			return sizeLen + v.getSerializedForm().remaining();
		}
	}

	private static void putRDFValue(ByteBuffer cv, RDFValue<?,?> v, int sizeLen) {
		if (v.isWellKnownIRI()) {
			cv.put(WELL_KNOWN_IRI_MARKER);
		} else {
			ByteBuffer ser = v.getSerializedForm();
			switch (sizeLen) {
				case Short.BYTES:
					cv.putShort((short) ser.remaining()).put(ser);
					break;
				case Integer.BYTES:
					cv.putInt(ser.remaining()).put(ser);
					break;
				default:
					throw new AssertionError("Unsupported sizeLen: "+sizeLen);
			}
		}
	}

	private static void putLastRDFValue(ByteBuffer cv, RDFValue<?,?> v) {
		if (v.isWellKnownIRI()) {
			cv.put(WELL_KNOWN_IRI_MARKER);
		} else {
			ByteBuffer ser = v.getSerializedForm();
			cv.put(ser);
		}
	}

	private static void skipId(ByteBuffer key, ByteBuffer cn, int keySize, int idSize) {
		key.position(key.position() + keySize);
		cn.position(cn.position() + idSize - keySize);
	}

	private static byte[] concat2(byte[] b1, byte[] b2) {
		byte[] b = new byte[b1.length + b2.length];
		System.arraycopy(b1, 0, b, 0, b1.length);
		System.arraycopy(b2, 0, b, b1.length, b2.length);
		return b;
	}

	private static byte[] concat3(byte[] b1, byte[] b2, byte[] b3) {
		byte[] b = new byte[b1.length + b2.length + b3.length];
		System.arraycopy(b1, 0, b, 0, b1.length);
		System.arraycopy(b2, 0, b, b1.length, b2.length);
		System.arraycopy(b3, 0, b, b1.length + b2.length, b3.length);
		return b;
	}

	private final Name name;
	final byte prefix;
	private final boolean isQuadIndex;
	private final RDFRole<T1> role1;
	private final RDFRole<T2> role2;
	private final RDFRole<T3> role3;
	private final RDFRole<T4> role4;
	private final int[] argIndices;
	private final int[] spocIndices;
	private final RDFFactory rdfFactory;

	StatementIndex(Name name, int prefix, boolean isQuadIndex, RDFRole<T1> role1, RDFRole<T2> role2, RDFRole<T3> role3, RDFRole<T4> role4, RDFFactory rdfFactory) {
		this.name = name;
		this.prefix = (byte) prefix;
		this.isQuadIndex = isQuadIndex;
		this.role1 = role1;
		this.role2 = role2;
		this.role3 = role3;
		this.role4 = role4;
		this.rdfFactory = rdfFactory;

		this.argIndices = new int[4];
		this.spocIndices = new int[4];
		RDFRole<?>[] roles = new RDFRole<?>[] {role1, role2, role3, role4};
		for (int i=0; i<roles.length; i++) {
			int spocIndex = roles[i].getName().ordinal();
			this.argIndices[i] = spocIndex;
			this.spocIndices[spocIndex] = i;
		}
	}

	public Name getName() {
		return name;
	}

	public boolean isQuadIndex() {
		return isQuadIndex;
	}

	private int get3KeyHashSize(RDFRole<?> role) {
		return isQuadIndex ? role.keyHashSize() : role.endKeyHashSize();
	}
	private <T extends SPOC<?>> byte[] get3KeyHash(StatementIndex<?,?,T,?> index, RDFIdentifier<T> k) {
		return isQuadIndex ? k.getKeyHash(index) : k.getEndKeyHash(index);
	}
	private int get3QualifierHashSize(RDFRole<?> role) {
		return isQuadIndex ? role.qualifierHashSize() : role.endQualifierHashSize();
	}
	private void write3QualifierHashTo(RDFIdentifier<?> k, ByteBuffer b) {
		if (isQuadIndex) {
			k.writeQualifierHashTo(b);
		} else {
			k.writeEndQualifierHashTo(b);
		}
	}

	private byte[][] newStopKeys() {
		return isQuadIndex ? new byte[][] {role1.stopKey(), role2.stopKey(), role3.stopKey(), role4.endStopKey()} : new byte[][] {role1.stopKey(), role2.stopKey(), role3.endStopKey()};
	}
	private byte[] keyHash(Identifier id) {
		return role1.keyHash(this, id);
	}
	private byte[] qualifierHash(Identifier id) {
		return role1.qualifierHash(id);
	}

	byte[] row(RDFIdentifier<T1> v1, RDFIdentifier<T2> v2, RDFIdentifier<T3> v3, @Nullable RDFIdentifier<T4> v4) {
		if (isQuadIndex && v4 == null) {
			throw new NullPointerException("Missing value from quad.");
		}
		ByteBuffer r = ByteBuffer.allocate(1 + role1.keyHashSize() + role2.keyHashSize() + get3KeyHashSize(role3) + (isQuadIndex ? role4.endKeyHashSize() : 0));
		r.put(prefix);
		r.put(v1.getKeyHash(this));
		r.put(v2.getKeyHash(this));
		r.put(get3KeyHash(this, v3));
		if(isQuadIndex) {
			r.put(v4.getEndKeyHash(this));
		}
		return r.array();
	}

	byte[] qualifier(RDFIdentifier<T1> v1, @Nullable RDFIdentifier<T2> v2, @Nullable RDFIdentifier<T3> v3, @Nullable RDFIdentifier<T4> v4) {
		ByteBuffer cq = ByteBuffer.allocate(role1.qualifierHashSize() + (v2 != null ? role2.qualifierHashSize() : 0) + (v3 != null ? get3QualifierHashSize(role3) : 0) + (v4 != null ? role4.endQualifierHashSize() : 0));
		v1.writeQualifierHashTo(cq);
		if(v2 != null) {
			v2.writeQualifierHashTo(cq);
    		if(v3 != null) {
				write3QualifierHashTo(v3, cq);
        		if(v4 != null) {
					v4.writeEndQualifierHashTo(cq);
        		}
    		}
		}
		return cq.array();
	}

	byte[] value(RDFValue<?,T1> v1, RDFValue<?,T2> v2, RDFValue<?,T3> v3, @Nullable RDFValue<?,T4> v4) {
		boolean hasQuad = (v4 != null);
		if (isQuadIndex && !hasQuad) {
			throw new NullPointerException("Missing value from quad.");
		}
		int sizeLen1 = role1.sizeLength();
		int sizeLen2 = role2.sizeLength();
		int sizeLen3 = role3.sizeLength();
		ByteBuffer cv = ByteBuffer.allocate(valueSize(v1, sizeLen1) + valueSize(v2, sizeLen2) + valueSize(v3, sizeLen3) + (hasQuad ? valueSize(v4, 0) : 0));
		putRDFValue(cv, v1, sizeLen1);
		putRDFValue(cv, v2, sizeLen2);
		putRDFValue(cv, v3, sizeLen3);
		if (hasQuad) {
			putLastRDFValue(cv, v4);
		}
		return cv.array();
	}

	Statement parseStatement(@Nullable RDFSubject subj, @Nullable RDFPredicate pred, @Nullable RDFObject obj, @Nullable RDFContext ctx, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, ValueIO.Reader reader) {
		RDFValue<?,?>[] args = new RDFValue<?,?>[] {subj, pred, obj, ctx};
		Value v1 = parseRDFValue(role1, args[argIndices[0]], key, cn, cv, role1.keyHashSize(), reader);
		Value v2 = parseRDFValue(role2, args[argIndices[1]], key, cn, cv, role2.keyHashSize(), reader);
		Value v3 = parseRDFValue(role3, args[argIndices[2]], key, cn, cv, get3KeyHashSize(role3), reader);
		Value v4 = parseLastRDFValue(role4, args[argIndices[3]], key, cn, cv, role4.endKeyHashSize(), reader);
		return createStatement(new Value[] {v1, v2, v3, v4}, reader.getValueFactory());
	}

    private Statement createStatement(Value[] vArray, ValueFactory vf) {
    	Resource s = (Resource) vArray[spocIndices[0]];
    	IRI p = (IRI) vArray[spocIndices[1]];
    	Value o = vArray[spocIndices[2]];
    	Resource c = (Resource) vArray[spocIndices[3]];
    	if (c == null) {
			return vf.createStatement(s, p, o);
		} else {
			return vf.createStatement(s, p, o, c);
		}
    }

    private Value parseRDFValue(RDFRole<?> role, @Nullable RDFValue<?,?> pattern, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, int keySize, ValueIO.Reader reader) {
    	byte marker = cv.get(cv.position()); // peek
    	int len;
    	if (marker == WELL_KNOWN_IRI_MARKER) {
    		len = cv.get();
    	} else {
			switch (role.sizeLength()) {
				case Short.BYTES:
		    		len = cv.getShort();
					break;
				case Integer.BYTES:
		    		len = cv.getInt();
					break;
				default:
					throw new AssertionError(String.format("Unsupported size length: %d", role.sizeLength()));
			}
    	}
   		return parseValue(role, pattern, key, cn, cv, keySize, len, reader);
    }

    private Value parseLastRDFValue(RDFRole<?> role, @Nullable RDFValue<?,?> pattern, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, int keySize, ValueIO.Reader reader) {
    	byte marker = cv.hasRemaining() ? cv.get(cv.position()) : 0; // peek
    	int len;
    	if (marker == WELL_KNOWN_IRI_MARKER) {
    		len = cv.get();
    	} else {
    		len = cv.remaining();
    	}
   		return parseValue(role, pattern, key, cn, cv, keySize, len, reader);
    }

	private Value parseValue(RDFRole<?> role, @Nullable RDFValue<?,?> pattern, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, int keySize, int len, ValueIO.Reader reader) {
    	if(pattern != null) {
    		// if we have been given the value then don't bother to read it and skip to the next
    		skipId(key, cn, keySize, rdfFactory.getIdSize());
    		if (len > 0) {
    			cv.position(cv.position() + len);
    		}
			return pattern.val;
    	} else if(len == WELL_KNOWN_IRI_MARKER) {
			Identifier id = parseId(role, key, cn, keySize);
			IRI iri = rdfFactory.getWellKnownIRI(id);
			if (iri == null) {
				throw new IllegalStateException(String.format("Unknown IRI hash: %s", id));
			}
			return iri;
		} else if(len > 0) {
			Identifier id = parseId(role, key, cn, keySize);
			int limit = cv.limit();
			cv.limit(cv.position() + len);
			Value value = reader.readValue(cv);
			cv.limit(limit);
			if (value instanceof Identifiable) {
				((Identifiable)value).setId(id);
			}
			return value;
		} else if(len == 0) {
			return null;
		} else {
			throw new AssertionError(String.format("Invalid RDF value length: %d", len));
		}
    }

	private Identifier parseId(RDFRole<?> role, ByteBuffer key, ByteBuffer cn, int keySize) {
		byte[] idBytes = new byte[rdfFactory.getIdSize()];
		role.unrotate(key.array(), key.arrayOffset() + key.position(), keySize, this, idBytes);
		key.position(key.position()+keySize);
		cn.get(idBytes, keySize, idBytes.length - keySize);
		return rdfFactory.id(idBytes);
	}

	private Scan scan(byte[][] startKeys, byte[][] stopKeys) {
		return HalyardTableUtils.scan(concat(false, startKeys), concat(true, stopKeys));
	}

	Scan scan(Identifier id) {
		byte[] kb = keyHash(id);
		byte[][] stopKeys = newStopKeys();
		stopKeys[0] = kb;
		return scan(new byte[][] {kb}, stopKeys).setFilter(new ColumnPrefixFilter(qualifierHash(id)));
	}
	public Scan scan() {
		return scanWithConstraints(null);
	}
	public Scan scanWithConstraints(LiteralConstraints constraints) {
		Scan scan = scan(EMPTY_2D, newStopKeys());
		if (constraints != null) {
			List<Filter> filters = new ArrayList<>();
			appendLiteralFilters(EMPTY_1D, EMPTY_1D, constraints, filters);
			scan.setFilter(new FilterList(filters));
		}
		return scan;
	}
	public Scan scan(RDFIdentifier<T1> k) {
		return scanWithConstraints(k, null);
	}
	public Scan scanWithConstraints(RDFIdentifier<T1> k, LiteralConstraints constraints) {
		byte[] kb = k.getKeyHash(this);
		byte[][] stopKeys = newStopKeys();
		stopKeys[0] = kb;
		Scan scan = scan(new byte[][] {kb}, stopKeys);
		Filter qf = new ColumnPrefixFilter(qualifier(k, null, null, null));
		if (constraints != null) {
			List<Filter> filters = new ArrayList<>();
			filters.add(qf);
			appendLiteralFilters(kb, kb, constraints, filters);
			scan.setFilter(new FilterList(filters));
		} else {
			scan.setFilter(qf);
		}
		return scan;
	}
	public Scan scan(RDFIdentifier<T1> k1, RDFIdentifier<T2> k2) {
		return scanWithConstraints(k1, k2, null);
	}
	public Scan scanWithConstraints(RDFIdentifier<T1> k1, RDFIdentifier<T2> k2, LiteralConstraints constraints) {
		byte[] k1b = k1.getKeyHash(this);
		byte[] k2b, stop2;
		if (k2 != null) {
			k2b = k2.getKeyHash(this);
			stop2 = k2b;
		} else {
			int k2size = role2.keyHashSize();
			k2b = new byte[k2size];
			stop2 = new byte[k2size];
			Arrays.fill(stop2, (byte) 0xFF);
		}
		byte[][] stopKeys = newStopKeys();
		stopKeys[0] = k1b;
		stopKeys[1] = stop2;
		Scan scan = scan(new byte[][] {k1b, k2b}, stopKeys);
		Filter qf = new ColumnPrefixFilter(qualifier(k1, k2, null, null));
		if (constraints != null) {
			List<Filter> filters = new ArrayList<>();
			filters.add(qf);
			appendLiteralFilters(concat2(k1b, k2b), concat2(k1b, stop2), constraints, filters);
			scan.setFilter(new FilterList(filters));
		} else {
			scan.setFilter(qf);
		}
		return scan;
	}
	public Scan scan(RDFIdentifier<T1> k1, RDFIdentifier<T2> k2, RDFIdentifier<T3> k3) {
		byte[] k1b = k1.getKeyHash(this);
		byte[] k2b = k2.getKeyHash(this);
		byte[] k3b = get3KeyHash(this, k3);
		byte[][] stopKeys = newStopKeys();
		stopKeys[0] = k1b;
		stopKeys[1] = k2b;
		stopKeys[2] = k3b;
		return scan(new byte[][] {k1b, k2b, k3b}, stopKeys).setFilter(new ColumnPrefixFilter(qualifier(k1, k2, k3, null)));
	}
	public Scan scanWithConstraints(RDFIdentifier<T1> k1, RDFIdentifier<T2> k2, RDFIdentifier<T3> k3, LiteralConstraints constraints) {
		byte[] k1b = k1.getKeyHash(this);
		byte[] k2b = k2.getKeyHash(this);
		byte[] k3b, stop3;
		if (k3 != null) {
			k3b = get3KeyHash(this, k3);
			stop3 = k3b;
		} else {
			int k3size = get3KeyHashSize(role3);
			k3b = new byte[k3size];
			stop3 = new byte[k3size];
			Arrays.fill(stop3, (byte) 0xFF);
		}
		byte[][] stopKeys = newStopKeys();
		stopKeys[0] = k1b;
		stopKeys[1] = k2b;
		stopKeys[2] = stop3;
		Scan scan = scan(new byte[][] {k1b, k2b, k3b}, stopKeys);
		Filter qf = new ColumnPrefixFilter(qualifier(k1, k2, k3, null));
		if (constraints != null) {
			List<Filter> filters = new ArrayList<>();
			filters.add(qf);
			appendLiteralFilters(concat3(k1b, k2b, k3b), concat3(k1b, k2b, stop3), constraints, filters);
			scan.setFilter(new FilterList(filters));
		} else {
			scan.setFilter(qf);
		}
		return scan;
	}
	public Scan scan(RDFIdentifier<T1> k1, RDFIdentifier<T2> k2, RDFIdentifier<T3> k3, RDFIdentifier<T4> k4) {
		byte[] k1b = k1.getKeyHash(this);
		byte[] k2b = k2.getKeyHash(this);
		byte[] k3b = get3KeyHash(this, k3);
		byte[] k4b = k4.getEndKeyHash(this);
		return scan(new byte[][] {k1b, k2b, k3b, k4b}, new byte[][] {k1b, k2b, k3b, k4b}).setFilter(new ColumnPrefixFilter(qualifier(k1, k2, k3, k4)));
	}

	private void appendLiteralFilters(byte[] startPrefix, byte[] stopPrefix, LiteralConstraints constraints, List<Filter> filters) {
		byte startTypeBits;
		byte endTypeBits;
		if (constraints.allLiterals()) {
			startTypeBits = (byte) 0;
			endTypeBits = Identifier.LITERAL_STOP_BITS;
		} else if (constraints.stringsOnly()) {
			startTypeBits = Identifier.STRING_DATATYPE_BITS;
			endTypeBits = Identifier.LITERAL_STOP_BITS;
		} else {
			// non-string literals
			startTypeBits = Identifier.NONSTRING_DATATYPE_BITS;
			endTypeBits = Identifier.STRING_DATATYPE_BITS;
		}
		int typeSaltSize = rdfFactory.getTypeSaltSize();
		List<RowRange> ranges = new ArrayList<>(typeSaltSize);
		for (int i=0; i<typeSaltSize; i++) {
			byte[] startRow = concat(false, startPrefix, rdfFactory.createTypeSalt(i, startTypeBits)); // inclusive
			byte[] stopRow = concat(false, stopPrefix, rdfFactory.createTypeSalt(i, endTypeBits)); // exclusive
			ranges.add(new RowRange(startRow, true, stopRow, false));
		}
		filters.add(new MultiRowRangeFilter(ranges));
	}

	/**
     * Helper method concatenating keys
     * @param trailingZero boolean switch adding trailing zero to the resulting key
     * @param fragments variable number of the key fragments as byte arrays
     * @return concatenated key as byte array
     */
    private byte[] concat(boolean trailingZero, byte[]... fragments) {
        int totalLen = 1; // for prefix
        for (byte[] fr : fragments) {
            totalLen += fr.length;
        }
        byte[] res = new byte[trailingZero ? totalLen + 1 : totalLen];
        res[0] = prefix;
        int offset = 1; // for prefix
        for (byte[] fr : fragments) {
            System.arraycopy(fr, 0, res, offset, fr.length);
            offset += fr.length;
        }
        return res;
    }

	@Override
	public String toString() {
		return name.toString();
	}
}
