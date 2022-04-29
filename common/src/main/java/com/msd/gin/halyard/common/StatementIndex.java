package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
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
			byte[] startKey = index.concat(false, rdfFactory.createTypeSalt(0, (byte)0)); // inclusive
			byte[] stopKey = index.concat(false, rdfFactory.createTypeSalt(0, Identifier.LITERAL_STOP_BITS)); // exclusive
			return HalyardTableUtils.scan(startKey, stopKey);
		} else {
			List<RowRange> ranges = new ArrayList<>(typeSaltSize);
			for (int i=0; i<typeSaltSize; i++) {
				byte[] startKey = index.concat(false, rdfFactory.createTypeSalt(i, (byte)0)); // inclusive
				byte[] stopKey = index.concat(false, rdfFactory.createTypeSalt(i, Identifier.LITERAL_STOP_BITS)); // exclusive
				ranges.add(new RowRange(startKey, true, stopKey, false));
			}
			return index.scan().setFilter(new MultiRowRangeFilter(ranges));
		}
	}

	public static final Scan scanLiterals(Resource graph, RDFFactory rdfFactory) {
		RDFContext ctx = rdfFactory.createContext(graph);
		StatementIndex<SPOC.C,SPOC.O,SPOC.S,SPOC.P> index = rdfFactory.getCOSPIndex();
		byte[] ctxb = ctx.getKeyHash(index);
		int typeSaltSize = rdfFactory.getTypeSaltSize();
		if (typeSaltSize == 1) {
			byte[] startKey = index.concat(false, ctxb, rdfFactory.createTypeSalt(0, (byte)0)); // inclusive
			byte[] stopKey = index.concat(false, ctxb, rdfFactory.createTypeSalt(0, Identifier.LITERAL_STOP_BITS)); // exclusive
			return HalyardTableUtils.scan(startKey, stopKey);
		} else {
			List<RowRange> ranges = new ArrayList<>(typeSaltSize);
			for (int i=0; i<typeSaltSize; i++) {
				byte[] startKey = index.concat(false, ctxb, rdfFactory.createTypeSalt(i, (byte)0)); // inclusive
				byte[] stopKey = index.concat(false, ctxb, rdfFactory.createTypeSalt(i, Identifier.LITERAL_STOP_BITS)); // exclusive
				ranges.add(new RowRange(startKey, true, stopKey, false));
			}
			return index.scan().setFilter(new MultiRowRangeFilter(ranges));
		}
	}

	/**
	 * @param sizeLen length of size field, 2 for short, 4 for int.
	 */
	private static int len(RDFValue<?,?> v, int sizeLen) {
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

	private final Name name;
	final byte prefix;
	private final IndexType indexType;
	private final RDFRole<?>[] roles;
	private final int[] argIndices;
	private final int[] spocIndices;
	private final RDFFactory rdfFactory;

	StatementIndex(Name name, int prefix, IndexType type, RDFRole<T1> role1, RDFRole<T2> role2, RDFRole<T3> role3, RDFRole<T4> role4, RDFFactory rdfFactory) {
		this.name = name;
		this.prefix = (byte) prefix;
		this.indexType = type;
		this.roles = new RDFRole<?>[] {role1, role2, role3, role4};
		this.rdfFactory = rdfFactory;

		this.argIndices = new int[4];
		this.spocIndices = new int[4];
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
		return indexType == IndexType.QUAD;
	}

	byte[][] newStopKeys() {
		return indexType.newStopKeys(this.roles);
	}
	byte[] keyHash(Identifier id) {
		return roles[0].keyHash(this, id);
	}
	byte[] qualifierHash(Identifier id) {
		return roles[0].qualifierHash(id);
	}

	byte[] row(RDFIdentifier<T1> v1, RDFIdentifier<T2> v2, RDFIdentifier<T3> v3, RDFIdentifier<T4> v4) {
		boolean hasQuad = isQuadIndex() || (v4 != null);
		ByteBuffer r = ByteBuffer.allocate(1 + v1.keyHashSize() + v2.keyHashSize() + indexType.get3KeyHashSize(v3.getRole()) + (hasQuad ? indexType.get4KeyHashSize(v4.getRole()) : 0));
		r.put(prefix);
		r.put(v1.getKeyHash(this));
		r.put(v2.getKeyHash(this));
		r.put(indexType.get3KeyHash(this, v3));
		if(hasQuad) {
			r.put(indexType.get4KeyHash(this, v4));
		}
		return r.array();
	}

	byte[] qualifier(RDFIdentifier<T1> v1, RDFIdentifier<T2> v2, RDFIdentifier<T3> v3, RDFIdentifier<T4> v4) {
		ByteBuffer cq = ByteBuffer.allocate(v1.qualifierHashSize() + (v2 != null ? v2.qualifierHashSize() : 0) + (v3 != null ? indexType.get3QualifierHashSize(v3.getRole()) : 0) + (v4 != null ? indexType.get4QualifierHashSize(v4.getRole()) : 0));
		v1.writeQualifierHashTo(cq);
		if(v2 != null) {
			v2.writeQualifierHashTo(cq);
    		if(v3 != null) {
				indexType.write3QualifierHashTo(v3, cq);
        		if(v4 != null) {
					indexType.write4QualifierHashTo(v4, cq);
        		}
    		}
		}
		return cq.array();
	}

	byte[] value(RDFValue<?,T1> v1, RDFValue<?,T2> v2, RDFValue<?,T3> v3, RDFValue<?,T4> v4) {
		boolean hasQuad = isQuadIndex() || (v4 != null);
		int sizeLen1 = roles[0].sizeLength();
		int sizeLen2 = roles[1].sizeLength();
		int sizeLen3 = roles[2].sizeLength();
		ByteBuffer cv = ByteBuffer.allocate(len(v1, sizeLen1) + len(v2, sizeLen2) + len(v3, sizeLen3) + (hasQuad ? len(v4, 0) : 0));
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
		Value v1 = parseRDFValue(roles[0], args[argIndices[0]], key, cn, cv, roles[0].keyHashSize(), reader);
		Value v2 = parseRDFValue(roles[1], args[argIndices[1]], key, cn, cv, roles[1].keyHashSize(), reader);
		Value v3 = parseRDFValue(roles[2], args[argIndices[2]], key, cn, cv, indexType.get3KeyHashSize(roles[2]), reader);
		Value v4 = parseLastRDFValue(roles[3], args[argIndices[3]], key, cn, cv, indexType.get4KeyHashSize(roles[3]), reader);
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

	public Scan scan() {
		return scan(new byte[0][], newStopKeys());
	}
	Scan scan(Identifier id) {
		byte[] kb = keyHash(id);
		byte[][] stopKeys = newStopKeys();
		stopKeys[0] = kb;
		return scan(new byte[][] {kb}, stopKeys).setFilter(new ColumnPrefixFilter(qualifierHash(id)));
	}
	public Scan scan(RDFIdentifier<T1> k) {
		byte[] kb = k.getKeyHash(this);
		byte[][] stopKeys = newStopKeys();
		stopKeys[0] = kb;
		return scan(new byte[][] {kb}, stopKeys).setFilter(new ColumnPrefixFilter(qualifier(k, null, null, null)));
	}
	public Scan scan(RDFIdentifier<T1> k1, RDFIdentifier<T2> k2) {
		byte[] k1b = k1.getKeyHash(this);
		byte[] k2b = k2.getKeyHash(this);
		byte[][] stopKeys = newStopKeys();
		stopKeys[0] = k1b;
		stopKeys[1] = k2b;
		return scan(new byte[][] {k1b, k2b}, stopKeys).setFilter(new ColumnPrefixFilter(qualifier(k1, k2, null, null)));
	}
	public Scan scan(RDFIdentifier<T1> k1, RDFIdentifier<T2> k2, RDFIdentifier<T3> k3) {
		byte[] k1b = k1.getKeyHash(this);
		byte[] k2b = k2.getKeyHash(this);
		byte[] k3b = indexType.get3KeyHash(this, k3);
		byte[][] stopKeys = newStopKeys();
		stopKeys[0] = k1b;
		stopKeys[1] = k2b;
		stopKeys[2] = k3b;
		return scan(new byte[][] {k1b, k2b, k3b}, stopKeys).setFilter(new ColumnPrefixFilter(qualifier(k1, k2, k3, null)));
	}
	public Scan scan(RDFIdentifier<T1> k1, RDFIdentifier<T2> k2, RDFIdentifier<T3> k3, RDFIdentifier<T4> k4) {
		byte[] k1b = k1.getKeyHash(this);
		byte[] k2b = k2.getKeyHash(this);
		byte[] k3b = indexType.get3KeyHash(this, k3);
		byte[] k4b = indexType.get4KeyHash(this, k4);
		return scan(new byte[][] {k1b, k2b, k3b, k4b}, new byte[][] {k1b, k2b, k3b, k4b}).setFilter(new ColumnPrefixFilter(qualifier(k1, k2, k3, k4)));
	}

    /**
     * Helper method concatenating keys
     * @param trailingZero boolean switch adding trailing zero to the resulting key
     * @param fragments variable number of the key fragments as byte arrays
     * @return concatenated key as byte array
     */
    byte[] concat(boolean trailingZero, byte[]... fragments) {
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
