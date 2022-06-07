package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
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
	private static final byte WELL_KNOWN_IRI_MARKER = (byte) ('#' | 0x80);  // marker must be negative (msb set) so it is distinguishable from a length (>=0)
	private static final int VAR_CARDINALITY = 10;

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
		int cardinality = VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY;
		return HalyardTableUtils.scan(
			spo.concat(false, spo.role1.startKey(), spo.role2.startKey(), spo.get3StartKey(), spo.get4StartKey()),
			cosp.concat(true, cosp.role1.stopKey(), cosp.role2.stopKey(), cosp.get3StopKey(), cosp.role4.endStopKey()),
			cardinality,
			true
		);
	}

	public static final Scan scanLiterals(RDFFactory rdfFactory) {
		StatementIndex<SPOC.O,SPOC.S,SPOC.P,SPOC.C> index = rdfFactory.getOSPIndex();
		int typeSaltSize = rdfFactory.typeSaltSize;
		if (typeSaltSize == 1) {
			int cardinality = VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY;
			return index.scan(
				rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role1.startKey()), index.role2.startKey(), index.get3StartKey(), index.get4StartKey(),
				rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role1.stopKey()), index.role2.stopKey(), index.get3StopKey(), index.role4.endStopKey(),
				cardinality,
				true
			);
		} else {
			return index.scanWithConstraint(new ValueConstraint(ValueType.LITERAL));
		}
	}

	public static final Scan scanLiterals(Resource graph, RDFFactory rdfFactory) {
		RDFContext ctx = rdfFactory.createContext(graph);
		StatementIndex<SPOC.C,SPOC.O,SPOC.S,SPOC.P> index = rdfFactory.getCOSPIndex();
		int typeSaltSize = rdfFactory.typeSaltSize;
		if (typeSaltSize == 1) {
			ByteSequence ctxb = new ByteArray(ctx.getKeyHash(index));
			int cardinality = VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY;
			return index.scan(
				ctxb, rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role2.startKey()), index.get3StartKey(), index.get4StartKey(),
				ctxb, rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role2.stopKey()), index.get3StopKey(), index.role4.endStopKey(),
				cardinality,
				true
			);
		} else {
			return index.scanWithConstraint(ctx, new ValueConstraint(ValueType.LITERAL));
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

	private static ByteSequence concat2(ByteSequence b1, ByteSequence b2) {
		return new ByteSequence() {
			@Override
			public ByteBuffer writeTo(ByteBuffer bb) {
				b1.writeTo(bb);
				b2.writeTo(bb);
				return bb;
			}

			@Override
			public int size() {
				return b1.size() + b2.size();
			}
		};
	}

	private static ByteSequence concat3(ByteSequence b1, ByteSequence b2, ByteSequence b3) {
		return new ByteSequence() {
			@Override
			public ByteBuffer writeTo(ByteBuffer bb) {
				b1.writeTo(bb);
				b2.writeTo(bb);
				b3.writeTo(bb);
				return bb;
			}

			@Override
			public int size() {
				return b1.size() + b2.size() + b3.size();
			}
		};
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

	private ByteSequence get3StartKey() {
		return isQuadIndex ? role3.startKey() : role3.endStartKey();
	}
	private ByteSequence get3StopKey() {
		return isQuadIndex ? role3.stopKey() : role3.endStopKey();
	}
	private int get3KeyHashSize() {
		return isQuadIndex ? role3.keyHashSize() : role3.endKeyHashSize();
	}
	private <T extends SPOC<?>> byte[] get3KeyHash(StatementIndex<?,?,T,?> index, RDFIdentifier<T> k) {
		return isQuadIndex ? k.getKeyHash(index) : k.getEndKeyHash(index);
	}
	private int get3QualifierHashSize() {
		return isQuadIndex ? role3.qualifierHashSize() : role3.endQualifierHashSize();
	}
	private void write3QualifierHashTo(RDFIdentifier<?> k, ByteBuffer b) {
		if (isQuadIndex) {
			k.writeQualifierHashTo(b);
		} else {
			k.writeEndQualifierHashTo(b);
		}
	}
	private ByteSequence get4StartKey() {
		return isQuadIndex ? role4.endStartKey() : ByteSequence.EMPTY;
	}

	byte[] row(RDFIdentifier<T1> v1, RDFIdentifier<T2> v2, RDFIdentifier<T3> v3, @Nullable RDFIdentifier<T4> v4) {
		boolean hasQuad = (v4 != null);
		if (isQuadIndex && !hasQuad) {
			throw new NullPointerException("Missing identifier from quad.");
		}
		byte[] r = new byte[1 + role1.keyHashSize() + role2.keyHashSize() + get3KeyHashSize() + (hasQuad ? role4.endKeyHashSize() : 0)];
		ByteBuffer bb = ByteBuffer.wrap(r);
		bb.put(prefix);
		bb.put(v1.getKeyHash(this));
		bb.put(v2.getKeyHash(this));
		bb.put(get3KeyHash(this, v3));
		if(hasQuad) {
			bb.put(v4.getEndKeyHash(this));
		}
		return r;
	}

	byte[] qualifier(RDFIdentifier<T1> v1, @Nullable RDFIdentifier<T2> v2, @Nullable RDFIdentifier<T3> v3, @Nullable RDFIdentifier<T4> v4) {
		byte[] cq = new byte[role1.qualifierHashSize() + (v2 != null ? role2.qualifierHashSize() : 0) + (v3 != null ? get3QualifierHashSize() : 0) + (v4 != null ? role4.endQualifierHashSize() : 0)];
		ByteBuffer bb = ByteBuffer.wrap(cq);
		v1.writeQualifierHashTo(bb);
		if(v2 != null) {
			v2.writeQualifierHashTo(bb);
    		if(v3 != null) {
				write3QualifierHashTo(v3, bb);
        		if(v4 != null) {
					v4.writeEndQualifierHashTo(bb);
        		}
    		}
		}
		return cq;
	}

	byte[] value(RDFValue<?,T1> v1, RDFValue<?,T2> v2, RDFValue<?,T3> v3, @Nullable RDFValue<?,T4> v4) {
		boolean hasQuad = (v4 != null);
		if (isQuadIndex && !hasQuad) {
			throw new NullPointerException("Missing value from quad.");
		}
		int sizeLen1 = role1.sizeLength();
		int sizeLen2 = role2.sizeLength();
		int sizeLen3 = role3.sizeLength();
		byte[] cv = new byte[valueSize(v1, sizeLen1) + valueSize(v2, sizeLen2) + valueSize(v3, sizeLen3) + (hasQuad ? valueSize(v4, 0) : 0)];
		ByteBuffer bb = ByteBuffer.wrap(cv);
		putRDFValue(bb, v1, sizeLen1);
		putRDFValue(bb, v2, sizeLen2);
		putRDFValue(bb, v3, sizeLen3);
		if (hasQuad) {
			putLastRDFValue(bb, v4);
		}
		return cv;
	}

	Statement parseStatement(@Nullable RDFSubject subj, @Nullable RDFPredicate pred, @Nullable RDFObject obj, @Nullable RDFContext ctx, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, ValueIO.Reader reader) {
		RDFValue<?,?>[] args = new RDFValue<?,?>[] {subj, pred, obj, ctx};
		Value v1 = parseRDFValue(role1, args[argIndices[0]], key, cn, cv, role1.keyHashSize(), reader);
		Value v2 = parseRDFValue(role2, args[argIndices[1]], key, cn, cv, role2.keyHashSize(), reader);
		Value v3 = parseRDFValue(role3, args[argIndices[2]], key, cn, cv, get3KeyHashSize(), reader);
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

    private Value parseRDFValue(RDFRole<?> role, @Nullable RDFValue<?,?> pattern, ByteBuffer key, ByteBuffer cq, ByteBuffer cv, int keySize, ValueIO.Reader reader) {
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
   		return parseValue(role, pattern, key, cq, cv, keySize, len, reader);
    }

    private Value parseLastRDFValue(RDFRole<?> role, @Nullable RDFValue<?,?> pattern, ByteBuffer key, ByteBuffer cq, ByteBuffer cv, int keySize, ValueIO.Reader reader) {
    	byte marker = cv.hasRemaining() ? cv.get(cv.position()) : 0; // peek
    	int len;
    	if (marker == WELL_KNOWN_IRI_MARKER) {
    		len = cv.get();
    	} else {
    		len = cv.remaining();
    	}
   		return parseValue(role, pattern, key, cq, cv, keySize, len, reader);
    }

	private Value parseValue(RDFRole<?> role, @Nullable RDFValue<?,?> pattern, ByteBuffer key, ByteBuffer cq, ByteBuffer cv, int keySize, int len, ValueIO.Reader reader) {
    	if(pattern != null) {
    		// if we have been given the value then don't bother to read it and skip to the next
    		skipId(key, cq, keySize, rdfFactory.getIdSize());
    		if (len > 0) {
    			cv.position(cv.position() + len);
    		}
			return pattern.val;
    	} else if(len == WELL_KNOWN_IRI_MARKER) {
			Identifier id = parseId(role, key, cq, keySize);
			IRI iri = rdfFactory.getWellKnownIRI(id);
			if (iri == null) {
				throw new IllegalStateException(String.format("Unknown IRI hash: %s", id));
			}
			return iri;
		} else if(len > 0) {
			Identifier id = parseId(role, key, cq, keySize);
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

	private Scan scan(ByteSequence k1Start, ByteSequence k2Start, ByteSequence k3Start, ByteSequence k4Start, ByteSequence k1Stop, ByteSequence k2Stop, ByteSequence k3Stop, ByteSequence k4Stop, int cardinality, boolean indiscriminate) {
		return HalyardTableUtils.scan(concat(false, k1Start, k2Start, k3Start, k4Start), concat(true, k1Stop, k2Stop, k3Stop, k4Stop), cardinality, indiscriminate);
	}

	Scan scan(Identifier id) {
		ByteSequence kb = new ByteArray(role1.keyHash(this, id));
		byte[] cq = role1.qualifierHash(id);
		int cardinality = VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY;
		return scan(
			kb, role2.startKey(), get3StartKey(), get4StartKey(),
			kb, role2.stopKey(), get3StopKey(), role4.endStopKey(),
			cardinality,
			false
		).setFilter(new ColumnPrefixFilter(cq));
	}
	public Scan scan() {
		return scanWithConstraint(null);
	}
	public Scan scanWithConstraint(ValueConstraint constraint) {
		int cardinality = VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY;
		Scan scan = scan(
			role1.startKey(), role2.startKey(), get3StartKey(), get4StartKey(),
			role1.stopKey(),  role2.stopKey(),  get3StopKey(),  role4.endStopKey(),
			cardinality,
			true
		);
		if (constraint != null) {
			List<Filter> filters = new ArrayList<>();
			appendLiteralFilters(ByteSequence.EMPTY, null,
				role1.startKey(), role1.stopKey(),
				concat3(role2.startKey(), get3StartKey(), get4StartKey()),
				concat3(role2.stopKey(),  get3StopKey(),  role4.endStopKey()),
				constraint, filters);
			scan.setFilter(new FilterList(filters));
		}
		return scan;
	}
	public Scan scan(RDFIdentifier<T1> k) {
		return scanWithConstraint(k, null);
	}
	public Scan scanWithConstraint(RDFIdentifier<T1> k, ValueConstraint constraint) {
		ByteSequence kb = new ByteArray(k.getKeyHash(this));
		int cardinality = VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY;
		Scan scan = scan(
			kb, role2.startKey(), get3StartKey(), get4StartKey(),
			kb, role2.stopKey(),  get3StopKey(),  role4.endStopKey(),
			cardinality,
			false
		);
		Filter qf = new ColumnPrefixFilter(qualifier(k, null, null, null));
		if (constraint != null) {
			List<Filter> filters = new ArrayList<>();
			filters.add(qf);
			appendLiteralFilters(kb, null,
				role2.startKey(), role2.stopKey(),
				concat2(get3StartKey(), get4StartKey()),
				concat2(get3StopKey(),  role4.endStopKey()),
				constraint, filters);
			scan.setFilter(new FilterList(filters));
		} else {
			scan.setFilter(qf);
		}
		return scan;
	}
	public Scan scan(RDFIdentifier<T1> k1, RDFIdentifier<T2> k2) {
		return scanWithConstraint(k1, k2, null);
	}
	public Scan scanWithConstraint(RDFIdentifier<T1> k1, RDFIdentifier<T2> k2, ValueConstraint constraint) {
		ByteSequence k1b = new ByteArray(k1.getKeyHash(this));
		ByteSequence k2b, stop2;
		int cardinality;
		if (k2 != null) {
			k2b = new ByteArray(k2.getKeyHash(this));
			stop2 = k2b;
			cardinality = VAR_CARDINALITY*VAR_CARDINALITY;
		} else {
			k2b = role2.startKey();
			stop2 = role2.stopKey();
			cardinality = VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY;
		}
		Scan scan = scan(
			k1b, k2b, get3StartKey(), get4StartKey(),
			k1b, stop2, get3StopKey(), role4.endStopKey(),
			cardinality,
			false
		);
		Filter qf = new ColumnPrefixFilter(qualifier(k1, k2, null, null));
		if (constraint != null) {
			List<Filter> filters = new ArrayList<>();
			filters.add(qf);
			appendLiteralFilters(concat2(k1b, k2b), k2 == null ? concat2(k1b, stop2) : null,
				get3StartKey(), get3StopKey(),
				get4StartKey(),
				role4.endStopKey(),
				constraint, filters);
			scan.setFilter(new FilterList(filters));
		} else {
			scan.setFilter(qf);
		}
		return scan;
	}
	public Scan scan(RDFIdentifier<T1> k1, RDFIdentifier<T2> k2, RDFIdentifier<T3> k3) {
		return scanWithConstraint(k1, k2, k3, null);
	}
	public Scan scanWithConstraint(RDFIdentifier<T1> k1, RDFIdentifier<T2> k2, RDFIdentifier<T3> k3, ValueConstraint constraint) {
		ByteSequence k1b = new ByteArray(k1.getKeyHash(this));
		ByteSequence k2b = new ByteArray(k2.getKeyHash(this));
		ByteSequence k3b, stop3;
		int cardinality;
		if (k3 != null) {
			k3b = new ByteArray(get3KeyHash(this, k3));
			stop3 = k3b;
			cardinality = VAR_CARDINALITY;
		} else {
			k3b = get3StartKey();
			stop3 = get3StopKey();
			cardinality = VAR_CARDINALITY*VAR_CARDINALITY;
		}
		Scan scan = scan(
			k1b, k2b, k3b, get4StartKey(),
			k1b, k2b, stop3, role4.endStopKey(),
			cardinality,
			false
		);
		Filter qf = new ColumnPrefixFilter(qualifier(k1, k2, k3, null));
		if (constraint != null) {
			List<Filter> filters = new ArrayList<>();
			filters.add(qf);
			appendLiteralFilters(concat3(k1b, k2b, k3b), k3 == null ? concat3(k1b, k2b, stop3) : null,
				get4StartKey(), role4.endStopKey(),
				ByteSequence.EMPTY,
				ByteSequence.EMPTY,
				constraint, filters);
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
		int cardinality = 1;
		return scan(
			new ByteArray(k1b), new ByteArray(k2b), new ByteArray(k3b), new ByteArray(k4b),
			new ByteArray(k1b), new ByteArray(k2b), new ByteArray(k3b), new ByteArray(k4b),
			cardinality,
			false
		).setFilter(new ColumnPrefixFilter(qualifier(k1, k2, k3, k4)));
	}

	private void appendLiteralFilters(ByteSequence prefix, ByteSequence stopPrefix, ByteSequence startKey, ByteSequence stopKey, ByteSequence trailingStartKeys, ByteSequence trailingStopKeys, ValueConstraint constraint, List<Filter> filters) {
		ValueType type = constraint.getValueType();
		IRI dt = null;
		if ((constraint instanceof ObjectConstraint)) {
			ObjectConstraint objConstraint = (ObjectConstraint) constraint;
			dt = objConstraint.getDatatype();
		}
		int typeSaltSize = rdfFactory.typeSaltSize;
		List<RowRange> ranges;
		if (stopPrefix == null) {
			ranges = new ArrayList<>(typeSaltSize);
			for (int i=0; i<typeSaltSize; i++) {
				byte[] startRow = concat(false, prefix, rdfFactory.writeSaltAndType(i, type, dt, startKey), trailingStartKeys); // exclusive
				byte[] stopRow = concat(true, prefix, rdfFactory.writeSaltAndType(i, type, dt, stopKey), trailingStopKeys); // exclusive
				ranges.add(new RowRange(startRow, true, stopRow, false));
			}
		} else {
			byte[] startRow = concat(false, prefix, rdfFactory.writeSaltAndType(0, type, dt, startKey), trailingStartKeys); // inclusive
			byte[] stopRow = concat(true, stopPrefix, rdfFactory.writeSaltAndType(typeSaltSize-1, type, dt, stopKey), trailingStopKeys); // exclusive
			ranges = Collections.<RowRange>singletonList(new RowRange(startRow, true, stopRow, false));
		}
		filters.add(new MultiRowRangeFilter(ranges));
	}

	/**
     * Helper method concatenating key parts.
     * @param trailingZero boolean switch adding trailing zero to the resulting key
     * @param fragments variable number of the key fragments as ByteSequences
     * @return concatenated key as byte array
     */
    private byte[] concat(boolean trailingZero, ByteSequence... fragments) {
        int totalLen = 1; // for prefix
        for (ByteSequence fr : fragments) {
            totalLen += fr.size();
        }
        byte[] res = new byte[trailingZero ? totalLen + 1 : totalLen];
        ByteBuffer bb = ByteBuffer.wrap(res);
        bb.put(prefix);
        for (ByteSequence fr : fragments) {
            fr.writeTo(bb);
        }
        return res;
    }

	@Override
	public String toString() {
		return name.toString();
	}
}
