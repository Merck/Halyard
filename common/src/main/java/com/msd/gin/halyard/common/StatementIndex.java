package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;

/**
 * Triples/quads are stored in multiple indices as different permutations.
 * These enums define each index.
 */
public enum StatementIndex {
	SPO(0, IndexType.TRIPLE) {
		@Override
		byte[] value(RDFValue<?> v1, RDFValue<?> v2, RDFValue<?> v3, RDFValue<?> v4) {
			ByteBuffer cv = ByteBuffer.allocate(len(v1, 2) + len(v2, 2) + len(v3, 4) + (v4 != null ? len(v4, 0) : 0));
			putShortRDFValue(cv, v1);
			putShortRDFValue(cv, v2);
			putIntRDFValue(cv, v3);
			if (v4 != null) {
				putLastRDFValue(cv, v4);
			}
			return cv.array();
		}
		@Override
    	Statement parseStatement(RDFSubject subj, RDFPredicate pred, RDFObject obj, RDFContext ctx, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, ValueIO.Reader reader) {
    		Resource s = parseShortRDFValue(this, RDFRole.SUBJECT, subj, key, cn, cv, RDFSubject.KEY_SIZE, reader);
    		IRI p = parseShortRDFValue(this, RDFRole.PREDICATE, pred, key, cn, cv, RDFPredicate.KEY_SIZE, reader);
    		Value o = parseIntRDFValue(this, RDFRole.OBJECT, obj, key, cn, cv, RDFObject.END_KEY_SIZE, reader);
    		Resource c = parseLastRDFValue(this, RDFRole.CONTEXT, ctx, key, cn, cv, RDFContext.KEY_SIZE, reader);
    		return createStatement(s, p, o, c, reader.getValueFactory());
    	}
		@Override
    	byte[][] newStopKeys() {
    		return new byte[][] {RDFSubject.STOP_KEY, RDFPredicate.STOP_KEY, RDFObject.END_STOP_KEY, RDFContext.STOP_KEY};
    	}
		@Override
    	byte[] keyHash(Identifier id) {
    		return RDFRole.SUBJECT.keyHash(this, id);
    	}
		@Override
		byte[] qualifierHash(Identifier id) {
			return RDFRole.SUBJECT.qualifierHash(id);
		}
	},
	POS(1, IndexType.TRIPLE) {
		@Override
		byte[] value(RDFValue<?> v1, RDFValue<?> v2, RDFValue<?> v3, RDFValue<?> v4) {
			ByteBuffer cv = ByteBuffer.allocate(len(v1, 2) + len(v2, 4) + len(v3, 2) + (v4 != null ? len(v4, 0) : 0));
			putShortRDFValue(cv, v1);
			putIntRDFValue(cv, v2);
			putShortRDFValue(cv, v3);
			if (v4 != null) {
				putLastRDFValue(cv, v4);
			}
			return cv.array();
		}
		@Override
    	Statement parseStatement(RDFSubject subj, RDFPredicate pred, RDFObject obj, RDFContext ctx, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, ValueIO.Reader reader) {
    		IRI p = parseShortRDFValue(this, RDFRole.PREDICATE, pred, key, cn, cv, RDFPredicate.KEY_SIZE, reader);
    		Value o = parseIntRDFValue(this, RDFRole.OBJECT, obj, key, cn, cv, RDFObject.KEY_SIZE, reader);
    		Resource s = parseShortRDFValue(this, RDFRole.SUBJECT, subj, key, cn, cv, RDFSubject.END_KEY_SIZE, reader);
    		Resource c = parseLastRDFValue(this, RDFRole.CONTEXT, ctx, key, cn, cv, RDFContext.KEY_SIZE, reader);
    		return createStatement(s, p, o, c, reader.getValueFactory());
    	}
		@Override
    	byte[][] newStopKeys() {
    		return new byte[][] {RDFPredicate.STOP_KEY, RDFObject.STOP_KEY, RDFSubject.END_STOP_KEY, RDFContext.STOP_KEY};
    	}
		@Override
    	byte[] keyHash(Identifier id) {
    		return RDFRole.PREDICATE.keyHash(this, id);
    	}
		@Override
		byte[] qualifierHash(Identifier id) {
			return RDFRole.PREDICATE.qualifierHash(id);
		}
	},
	OSP(2, IndexType.TRIPLE) {
		@Override
		byte[] value(RDFValue<?> v1, RDFValue<?> v2, RDFValue<?> v3, RDFValue<?> v4) {
			ByteBuffer cv = ByteBuffer.allocate(len(v1, 4) + len(v2, 2) + len(v3, 2) + (v4 != null ? len(v4, 0) : 0));
			putIntRDFValue(cv, v1);
			putShortRDFValue(cv, v2);
			putShortRDFValue(cv, v3);
			if (v4 != null) {
				putLastRDFValue(cv, v4);
			}
			return cv.array();
		}
		@Override
    	Statement parseStatement(RDFSubject subj, RDFPredicate pred, RDFObject obj, RDFContext ctx, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, ValueIO.Reader reader) {
    		Value o = parseIntRDFValue(this, RDFRole.OBJECT, obj, key, cn, cv, RDFObject.KEY_SIZE, reader);
    		Resource s = parseShortRDFValue(this, RDFRole.SUBJECT, subj, key, cn, cv, RDFSubject.KEY_SIZE, reader);
    		IRI p = parseShortRDFValue(this, RDFRole.PREDICATE, pred, key, cn, cv, RDFPredicate.END_KEY_SIZE, reader);
    		Resource c = parseLastRDFValue(this, RDFRole.CONTEXT, ctx, key, cn, cv, RDFContext.KEY_SIZE, reader);
    		return createStatement(s, p, o, c, reader.getValueFactory());
    	}
		@Override
    	byte[][] newStopKeys() {
    		return new byte[][] {RDFObject.STOP_KEY, RDFSubject.STOP_KEY, RDFPredicate.END_STOP_KEY, RDFContext.STOP_KEY};
    	}
		@Override
    	byte[] keyHash(Identifier id) {
    		return RDFRole.OBJECT.keyHash(this, id);
    	}
		@Override
		byte[] qualifierHash(Identifier id) {
			return RDFRole.OBJECT.qualifierHash(id);
		}
	},
	CSPO(3, IndexType.QUAD) {
		@Override
		byte[] value(RDFValue<?> v1, RDFValue<?> v2, RDFValue<?> v3, RDFValue<?> v4) {
			ByteBuffer cv = ByteBuffer.allocate(len(v1, 2) + len(v2, 2) + len(v3, 2) + len(v4, 0));
			putShortRDFValue(cv, v1);
			putShortRDFValue(cv, v2);
			putShortRDFValue(cv, v3);
			putLastRDFValue(cv, v4);
			return cv.array();
		}
		@Override
    	Statement parseStatement(RDFSubject subj, RDFPredicate pred, RDFObject obj, RDFContext ctx, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, ValueIO.Reader reader) {
    		Resource c = parseShortRDFValue(this, RDFRole.CONTEXT, ctx, key, cn, cv, RDFContext.KEY_SIZE, reader);
    		Resource s = parseShortRDFValue(this, RDFRole.SUBJECT, subj, key, cn, cv, RDFSubject.KEY_SIZE, reader);
    		IRI p = parseShortRDFValue(this, RDFRole.PREDICATE, pred, key, cn, cv, RDFPredicate.KEY_SIZE, reader);
    		Value o = parseLastRDFValue(this, RDFRole.OBJECT, obj, key, cn, cv, RDFObject.END_KEY_SIZE, reader);
    		return createStatement(s, p, o, c, reader.getValueFactory());
    	}
		@Override
    	byte[][] newStopKeys() {
    		return new byte[][] {RDFContext.STOP_KEY, RDFSubject.STOP_KEY, RDFPredicate.STOP_KEY, RDFObject.END_STOP_KEY};
    	}
		@Override
    	byte[] keyHash(Identifier id) {
    		return RDFRole.CONTEXT.keyHash(this, id);
    	}
		@Override
		byte[] qualifierHash(Identifier id) {
			return RDFRole.CONTEXT.qualifierHash(id);
		}
	},
	CPOS(4, IndexType.QUAD) {
		@Override
		byte[] value(RDFValue<?> v1, RDFValue<?> v2, RDFValue<?> v3, RDFValue<?> v4) {
			ByteBuffer cv = ByteBuffer.allocate(len(v1, 2) + len(v2, 2) + len(v3, 4) + len(v4, 0));
			putShortRDFValue(cv, v1);
			putShortRDFValue(cv, v2);
			putIntRDFValue(cv, v3);
			putLastRDFValue(cv, v4);
			return cv.array();
		}
		@Override
    	Statement parseStatement(RDFSubject subj, RDFPredicate pred, RDFObject obj, RDFContext ctx, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, ValueIO.Reader reader) {
    		Resource c = parseShortRDFValue(this, RDFRole.CONTEXT, ctx, key, cn, cv, RDFContext.KEY_SIZE, reader);
    		IRI p = parseShortRDFValue(this, RDFRole.PREDICATE, pred, key, cn, cv, RDFPredicate.KEY_SIZE, reader);
    		Value o = parseIntRDFValue(this, RDFRole.OBJECT, obj, key, cn, cv, RDFObject.KEY_SIZE, reader);
    		Resource s = parseLastRDFValue(this, RDFRole.SUBJECT, subj, key, cn, cv, RDFSubject.END_KEY_SIZE, reader);
    		return createStatement(s, p, o, c, reader.getValueFactory());
    	}
		@Override
    	byte[][] newStopKeys() {
    		return new byte[][] {RDFContext.STOP_KEY, RDFPredicate.STOP_KEY, RDFObject.STOP_KEY, RDFSubject.END_STOP_KEY};
    	}
		@Override
    	byte[] keyHash(Identifier id) {
    		return RDFRole.CONTEXT.keyHash(this, id);
    	}
		@Override
		byte[] qualifierHash(Identifier id) {
			return RDFRole.CONTEXT.qualifierHash(id);
		}
	},
	COSP(5, IndexType.QUAD) {
		@Override
		byte[] value(RDFValue<?> v1, RDFValue<?> v2, RDFValue<?> v3, RDFValue<?> v4) {
			ByteBuffer cv = ByteBuffer.allocate(len(v1, 2) + len(v2, 4) + len(v3, 2) + len(v4, 0));
			putShortRDFValue(cv, v1);
			putIntRDFValue(cv, v2);
			putShortRDFValue(cv, v3);
			putLastRDFValue(cv, v4);
			return cv.array();
		}
		@Override
    	Statement parseStatement(RDFSubject subj, RDFPredicate pred, RDFObject obj, RDFContext ctx, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, ValueIO.Reader reader) {
    		Resource c = parseShortRDFValue(this, RDFRole.CONTEXT, ctx, key, cn, cv, RDFContext.KEY_SIZE, reader);
    		Value o = parseIntRDFValue(this, RDFRole.OBJECT, obj, key, cn, cv, RDFObject.KEY_SIZE, reader);
    		Resource s = parseShortRDFValue(this, RDFRole.SUBJECT, subj, key, cn, cv, RDFSubject.KEY_SIZE, reader);
    		IRI p = parseLastRDFValue(this, RDFRole.PREDICATE, pred, key, cn, cv, RDFPredicate.END_KEY_SIZE, reader);
    		return createStatement(s, p, o, c, reader.getValueFactory());
    	}
		@Override
    	byte[][] newStopKeys() {
    		return new byte[][] {RDFContext.STOP_KEY, RDFObject.STOP_KEY, RDFSubject.STOP_KEY, RDFPredicate.END_STOP_KEY};
    	}
		@Override
    	byte[] keyHash(Identifier id) {
    		return RDFRole.CONTEXT.keyHash(this, id);
    	}
		@Override
		byte[] qualifierHash(Identifier id) {
			return RDFRole.CONTEXT.qualifierHash(id);
		}
	};

	private static final byte WELL_KNOWN_IRI_MARKER = (byte) ('#' | 0x80);  // marker must be negative (msb set) so it is distinguishable from a length (>=0)

	public static StatementIndex toIndex(byte prefix) {
		switch(prefix) {
			case 0: return SPO;
			case 1: return POS;
			case 2: return OSP;
			case 3: return CSPO;
			case 4: return CPOS;
			case 5: return COSP;
			default: throw new AssertionError(String.format("Invalid prefix: %s", prefix));
		}
	}

	public static final Scan scanLiterals(IdentifiableValueIO valueIO) {
		int typeSaltSize = valueIO.getTypeSaltSize();
		StatementIndex index = OSP;
		List<RowRange> ranges = new ArrayList<>(typeSaltSize);
		for (int i=0; i<typeSaltSize; i++) {
			byte[] startKey = index.concat(false, new byte[] {(byte) i}); // inclusive
			byte[] stopKey = index.concat(false, new byte[] {(byte) i, Identifier.LITERAL_STOP_BITS}); // exclusive
			ranges.add(new RowRange(startKey, true, stopKey, false));
		}
		return index.scan().setFilter(new MultiRowRangeFilter(ranges));
	}

	public static final Scan scanLiterals(Resource graph, IdentifiableValueIO valueIO) {
		RDFContext ctx = RDFContext.create(graph, valueIO);
		int typeSaltSize = valueIO.getTypeSaltSize();
		StatementIndex index = COSP;
		byte[] ctxb = ctx.getKeyHash(index);
		List<RowRange> ranges = new ArrayList<>(typeSaltSize);
		for (int i=0; i<typeSaltSize; i++) {
			byte[] startKey = index.concat(false, ctxb, new byte[] {(byte) i}); // inclusive
			byte[] stopKey = index.concat(false, ctxb, new byte[] {(byte) i, Identifier.LITERAL_STOP_BITS}); // exclusive
			ranges.add(new RowRange(startKey, true, stopKey, false));
		}
		return index.scan().setFilter(new MultiRowRangeFilter(ranges));
	}

    private static Statement createStatement(Resource s, IRI p, Value o, Resource c, ValueFactory vf) {
		if (c == null) {
			return vf.createStatement(s, p, o);
		} else {
			return vf.createStatement(s, p, o, c);
		}
    }

	/**
	 * @param sizeLen length of size field, 2 for short, 4 for int.
	 */
	private static int len(RDFValue<?> v, int sizeLen) {
		if (v.isWellKnownIRI()) {
			return 1;
		} else {
			return sizeLen + v.getSerializedForm().remaining();
		}
	}

	private static void putShortRDFValue(ByteBuffer cv, RDFValue<?> v) {
		if (v.isWellKnownIRI()) {
			cv.put(WELL_KNOWN_IRI_MARKER);
		} else {
			ByteBuffer ser = v.getSerializedForm();
			cv.putShort((short) ser.remaining()).put(ser);
		}
	}

	private static void putIntRDFValue(ByteBuffer cv, RDFValue<?> v) {
		if (v.isWellKnownIRI()) {
			cv.put(WELL_KNOWN_IRI_MARKER);
		} else {
			ByteBuffer ser = v.getSerializedForm();
			cv.putInt(ser.remaining()).put(ser);
		}
	}

	private static void putLastRDFValue(ByteBuffer cv, RDFValue<?> v) {
		if (v.isWellKnownIRI()) {
			cv.put(WELL_KNOWN_IRI_MARKER);
		} else {
			ByteBuffer ser = v.getSerializedForm();
			cv.put(ser);
		}
	}

    private static <V extends Value> V parseShortRDFValue(StatementIndex index, RDFRole role, @Nullable RDFValue<V> pattern, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, int keySize, ValueIO.Reader reader) {
    	byte marker = cv.get(cv.position()); // peek
    	int len;
    	if (marker == WELL_KNOWN_IRI_MARKER) {
    		len = cv.get();
    	} else {
    		len = cv.getShort();
    	}
   		return parseRDFValue(index, role, pattern, key, cn, cv, keySize, len, reader);
    }

    private static <V extends Value> V parseIntRDFValue(StatementIndex index, RDFRole role, @Nullable RDFValue<V> pattern, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, int keySize, ValueIO.Reader reader) {
    	byte marker = cv.get(cv.position()); // peek
    	int len;
    	if (marker == WELL_KNOWN_IRI_MARKER) {
    		len = cv.get();
    	} else {
    		len = cv.getInt();
    	}
   		return parseRDFValue(index, role, pattern, key, cn, cv, keySize, len, reader);
    }

    private static <V extends Value> V parseLastRDFValue(StatementIndex index, RDFRole role, @Nullable RDFValue<V> pattern, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, int keySize, ValueIO.Reader reader) {
    	byte marker = cv.hasRemaining() ? cv.get(cv.position()) : 0; // peek
    	int len;
    	if (marker == WELL_KNOWN_IRI_MARKER) {
    		len = cv.get();
    	} else {
    		len = cv.remaining();
    	}
   		return parseRDFValue(index, role, pattern, key, cn, cv, keySize, len, reader);
    }

    @SuppressWarnings("unchecked")
	private static <V extends Value> V parseRDFValue(StatementIndex index, RDFRole role, @Nullable RDFValue<V> pattern, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, int keySize, int len, ValueIO.Reader reader) {
    	IdentifiableValueIO valueIO = (IdentifiableValueIO) reader.getValueIO();
    	if(pattern != null) {
    		// if we have been given the value then don't bother to read it and skip to the next
    		skipId(key, cn, keySize, valueIO.getIdSize());
    		if (len > 0) {
    			cv.position(cv.position() + len);
    		}
			return pattern.val;
    	} else if(len == WELL_KNOWN_IRI_MARKER) {
			Identifier id = parseId(index, role, key, cn, keySize, valueIO);
			IRI iri = valueIO.getWellKnownIRI(id);
			if (iri == null) {
				throw new IllegalStateException(String.format("Unknown IRI hash: %s", id));
			}
			return (V) iri;
		} else if(len > 0) {
			Identifier id = parseId(index, role, key, cn, keySize, valueIO);
			int limit = cv.limit();
			cv.limit(cv.position() + len);
			V value = (V) reader.readValue(cv);
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

	private static Identifier parseId(StatementIndex index, RDFRole role, ByteBuffer key, ByteBuffer cn, int keySize, IdentifiableValueIO valueIO) {
		byte[] idBytes = new byte[valueIO.getIdSize()];
		role.unrotate(key.array(), key.arrayOffset()+key.position(), keySize, index, idBytes);
		key.position(key.position()+keySize);
		cn.get(idBytes, keySize, idBytes.length - keySize);
		return valueIO.id(idBytes);
	}

	private static void skipId(ByteBuffer key, ByteBuffer cn, int keySize, int idSize) {
		key.position(key.position() + keySize);
		cn.position(cn.position() + idSize - keySize);
	}

	protected final byte prefix;
	private final IndexType indexType;

	StatementIndex(int prefix, IndexType type) {
		this.prefix = (byte) prefix;
		this.indexType = type;
	}

	public final boolean isQuadIndex() {
		return indexType == IndexType.QUAD;
	}

	final byte[] row(RDFIdentifier v1, RDFIdentifier v2, RDFIdentifier v3, RDFIdentifier v4) {
		return indexType.row(this, v1, v2, v3, v4);
	}

	final byte[] qualifier(RDFIdentifier v1, RDFIdentifier v2, RDFIdentifier v3, RDFIdentifier v4) {
		return indexType.qualifier(this, v1, v2, v3, v4);
	}

	abstract byte[] value(RDFValue<?> v1, RDFValue<?> v2, RDFValue<?> v3, RDFValue<?> v4);
	abstract Statement parseStatement(@Nullable RDFSubject subj, @Nullable RDFPredicate pred, @Nullable RDFObject obj, @Nullable RDFContext ctx, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, ValueIO.Reader reader);
	abstract byte[][] newStopKeys();
	abstract byte[] keyHash(Identifier id);
	abstract byte[] qualifierHash(Identifier id);
	public final Scan scan() {
		return indexType.scan(this);
	}
	final Scan scan(Identifier id) {
		return indexType.scan(this, id);
	}
	public final Scan scan(RDFIdentifier k) {
		return indexType.scan(this, k);
	}
	public final Scan scan(RDFIdentifier k1, RDFIdentifier k2) {
		return indexType.scan(this, k1, k2);
	}
	public final Scan scan(RDFIdentifier k1, RDFIdentifier k2, RDFIdentifier k3) {
		return indexType.scan(this, k1, k2, k3);
	}
	public final Scan scan(RDFIdentifier k1, RDFIdentifier k2, RDFIdentifier k3, RDFIdentifier k4) {
		return indexType.scan(this, k1, k2, k3, k4);
	}

    /**
     * Helper method concatenating keys
     * @param trailingZero boolean switch adding trailing zero to the resulting key
     * @param fragments variable number of the key fragments as byte arrays
     * @return concatenated key as byte array
     */
    byte[] concat(boolean trailingZero, byte[]... fragments) {
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
        return res;
    }
}
