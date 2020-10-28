package com.msd.gin.halyard.common;

import com.msd.gin.halyard.common.HalyardTableUtils.TripleFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.client.Scan;
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
    	Statement parseStatement(RDFSubject subj, RDFPredicate pred, RDFObject obj, RDFContext ctx, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, ValueFactory vf, TripleFactory tf) throws IOException {
    		Resource s = parseShortRDFValue(this, RDFRole.SUBJECT, subj, key, cn, cv, RDFSubject.KEY_SIZE, vf, tf);
    		IRI p = parseShortRDFValue(this, RDFRole.PREDICATE, pred, key, cn, cv, RDFPredicate.KEY_SIZE, vf, null);
    		Value o = parseIntRDFValue(this, RDFRole.OBJECT, obj, key, cn, cv, RDFObject.END_KEY_SIZE, vf, tf);
    		Resource c = parseLastRDFValue(this, RDFRole.CONTEXT, ctx, key, cn, cv, RDFContext.KEY_SIZE, vf, null);
    		return createStatement(s, p, o, c, vf);
    	}
    	byte[][] newStopKeys() {
    		return new byte[][] {RDFSubject.STOP_KEY, RDFPredicate.STOP_KEY, RDFObject.END_STOP_KEY, RDFContext.STOP_KEY};
    	}
    	byte[] keyHash(byte[] id) {
    		return RDFRole.SUBJECT.keyHash(this, id);
    	}
    	byte[] qualifierHash(byte[] id) {
    		return RDFRole.SUBJECT.qualifierHash(id);
    	}
	},
	POS(1, IndexType.TRIPLE) {
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
    	Statement parseStatement(RDFSubject subj, RDFPredicate pred, RDFObject obj, RDFContext ctx, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, ValueFactory vf, TripleFactory tf) throws IOException {
    		IRI p = parseShortRDFValue(this, RDFRole.PREDICATE, pred, key, cn, cv, RDFPredicate.KEY_SIZE, vf, null);
    		Value o = parseIntRDFValue(this, RDFRole.OBJECT, obj, key, cn, cv, RDFObject.KEY_SIZE, vf, tf);
    		Resource s = parseShortRDFValue(this, RDFRole.SUBJECT, subj, key, cn, cv, RDFSubject.END_KEY_SIZE, vf, tf);
    		Resource c = parseLastRDFValue(this, RDFRole.CONTEXT, ctx, key, cn, cv, RDFContext.KEY_SIZE, vf, null);
    		return createStatement(s, p, o, c, vf);
    	}
    	byte[][] newStopKeys() {
    		return new byte[][] {RDFPredicate.STOP_KEY, RDFObject.STOP_KEY, RDFSubject.END_STOP_KEY, RDFContext.STOP_KEY};
    	}
    	byte[] keyHash(byte[] id) {
    		return RDFRole.PREDICATE.keyHash(this, id);
    	}
    	byte[] qualifierHash(byte[] id) {
    		return RDFRole.PREDICATE.qualifierHash(id);
    	}
	},
	OSP(2, IndexType.TRIPLE) {
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
    	Statement parseStatement(RDFSubject subj, RDFPredicate pred, RDFObject obj, RDFContext ctx, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, ValueFactory vf, TripleFactory tf) throws IOException {
    		Value o = parseIntRDFValue(this, RDFRole.OBJECT, obj, key, cn, cv, RDFObject.KEY_SIZE, vf, tf);
    		Resource s = parseShortRDFValue(this, RDFRole.SUBJECT, subj, key, cn, cv, RDFSubject.KEY_SIZE, vf, tf);
    		IRI p = parseShortRDFValue(this, RDFRole.PREDICATE, pred, key, cn, cv, RDFPredicate.END_KEY_SIZE, vf, null);
    		Resource c = parseLastRDFValue(this, RDFRole.CONTEXT, ctx, key, cn, cv, RDFContext.KEY_SIZE, vf, null);
    		return createStatement(s, p, o, c, vf);
    	}
    	byte[][] newStopKeys() {
    		return new byte[][] {RDFObject.STOP_KEY, RDFSubject.STOP_KEY, RDFPredicate.END_STOP_KEY, RDFContext.STOP_KEY};
    	}
    	byte[] keyHash(byte[] id) {
    		return RDFRole.OBJECT.keyHash(this, id);
    	}
    	byte[] qualifierHash(byte[] id) {
    		return RDFRole.OBJECT.qualifierHash(id);
    	}
	},
	CSPO(3, IndexType.QUAD) {
		byte[] value(RDFValue<?> v1, RDFValue<?> v2, RDFValue<?> v3, RDFValue<?> v4) {
			ByteBuffer cv = ByteBuffer.allocate(len(v1, 2) + len(v2, 2) + len(v3, 2) + len(v4, 0));
			putShortRDFValue(cv, v1);
			putShortRDFValue(cv, v2);
			putShortRDFValue(cv, v3);
			putLastRDFValue(cv, v4);
			return cv.array();
		}
    	Statement parseStatement(RDFSubject subj, RDFPredicate pred, RDFObject obj, RDFContext ctx, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, ValueFactory vf, TripleFactory tf) throws IOException {
    		Resource c = parseShortRDFValue(this, RDFRole.CONTEXT, ctx, key, cn, cv, RDFContext.KEY_SIZE, vf, null);
    		Resource s = parseShortRDFValue(this, RDFRole.SUBJECT, subj, key, cn, cv, RDFSubject.KEY_SIZE, vf, tf);
    		IRI p = parseShortRDFValue(this, RDFRole.PREDICATE, pred, key, cn, cv, RDFPredicate.KEY_SIZE, vf, null);
    		Value o = parseLastRDFValue(this, RDFRole.OBJECT, obj, key, cn, cv, RDFObject.END_KEY_SIZE, vf, tf);
    		return createStatement(s, p, o, c, vf);
    	}
    	byte[][] newStopKeys() {
    		return new byte[][] {RDFContext.STOP_KEY, RDFSubject.STOP_KEY, RDFPredicate.STOP_KEY, RDFObject.END_STOP_KEY};
    	}
    	byte[] keyHash(byte[] id) {
    		return RDFRole.CONTEXT.keyHash(this, id);
    	}
    	byte[] qualifierHash(byte[] id) {
    		return RDFRole.CONTEXT.qualifierHash(id);
    	}
	},
	CPOS(4, IndexType.QUAD) {
		byte[] value(RDFValue<?> v1, RDFValue<?> v2, RDFValue<?> v3, RDFValue<?> v4) {
			ByteBuffer cv = ByteBuffer.allocate(len(v1, 2) + len(v2, 2) + len(v3, 4) + len(v4, 0));
			putShortRDFValue(cv, v1);
			putShortRDFValue(cv, v2);
			putIntRDFValue(cv, v3);
			putLastRDFValue(cv, v4);
			return cv.array();
		}
    	Statement parseStatement(RDFSubject subj, RDFPredicate pred, RDFObject obj, RDFContext ctx, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, ValueFactory vf, TripleFactory tf) throws IOException {
    		Resource c = parseShortRDFValue(this, RDFRole.CONTEXT, ctx, key, cn, cv, RDFContext.KEY_SIZE, vf, null);
    		IRI p = parseShortRDFValue(this, RDFRole.PREDICATE, pred, key, cn, cv, RDFPredicate.KEY_SIZE, vf, null);
    		Value o = parseIntRDFValue(this, RDFRole.OBJECT, obj, key, cn, cv, RDFObject.KEY_SIZE, vf, tf);
    		Resource s = parseLastRDFValue(this, RDFRole.SUBJECT, subj, key, cn, cv, RDFSubject.END_KEY_SIZE, vf, tf);
    		return createStatement(s, p, o, c, vf);
    	}
    	byte[][] newStopKeys() {
    		return new byte[][] {RDFContext.STOP_KEY, RDFPredicate.STOP_KEY, RDFObject.STOP_KEY, RDFSubject.END_STOP_KEY};
    	}
    	byte[] keyHash(byte[] id) {
    		return RDFRole.CONTEXT.keyHash(this, id);
    	}
    	byte[] qualifierHash(byte[] id) {
    		return RDFRole.CONTEXT.qualifierHash(id);
    	}
	},
	COSP(5, IndexType.QUAD) {
		byte[] value(RDFValue<?> v1, RDFValue<?> v2, RDFValue<?> v3, RDFValue<?> v4) {
			ByteBuffer cv = ByteBuffer.allocate(len(v1, 2) + len(v2, 4) + len(v3, 2) + len(v4, 0));
			putShortRDFValue(cv, v1);
			putIntRDFValue(cv, v2);
			putShortRDFValue(cv, v3);
			putLastRDFValue(cv, v4);
			return cv.array();
		}
    	Statement parseStatement(RDFSubject subj, RDFPredicate pred, RDFObject obj, RDFContext ctx, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, ValueFactory vf, TripleFactory tf) throws IOException {
    		Resource c = parseShortRDFValue(this, RDFRole.CONTEXT, ctx, key, cn, cv, RDFContext.KEY_SIZE, vf, null);
    		Value o = parseIntRDFValue(this, RDFRole.OBJECT, obj, key, cn, cv, RDFObject.KEY_SIZE, vf, tf);
    		Resource s = parseShortRDFValue(this, RDFRole.SUBJECT, subj, key, cn, cv, RDFSubject.KEY_SIZE, vf, tf);
    		IRI p = parseLastRDFValue(this, RDFRole.PREDICATE, pred, key, cn, cv, RDFPredicate.END_KEY_SIZE, vf, null);
    		return createStatement(s, p, o, c, vf);
    	}
    	byte[][] newStopKeys() {
    		return new byte[][] {RDFContext.STOP_KEY, RDFObject.STOP_KEY, RDFSubject.STOP_KEY, RDFPredicate.END_STOP_KEY};
    	}
    	byte[] keyHash(byte[] id) {
    		return RDFRole.CONTEXT.keyHash(this, id);
    	}
    	byte[] qualifierHash(byte[] id) {
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

	public static final Scan scanLiterals() {
		return IndexType.scanLiterals();
	}
	public static final Scan scanLiterals(RDFContext ctx) {
		return IndexType.scanLiterals(ctx);
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
		if (ValueIO.WELL_KNOWN_IRIS.containsValue(v.val)) {
			return 1;
		} else {
			return sizeLen + v.getSerializedForm().length;
		}
	}

	private static void putShortRDFValue(ByteBuffer cv, RDFValue<?> v) {
		if (ValueIO.WELL_KNOWN_IRIS.containsValue(v.val)) {
			cv.put(WELL_KNOWN_IRI_MARKER);
		} else {
			byte[] ser = v.getSerializedForm();
			cv.putShort((short) ser.length).put(ser);
		}
	}

	private static void putIntRDFValue(ByteBuffer cv, RDFValue<?> v) {
		if (ValueIO.WELL_KNOWN_IRIS.containsValue(v.val)) {
			cv.put(WELL_KNOWN_IRI_MARKER);
		} else {
			byte[] ser = v.getSerializedForm();
			cv.putInt(ser.length).put(ser);
		}
	}

	private static void putLastRDFValue(ByteBuffer cv, RDFValue<?> v) {
		if (ValueIO.WELL_KNOWN_IRIS.containsValue(v.val)) {
			cv.put(WELL_KNOWN_IRI_MARKER);
		} else {
			byte[] ser = v.getSerializedForm();
			cv.put(ser);
		}
	}

    private static <V extends Value> V parseShortRDFValue(StatementIndex index, RDFRole role, RDFValue<V> pattern, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, int keySize, ValueFactory vf, TripleFactory tf) throws IOException {
    	byte marker = cv.get(cv.position()); // peek
    	int len;
    	if (marker == WELL_KNOWN_IRI_MARKER) {
    		len = cv.get();
    	} else {
    		len = cv.getShort();
    	}
   		return parseRDFValue(index, role, pattern, key, cn, cv, keySize, Hashes.ID_SIZE, len, vf, tf);
    }

    private static <V extends Value> V parseIntRDFValue(StatementIndex index, RDFRole role, RDFValue<V> pattern, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, int keySize, ValueFactory vf, TripleFactory tf) throws IOException {
    	byte marker = cv.get(cv.position()); // peek
    	int len;
    	if (marker == WELL_KNOWN_IRI_MARKER) {
    		len = cv.get();
    	} else {
    		len = cv.getInt();
    	}
   		return parseRDFValue(index, role, pattern, key, cn, cv, keySize, Hashes.ID_SIZE, len, vf, tf);
    }

    private static <V extends Value> V parseLastRDFValue(StatementIndex index, RDFRole role, RDFValue<V> pattern, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, int keySize, ValueFactory vf, TripleFactory tf) throws IOException {
    	byte marker = cv.hasRemaining() ? cv.get(cv.position()) : 0; // peek
    	int len;
    	if (marker == WELL_KNOWN_IRI_MARKER) {
    		len = cv.get();
    	} else {
    		len = cv.remaining();
    	}
   		return parseRDFValue(index, role, pattern, key, cn, cv, keySize, Hashes.ID_SIZE, len, vf, tf);
    }

    @SuppressWarnings("unchecked")
	private static <V extends Value> V parseRDFValue(StatementIndex index, RDFRole role, RDFValue<V> pattern, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, int keySize, int idSize, int len, ValueFactory vf, TripleFactory tf) throws IOException {
    	if(pattern != null) {
    		// if we have been given the value then don't bother to read it and skip to the next
    		skipId(key, cn, keySize, idSize);
    		if (len > 0) {
    			cv.position(cv.position() + len);
    		}
			return pattern.val;
    	} else if(len == WELL_KNOWN_IRI_MARKER) {
			ByteBuffer id = parseId(index, role, key, cn, keySize, idSize);
			IRI iri = ValueIO.WELL_KNOWN_IRI_IDS.get(id);
			if (iri == null) {
				throw new IllegalStateException(String.format("Unknown IRI hash: %s", Hashes.encode(id)));
			}
			return (V) iri;
		} else if(len > 0) {
			ByteBuffer id = parseId(index, role, key, cn, keySize, idSize);
			int limit = cv.limit();
			cv.limit(cv.position() + len);
			V value = (V) ValueIO.readValue(cv, vf, tf);
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

	private static ByteBuffer parseId(StatementIndex index, RDFRole role, ByteBuffer key, ByteBuffer cn, int keySize, int idSize) {
		ByteBuffer id = ByteBuffer.allocate(idSize);
		role.rotateLeft(key.array(), key.arrayOffset()+key.position(), keySize, index, id.array());
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
	abstract Statement parseStatement(RDFSubject subj, RDFPredicate pred, RDFObject obj, RDFContext ctx, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, ValueFactory vf, TripleFactory tf) throws IOException;
	abstract byte[][] newStopKeys();
	abstract byte[] keyHash(byte[] id);
	abstract byte[] qualifierHash(byte[] id);
	public final Scan scan() {
		return indexType.scan(this);
	}
	final Scan scan(byte[] id) {
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
}
