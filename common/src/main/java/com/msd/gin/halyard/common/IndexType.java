package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;

enum IndexType {
	TRIPLE {
		byte[] row(StatementIndex index, RDFIdentifier v1, RDFIdentifier v2, RDFIdentifier v3, RDFIdentifier v4) {
			ByteBuffer r = ByteBuffer.allocate(1 + v1.keyHashSize() + v2.keyHashSize() + v3.endKeyHashSize() + (v4 != null ? v4.keyHashSize() : 0));
			r.put(index.prefix);
			r.put(v1.getKeyHash(index));
			r.put(v2.getKeyHash(index));
			r.put(v3.getEndKeyHash(index));
    		if(v4 != null) {
				r.put(v4.getKeyHash(index));
    		}
    		return r.array();
		}

		byte[] qualifier(StatementIndex index, RDFIdentifier v1, RDFIdentifier v2, RDFIdentifier v3, RDFIdentifier v4) {
			ByteBuffer cq = ByteBuffer.allocate(v1.qualifierHashSize() + (v2 != null ? v2.qualifierHashSize() : 0) + (v3 != null ? v3.endQualifierHashSize() : 0) + (v4 != null ? v4.qualifierHashSize() : 0));
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
    		return cq.array();
    	}

		Scan scan(StatementIndex index, RDFIdentifier k1, RDFIdentifier k2, RDFIdentifier k3) {
			byte[] k1b = k1.getKeyHash(index);
			byte[] k2b = k2.getKeyHash(index);
			byte[] k3b = k3.getEndKeyHash(index);
			byte[][] stopKeys = index.newStopKeys();
			stopKeys[0] = k1b;
			stopKeys[1] = k2b;
			stopKeys[2] = k3b;
			return HalyardTableUtils.scan(concat(index, false, k1b, k2b, k3b), concat(index, true, stopKeys)).setFilter(new ColumnPrefixFilter(index.qualifier(k1, k2, k3, null)));
		}

		Scan scan(StatementIndex index, RDFIdentifier k1, RDFIdentifier k2, RDFIdentifier k3, RDFIdentifier k4) {
			byte[] k1b = k1.getKeyHash(index);
			byte[] k2b = k2.getKeyHash(index);
			byte[] k3b = k3.getEndKeyHash(index);
			byte[] k4b = k4.getKeyHash(index);
			return HalyardTableUtils.scan(concat(index, false, k1b, k2b, k3b, k4b), concat(index, true, k1b, k2b, k3b, k4b)).setFilter(new ColumnPrefixFilter(index.qualifier(k1, k2, k3, k4)));
		}
	},

	QUAD {
		byte[] row(StatementIndex index, RDFIdentifier v1, RDFIdentifier v2, RDFIdentifier v3, RDFIdentifier v4) {
    		ByteBuffer r = ByteBuffer.allocate(1 + v1.keyHashSize() + v2.keyHashSize() + v3.keyHashSize() + v4.endKeyHashSize());
			r.put(index.prefix);
    		r.put(v1.getKeyHash(index));
    		r.put(v2.getKeyHash(index));
    		r.put(v3.getKeyHash(index));
   			r.put(v4.getEndKeyHash(index));
   			return r.array();
		}

		byte[] qualifier(StatementIndex index, RDFIdentifier v1, RDFIdentifier v2, RDFIdentifier v3, RDFIdentifier v4) {
    		ByteBuffer cq = ByteBuffer.allocate(v1.qualifierHashSize() + (v2 != null ? v2.qualifierHashSize() : 0) + (v3 != null ? v3.qualifierHashSize() : 0) + (v4 != null ? v4.endQualifierHashSize() : 0));
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
    		return cq.array();
		}

		Scan scan(StatementIndex index, RDFIdentifier k1, RDFIdentifier k2, RDFIdentifier k3) {
			byte[] k1b = k1.getKeyHash(index);
			byte[] k2b = k2.getKeyHash(index);
			byte[] k3b = k3.getKeyHash(index);
			byte[][] stopKeys = index.newStopKeys();
			stopKeys[0] = k1b;
			stopKeys[1] = k2b;
			stopKeys[2] = k3b;
			return HalyardTableUtils.scan(concat(index, false, k1b, k2b, k3b), concat(index, true, stopKeys)).setFilter(new ColumnPrefixFilter(index.qualifier(k1, k2, k3, null)));
		}

		Scan scan(StatementIndex index, RDFIdentifier k1, RDFIdentifier k2, RDFIdentifier k3, RDFIdentifier k4) {
			byte[] k1b = k1.getKeyHash(index);
			byte[] k2b = k2.getKeyHash(index);
			byte[] k3b = k3.getKeyHash(index);
			byte[] k4b = k4.getEndKeyHash(index);
			return HalyardTableUtils.scan(concat(index, false, k1b, k2b, k3b, k4b), concat(index, true, k1b, k2b, k3b, k4b)).setFilter(new ColumnPrefixFilter(index.qualifier(k1, k2, k3, k4)));
		}
	};

	/** exclusive */
	private static final byte[] LITERAL_STOP_KEY = new byte[] { (byte) 0x80 };

	static Scan scanLiterals() {
		StatementIndex index = StatementIndex.OSP;
		return HalyardTableUtils.scan(concat(index, false), concat(index, false, LITERAL_STOP_KEY));
	}

	static Scan scanLiterals(RDFContext ctx) {
		StatementIndex index = StatementIndex.COSP;
		byte[] ctxb = ctx.getKeyHash(index);
		return HalyardTableUtils.scan(concat(index, false, ctxb), concat(index, false, ctxb, LITERAL_STOP_KEY)).setFilter(new ColumnPrefixFilter(index.qualifier(ctx, null, null, null)));
	}

    /**
     * Helper method concatenating keys
     * @param prefix key prefix byte
     * @param trailingZero boolean switch adding trailing zero to the resulting key
     * @param fragments variable number of the key fragments as byte arrays
     * @return concatenated key as byte array
     */
    private static byte[] concat(StatementIndex index, boolean trailingZero, byte[]... fragments) {
        int i = 1;
        for (byte[] fr : fragments) {
            i += fr.length;
        }
        byte[] res = new byte[trailingZero ? i + 1 : i];
        res[0] = index.prefix;
        i = 1;
        for (byte[] fr : fragments) {
            System.arraycopy(fr, 0, res, i, fr.length);
            i += fr.length;
        }
        return res;
    }

	abstract byte[] row(StatementIndex index, RDFIdentifier v1, RDFIdentifier v2, RDFIdentifier v3, RDFIdentifier v4);
	abstract byte[] qualifier(StatementIndex index, RDFIdentifier v1, RDFIdentifier v2, RDFIdentifier v3, RDFIdentifier v4);
	final Scan scan(StatementIndex index) {
		return HalyardTableUtils.scan(concat(index, false), concat(index, true, index.newStopKeys()));
	}
	final Scan scan(StatementIndex index, byte[] id) {
		byte[] kb = index.keyHash(id);
		byte[][] stopKeys = index.newStopKeys();
		stopKeys[0] = kb;
		return HalyardTableUtils.scan(concat(index, false, kb), concat(index, true, stopKeys)).setFilter(new ColumnPrefixFilter(index.qualifierHash(id)));
	}
	final Scan scan(StatementIndex index, RDFIdentifier k) {
		byte[] kb = k.getKeyHash(index);
		byte[][] stopKeys = index.newStopKeys();
		stopKeys[0] = kb;
		return HalyardTableUtils.scan(concat(index, false, kb), concat(index, true, stopKeys)).setFilter(new ColumnPrefixFilter(index.qualifier(k, null, null, null)));
	}
	final Scan scan(StatementIndex index, RDFIdentifier k1, RDFIdentifier k2) {
		byte[] k1b = k1.getKeyHash(index);
		byte[] k2b = k2.getKeyHash(index);
		byte[][] stopKeys = index.newStopKeys();
		stopKeys[0] = k1b;
		stopKeys[1] = k2b;
		return HalyardTableUtils.scan(concat(index, false, k1b, k2b), concat(index, true, stopKeys)).setFilter(new ColumnPrefixFilter(index.qualifier(k1, k2, null, null)));
	}
	abstract Scan scan(StatementIndex index, RDFIdentifier k1, RDFIdentifier k2, RDFIdentifier k3);
	abstract Scan scan(StatementIndex index, RDFIdentifier k1, RDFIdentifier k2, RDFIdentifier k3, RDFIdentifier k4);
}
