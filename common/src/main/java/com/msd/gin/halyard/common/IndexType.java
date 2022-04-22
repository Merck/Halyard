package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;

enum IndexType {
	TRIPLE {
		@Override
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

		@Override
		byte[] qualifier(StatementIndex index, RDFIdentifier v1, RDFIdentifier v2, RDFIdentifier v3, RDFIdentifier v4) {
			ByteBuffer cq = ByteBuffer.allocate(v1.qualifierHashSize() + (v2 != null ? v2.qualifierHashSize() : 0) + (v3 != null ? v3.endQualifierHashSize() : 0) + (v4 != null ? v4.qualifierHashSize() : 0));
			v1.writeQualifierHashTo(cq);
    		if(v2 != null) {
				v2.writeQualifierHashTo(cq);
        		if(v3 != null) {
					v3.writeEndQualifierHashTo(cq);
	        		if(v4 != null) {
						v4.writeQualifierHashTo(cq);
	        		}
        		}
    		}
    		return cq.array();
    	}

		@Override
		Scan scan(StatementIndex index, RDFIdentifier k1, RDFIdentifier k2, RDFIdentifier k3, RDFFactory rdfFactory) {
			byte[] k1b = k1.getKeyHash(index);
			byte[] k2b = k2.getKeyHash(index);
			byte[] k3b = k3.getEndKeyHash(index);
			byte[][] stopKeys = index.newStopKeys(rdfFactory);
			stopKeys[0] = k1b;
			stopKeys[1] = k2b;
			stopKeys[2] = k3b;
			return scan(index, new byte[][] {k1b, k2b, k3b}, stopKeys).setFilter(new ColumnPrefixFilter(index.qualifier(k1, k2, k3, null)));
		}

		@Override
		Scan scan(StatementIndex index, RDFIdentifier k1, RDFIdentifier k2, RDFIdentifier k3, RDFIdentifier k4) {
			byte[] k1b = k1.getKeyHash(index);
			byte[] k2b = k2.getKeyHash(index);
			byte[] k3b = k3.getEndKeyHash(index);
			byte[] k4b = k4.getKeyHash(index);
			return scan(index, new byte[][] {k1b, k2b, k3b, k4b}, new byte[][] {k1b, k2b, k3b, k4b}).setFilter(new ColumnPrefixFilter(index.qualifier(k1, k2, k3, k4)));
		}
	},

	QUAD {
		@Override
		byte[] row(StatementIndex index, RDFIdentifier v1, RDFIdentifier v2, RDFIdentifier v3, RDFIdentifier v4) {
    		ByteBuffer r = ByteBuffer.allocate(1 + v1.keyHashSize() + v2.keyHashSize() + v3.keyHashSize() + v4.endKeyHashSize());
			r.put(index.prefix);
    		r.put(v1.getKeyHash(index));
    		r.put(v2.getKeyHash(index));
    		r.put(v3.getKeyHash(index));
   			r.put(v4.getEndKeyHash(index));
   			return r.array();
		}

		@Override
		byte[] qualifier(StatementIndex index, RDFIdentifier v1, RDFIdentifier v2, RDFIdentifier v3, RDFIdentifier v4) {
    		ByteBuffer cq = ByteBuffer.allocate(v1.qualifierHashSize() + (v2 != null ? v2.qualifierHashSize() : 0) + (v3 != null ? v3.qualifierHashSize() : 0) + (v4 != null ? v4.endQualifierHashSize() : 0));
    		v1.writeQualifierHashTo(cq);
    		if(v2 != null) {
        		v2.writeQualifierHashTo(cq);
        		if(v3 != null) {
	        		v3.writeQualifierHashTo(cq);
	        		if(v4 != null) {
	        			v4.writeEndQualifierHashTo(cq);
	        		}
        		}
    		}
    		return cq.array();
		}

		@Override
		Scan scan(StatementIndex index, RDFIdentifier k1, RDFIdentifier k2, RDFIdentifier k3, RDFFactory rdfFactory) {
			byte[] k1b = k1.getKeyHash(index);
			byte[] k2b = k2.getKeyHash(index);
			byte[] k3b = k3.getKeyHash(index);
			byte[][] stopKeys = index.newStopKeys(rdfFactory);
			stopKeys[0] = k1b;
			stopKeys[1] = k2b;
			stopKeys[2] = k3b;
			return scan(index, new byte[][] {k1b, k2b, k3b}, stopKeys).setFilter(new ColumnPrefixFilter(index.qualifier(k1, k2, k3, null)));
		}

		@Override
		Scan scan(StatementIndex index, RDFIdentifier k1, RDFIdentifier k2, RDFIdentifier k3, RDFIdentifier k4) {
			byte[] k1b = k1.getKeyHash(index);
			byte[] k2b = k2.getKeyHash(index);
			byte[] k3b = k3.getKeyHash(index);
			byte[] k4b = k4.getEndKeyHash(index);
			return scan(index, new byte[][] {k1b, k2b, k3b, k4b}, new byte[][] {k1b, k2b, k3b, k4b}).setFilter(new ColumnPrefixFilter(index.qualifier(k1, k2, k3, k4)));
		}
	};

	abstract byte[] row(StatementIndex index, RDFIdentifier v1, RDFIdentifier v2, RDFIdentifier v3, RDFIdentifier v4);
	abstract byte[] qualifier(StatementIndex index, RDFIdentifier v1, RDFIdentifier v2, RDFIdentifier v3, RDFIdentifier v4);
	final Scan scan(StatementIndex index, RDFFactory rdfFactory) {
		return scan(index, new byte[0][], index.newStopKeys(rdfFactory));
	}
	final Scan scan(StatementIndex index, Identifier id, RDFFactory rdfFactory) {
		byte[] kb = index.keyHash(id, rdfFactory);
		byte[][] stopKeys = index.newStopKeys(rdfFactory);
		stopKeys[0] = kb;
		return scan(index, new byte[][] {kb}, stopKeys).setFilter(new ColumnPrefixFilter(index.qualifierHash(id, rdfFactory)));
	}
	final Scan scan(StatementIndex index, RDFIdentifier k, RDFFactory rdfFactory) {
		byte[] kb = k.getKeyHash(index);
		byte[][] stopKeys = index.newStopKeys(rdfFactory);
		stopKeys[0] = kb;
		return scan(index, new byte[][] {kb}, stopKeys).setFilter(new ColumnPrefixFilter(index.qualifier(k, null, null, null)));
	}
	final Scan scan(StatementIndex index, RDFIdentifier k1, RDFIdentifier k2, RDFFactory rdfFactory) {
		byte[] k1b = k1.getKeyHash(index);
		byte[] k2b = k2.getKeyHash(index);
		byte[][] stopKeys = index.newStopKeys(rdfFactory);
		stopKeys[0] = k1b;
		stopKeys[1] = k2b;
		return scan(index, new byte[][] {k1b, k2b}, stopKeys).setFilter(new ColumnPrefixFilter(index.qualifier(k1, k2, null, null)));
	}
	final Scan scan(StatementIndex index, byte[][] startKeys, byte[][] stopKeys) {
		return HalyardTableUtils.scan(index.concat(false, startKeys), index.concat(true, stopKeys));
	}
	abstract Scan scan(StatementIndex index, RDFIdentifier k1, RDFIdentifier k2, RDFIdentifier k3, RDFFactory rdfFactory);
	abstract Scan scan(StatementIndex index, RDFIdentifier k1, RDFIdentifier k2, RDFIdentifier k3, RDFIdentifier k4);
}
