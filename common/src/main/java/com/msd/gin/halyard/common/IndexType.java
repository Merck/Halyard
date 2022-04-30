package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;

enum IndexType {
	TRIPLE {
		@Override
		int get3KeyHashSize(RDFRole<?> role) {
			return role.endKeyHashSize();
		}
		@Override
		<T extends SPOC<?>> byte[] get3KeyHash(StatementIndex<?,?,T,?> index, RDFIdentifier<T> k) {
			return k.getEndKeyHash(index);
		}
		@Override
		int get3QualifierHashSize(RDFRole<?> role) {
			return role.endQualifierHashSize();
		}
		@Override
		void write3QualifierHashTo(RDFIdentifier<?> k, ByteBuffer b) {
			k.writeEndQualifierHashTo(b);
		}
		@Override
		byte[][] newStopKeys(RDFRole<?>[] roles) {
    		return new byte[][] {roles[0].stopKey(), roles[1].stopKey(), roles[2].endStopKey()};
		}
	},

	QUAD {
		@Override
		int get3KeyHashSize(RDFRole<?> role) {
			return role.keyHashSize();
		}
		@Override
		<T extends SPOC<?>> byte[] get3KeyHash(StatementIndex<?,?,T,?> index, RDFIdentifier<T> k) {
			return k.getKeyHash(index);
		}
		@Override
		int get3QualifierHashSize(RDFRole<?> role) {
			return role.qualifierHashSize();
		}
		@Override
		void write3QualifierHashTo(RDFIdentifier<?> k, ByteBuffer b) {
			k.writeQualifierHashTo(b);
		}
		@Override
		byte[][] newStopKeys(RDFRole<?>[] roles) {
    		return new byte[][] {roles[0].stopKey(), roles[1].stopKey(), roles[2].stopKey(), roles[3].endStopKey()};
		}
	};

	abstract int get3KeyHashSize(RDFRole<?> role);
	final int get4KeyHashSize(RDFRole<?> role) {
		return role.endKeyHashSize();
	}
	abstract <T extends SPOC<?>> byte[] get3KeyHash(StatementIndex<?,?,T,?> index, RDFIdentifier<T> k);
	final <T extends SPOC<?>> byte[] get4KeyHash(StatementIndex<?,?,?,T> index, RDFIdentifier<T> k) {
		return k.getEndKeyHash(index);
	}
	abstract int get3QualifierHashSize(RDFRole<?> role);
	final int get4QualifierHashSize(RDFRole<?> role) {
		return role.endQualifierHashSize();
	}
	abstract void write3QualifierHashTo(RDFIdentifier<?> k, ByteBuffer b);
	final void write4QualifierHashTo(RDFIdentifier<?> k, ByteBuffer b) {
		k.writeEndQualifierHashTo(b);
	}
	abstract byte[][] newStopKeys(RDFRole<?>[] roles);
}
