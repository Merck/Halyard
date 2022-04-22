package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;

enum IndexType {
	TRIPLE {
		@Override
		int get3KeyHashSize(RDFRole<?> role) {
			return role.endKeyHashSize();
		}
		@Override
		int get4KeyHashSize(RDFRole<?> role) {
			return role.keyHashSize();
		}
		@Override
		<T extends SPOC<?>> byte[] get3KeyHash(StatementIndex<?,?,T,?> index, RDFIdentifier<T> k) {
			return k.getEndKeyHash(index);
		}
		@Override
		<T extends SPOC<?>> byte[] get4KeyHash(StatementIndex<?,?,?,T> index, RDFIdentifier<T> k) {
			return k.getKeyHash(index);
		}
		@Override
		int get3QualifierHashSize(RDFRole<?> role) {
			return role.endQualifierHashSize();
		}
		@Override
		int get4QualifierHashSize(RDFRole<?> role) {
			return role.qualifierHashSize();
		}
		@Override
		void write3QualifierHashTo(RDFIdentifier<?> k, ByteBuffer b) {
			k.writeEndQualifierHashTo(b);
		}
		@Override
		void write4QualifierHashTo(RDFIdentifier<?> k, ByteBuffer b) {
			k.writeQualifierHashTo(b);
		}
		@Override
		byte[][] newStopKeys(RDFRole<?>[] roles) {
    		return new byte[][] {roles[0].stopKey(), roles[1].stopKey(), roles[2].endStopKey(), roles[3].stopKey()};
		}
	},

	QUAD {
		@Override
		int get3KeyHashSize(RDFRole<?> role) {
			return role.keyHashSize();
		}
		@Override
		int get4KeyHashSize(RDFRole<?> role) {
			return role.endKeyHashSize();
		}
		@Override
		<T extends SPOC<?>> byte[] get3KeyHash(StatementIndex<?,?,T,?> index, RDFIdentifier<T> k) {
			return k.getKeyHash(index);
		}
		@Override
		<T extends SPOC<?>> byte[] get4KeyHash(StatementIndex<?,?,?,T> index, RDFIdentifier<T> k) {
			return k.getEndKeyHash(index);
		}
		@Override
		int get3QualifierHashSize(RDFRole<?> role) {
			return role.qualifierHashSize();
		}
		@Override
		int get4QualifierHashSize(RDFRole<?> role) {
			return role.endQualifierHashSize();
		}
		@Override
		void write3QualifierHashTo(RDFIdentifier<?> k, ByteBuffer b) {
			k.writeQualifierHashTo(b);
		}
		@Override
		void write4QualifierHashTo(RDFIdentifier<?> k, ByteBuffer b) {
			k.writeEndQualifierHashTo(b);
		}
		@Override
		byte[][] newStopKeys(RDFRole<?>[] roles) {
    		return new byte[][] {roles[0].stopKey(), roles[1].stopKey(), roles[2].stopKey(), roles[3].endStopKey()};
		}
	};

	abstract int get3KeyHashSize(RDFRole<?> role);
	abstract int get4KeyHashSize(RDFRole<?> role);
	abstract <T extends SPOC<?>> byte[] get3KeyHash(StatementIndex<?,?,T,?> index, RDFIdentifier<T> k);
	abstract <T extends SPOC<?>> byte[] get4KeyHash(StatementIndex<?,?,?,T> index, RDFIdentifier<T> k);
	abstract int get3QualifierHashSize(RDFRole<?> role);
	abstract int get4QualifierHashSize(RDFRole<?> role);
	abstract void write3QualifierHashTo(RDFIdentifier<?> k, ByteBuffer b);
	abstract void write4QualifierHashTo(RDFIdentifier<?> k, ByteBuffer b);
	abstract byte[][] newStopKeys(RDFRole<?>[] roles);
}
