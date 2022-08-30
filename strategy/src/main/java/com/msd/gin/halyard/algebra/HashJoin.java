package com.msd.gin.halyard.algebra;

public final class HashJoin extends Algorithm {
	public static final HashJoin INSTANCE = new HashJoin();
	public static final String NAME = HashJoin.class.getSimpleName();

	private HashJoin() {}
}
