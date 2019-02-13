package com.msd.gin.halyard.lubm;

public class HalyardRepositoryFactory extends edu.lehigh.swat.bench.ubt.api.RepositoryFactory {

	public edu.lehigh.swat.bench.ubt.api.Repository create() {
		return new HalyardRepository();
	}
}
