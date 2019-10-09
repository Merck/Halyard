package com.msd.gin.halyard.repository;

import com.msd.gin.halyard.sail.HBaseSail;

import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.SailException;

public class HBaseRepository extends SailRepository {

	public HBaseRepository(HBaseSail sail) {
		super(sail);
	}

	@Override
	public SailRepositoryConnection getConnection() throws RepositoryException {
		try {
			return new HBaseRepositoryConnection(this, getSail().getConnection());
		} catch (SailException e) {
			throw new RepositoryException(e);
		}
	}

}
