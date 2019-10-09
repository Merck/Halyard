package com.msd.gin.halyard.repository;

import com.msd.gin.halyard.sail.HBaseSail;

import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.parser.ParsedUpdate;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.SailConnection;

public class HBaseRepositoryConnection extends SailRepositoryConnection {
	private final HBaseSail sail;

	protected HBaseRepositoryConnection(HBaseRepository repository, SailConnection sailConnection) {
		super(repository, sailConnection);
		this.sail = (HBaseSail) repository.getSail();
	}

	@Override
	public Update prepareUpdate(QueryLanguage ql, String update, String baseURI) throws RepositoryException, MalformedQueryException {
		ParsedUpdate parsedUpdate = QueryParserUtil.parseUpdate(ql, update, baseURI);

		return new HBaseUpdate(parsedUpdate, sail, this);
	}
}
