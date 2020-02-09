package com.msd.gin.halyard.repository;

import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.sail.HBaseSailConnection;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.Operation;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.impl.AbstractParserQuery;
import org.eclipse.rdf4j.query.impl.AbstractParserUpdate;
import org.eclipse.rdf4j.query.parser.ParsedUpdate;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailBooleanQuery;
import org.eclipse.rdf4j.repository.sail.SailGraphQuery;
import org.eclipse.rdf4j.repository.sail.SailQuery;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailTupleQuery;
import org.eclipse.rdf4j.repository.sail.SailUpdate;
import org.eclipse.rdf4j.sail.SailConnection;

public class HBaseRepositoryConnection extends SailRepositoryConnection {
	private final HBaseSail sail;

	protected HBaseRepositoryConnection(HBaseRepository repository, SailConnection sailConnection) {
		super(repository, sailConnection);
		this.sail = (HBaseSail) repository.getSail();
	}

	private void addImplicitBindings(Operation op) {
		ValueFactory vf = getValueFactory();
		String sourceString;
		if (op instanceof AbstractParserQuery) {
			sourceString = ((AbstractParserQuery) op).getParsedQuery().getSourceString();
		} else if (op instanceof AbstractParserUpdate) {
			sourceString = ((AbstractParserUpdate) op).getParsedUpdate().getSourceString();
		} else {
			sourceString = null;
		}
		if (sourceString != null) {
			op.setBinding(HBaseSailConnection.SOURCE_STRING_BINDING, vf.createLiteral(sourceString));
		}
	}

	@Override
	public SailQuery prepareQuery(QueryLanguage ql, String queryString, String baseURI) throws MalformedQueryException {
		SailQuery query = super.prepareQuery(ql, queryString, baseURI);
		addImplicitBindings(query);
		return query;
	}

	@Override
	public SailTupleQuery prepareTupleQuery(QueryLanguage ql, String queryString, String baseURI) throws MalformedQueryException {
		SailTupleQuery query = super.prepareTupleQuery(ql, queryString, baseURI);
		addImplicitBindings(query);
		return query;
	}

	@Override
	public SailGraphQuery prepareGraphQuery(QueryLanguage ql, String queryString, String baseURI) throws MalformedQueryException {
		SailGraphQuery query = super.prepareGraphQuery(ql, queryString, baseURI);
		addImplicitBindings(query);
		return query;
	}

	@Override
	public SailBooleanQuery prepareBooleanQuery(QueryLanguage ql, String queryString, String baseURI) throws MalformedQueryException {
		SailBooleanQuery query = super.prepareBooleanQuery(ql, queryString, baseURI);
		addImplicitBindings(query);
		return query;
	}

	@Override
	public Update prepareUpdate(QueryLanguage ql, String updateQuery, String baseURI) throws RepositoryException, MalformedQueryException {
		ParsedUpdate parsedUpdate = QueryParserUtil.parseUpdate(ql, updateQuery, baseURI);

		SailUpdate update = new HBaseUpdate(parsedUpdate, sail, this);
		addImplicitBindings(update);
		return update;
	}
}
