package com.msd.gin.halyard.repository;

import com.msd.gin.halyard.common.TimeLimitTupleQueryResultHandler;
import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.sail.HBaseSailConnection;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.Operation;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.impl.AbstractParserQuery;
import org.eclipse.rdf4j.query.impl.AbstractParserUpdate;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.ParsedUpdate;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailBooleanQuery;
import org.eclipse.rdf4j.repository.sail.SailGraphQuery;
import org.eclipse.rdf4j.repository.sail.SailQuery;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailTupleQuery;
import org.eclipse.rdf4j.repository.sail.SailUpdate;
import org.eclipse.rdf4j.sail.SailException;

public class HBaseRepositoryConnection extends SailRepositoryConnection {
	private final HBaseSail sail;

	protected HBaseRepositoryConnection(HBaseRepository repository, HBaseSailConnection sailConnection) {
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
		Optional<TupleExpr> sailTupleExpr = getSailConnection().prepareQuery(ql, Query.QueryType.TUPLE, queryString, baseURI);

		ParsedTupleQuery parsedQuery = sailTupleExpr.map(expr -> new ParsedTupleQuery(queryString, expr)).orElse(QueryParserUtil.parseTupleQuery(ql, queryString, baseURI));
		SailTupleQuery query = new SailTupleQuery(parsedQuery, this) {
			@Override
			public void evaluate(TupleQueryResultHandler handler) throws QueryEvaluationException, TupleQueryResultHandlerException {
				TupleExpr tupleExpr = getParsedQuery().getTupleExpr();
				try {
					HBaseSailConnection sailCon = (HBaseSailConnection) getConnection().getSailConnection();
					handler = HBaseRepositoryConnection.enforceMaxQueryTime(handler, getMaxExecutionTime());
					sailCon.evaluate(handler, tupleExpr, getActiveDataset(), getBindings(), getIncludeInferred());
				} catch (SailException e) {
					throw new QueryEvaluationException(e.getMessage(), e);
				}
			}
		};
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

	private static TupleQueryResultHandler enforceMaxQueryTime(TupleQueryResultHandler handler, int maxTimeSecs) {
		if (maxTimeSecs > 0) {
			handler = new TimeLimitTupleQueryResultHandler(handler, TimeUnit.SECONDS.toMillis(maxTimeSecs)) {
				@Override
				protected void throwInterruptedException() {
					throw new TupleQueryResultHandlerException("Query evaluation took too long");
				}
			};
		}
		return handler;
	}
}
