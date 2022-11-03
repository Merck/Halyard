package com.msd.gin.halyard.query;

import java.util.List;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryResultHandlerException;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;

public abstract class TupleQueryResultHandlerWrapper implements TupleQueryResultHandler {
	private final TupleQueryResultHandler delegate;

	public TupleQueryResultHandlerWrapper(TupleQueryResultHandler handler) {
		this.delegate = handler;
	}

	@Override
	public void startQueryResult(List<String> bindingNames) throws TupleQueryResultHandlerException {
		delegate.startQueryResult(bindingNames);
	}

	@Override
	public void handleBoolean(boolean value) throws QueryResultHandlerException {
		delegate.handleBoolean(value);
	}

	@Override
	public void handleLinks(List<String> linkUrls) throws QueryResultHandlerException {
		delegate.handleLinks(linkUrls);
	}

	@Override
	public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
		delegate.handleSolution(bindingSet);
	}

	@Override
	public void endQueryResult() throws TupleQueryResultHandlerException {
		delegate.endQueryResult();
	}
}
