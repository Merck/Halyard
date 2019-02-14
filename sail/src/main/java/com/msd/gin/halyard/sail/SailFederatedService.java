package com.msd.gin.halyard.sail;

import java.util.Set;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.repository.sparql.query.InsertBindingSetCursor;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;

import com.msd.gin.halyard.strategy.HalyardEvaluationStrategy.ServiceRoot;

public class SailFederatedService implements FederatedService {
	private final Sail sail;
	private boolean initialized;
	private SailConnection conn;

	public SailFederatedService(Sail sail) {
		this.sail = sail;
	}

	public Sail getSail() {
		return sail;
	}

	@Override
	public void initialize() throws QueryEvaluationException {
		if (!initialized) {
			sail.initialize();
			initialized = true;
		}
	}

	@Override
	public boolean isInitialized() {
		return initialized;
	}

	@Override
	public void shutdown() throws QueryEvaluationException {
		if (conn != null) {
			conn.close();
			conn = null;
		}
		sail.shutDown();
	}

	@Override
	public boolean ask(Service service, BindingSet bindings, String baseUri) throws QueryEvaluationException {
		try (CloseableIteration<? extends BindingSet, QueryEvaluationException> res = getConnection()
				.evaluate(new ServiceRoot(service.getArg()), null, bindings, true)) {
			return res.hasNext();
		}
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> select(Service service, Set<String> projectionVars,
			BindingSet bindings, String baseUri) throws QueryEvaluationException {
		CloseableIteration<? extends BindingSet, QueryEvaluationException> iter = getConnection().evaluate(new ServiceRoot(service.getArg()), null, bindings, true);
		return new InsertBindingSetCursor((CloseableIteration<BindingSet, QueryEvaluationException>) iter, bindings);
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Service service,
			CloseableIteration<BindingSet, QueryEvaluationException> bindings, String baseUri)
			throws QueryEvaluationException {
		throw new UnsupportedOperationException();
	}

	protected SailConnection getConnection() {
		if (conn == null) {
			conn = sail.getConnection();
		}
		return conn;
	}
}
