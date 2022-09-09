package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.algebra.ServiceRoot;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.SilentIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.repository.sparql.federation.JoinExecutorBase;
import org.eclipse.rdf4j.repository.sparql.query.InsertBindingSetCursor;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;

public class SailFederatedService implements FederatedService {
	private final Sail sail;
	private boolean initialized;

	public SailFederatedService(Sail sail) {
		this.sail = sail;
	}

	public Sail getSail() {
		return sail;
	}

	@Override
	public void initialize() throws QueryEvaluationException {
		if (!initialized) {
			sail.init();
			initialized = true;
		}
	}

	@Override
	public boolean isInitialized() {
		return initialized;
	}

	@Override
	public void shutdown() throws QueryEvaluationException {
		sail.shutDown();
	}

	@Override
	public boolean ask(Service service, BindingSet bindings, String baseUri) throws QueryEvaluationException {
		try (SailConnection conn = sail.getConnection()) {
			try (CloseableIteration<? extends BindingSet, QueryEvaluationException> res = conn.evaluate(ServiceRoot.create(service), null, bindings, true)) {
				return res.hasNext();
			}
		}
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> select(Service service, Set<String> projectionVars,
			BindingSet bindings, String baseUri) throws QueryEvaluationException {
		try (SailConnection conn = sail.getConnection()) {
			CloseableIteration<? extends BindingSet, QueryEvaluationException> iter = conn.evaluate(ServiceRoot.create(service), null, bindings, true);
			CloseableIteration<BindingSet, QueryEvaluationException> result = new InsertBindingSetCursor((CloseableIteration<BindingSet, QueryEvaluationException>) iter, bindings);
			if (service.isSilent()) {
				return new SilentIteration<>(result);
			} else {
				return result;
			}
		}
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Service service,
			CloseableIteration<BindingSet, QueryEvaluationException> bindings, String baseUri)
			throws QueryEvaluationException {
		List<BindingSet> allBindings = new ArrayList<>();
		while (bindings.hasNext()) {
			allBindings.add(bindings.next());
		}

		if (allBindings.isEmpty()) {
			return new EmptyIteration<>();
		}

		CloseableIteration<BindingSet, QueryEvaluationException> result = new SimpleServiceIteration(service, allBindings, baseUri);

		if (service.isSilent()) {
			return new SilentIteration<>(result);
		} else {
			return result;
		}
	}


	private final class SimpleServiceIteration extends JoinExecutorBase<BindingSet> {

		private final Service service;
		private final List<BindingSet> allBindings;
		private final String baseUri;

		public SimpleServiceIteration(Service service, List<BindingSet> allBindings, String baseUri) {
			super(null, null, null);
			this.service = service;
			this.allBindings = allBindings;
			this.baseUri = baseUri;
			run();
		}

		@Override
		protected void handleBindings() throws Exception {
			Set<String> projectionVars = new HashSet<>(service.getServiceVars());
			for (BindingSet b : allBindings) {
				addResult(select(service, projectionVars, b, baseUri));
			}
		}
	}
}
