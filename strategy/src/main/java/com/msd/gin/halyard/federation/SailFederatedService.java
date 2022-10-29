package com.msd.gin.halyard.federation;

import com.msd.gin.halyard.algebra.ServiceRoot;
import com.msd.gin.halyard.sail.ExtendedSailConnection;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.SilentIteration;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryResultHandlerException;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.repository.sparql.federation.JoinExecutorBase;
import org.eclipse.rdf4j.repository.sparql.query.InsertBindingSetCursor;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;

public class SailFederatedService implements ExtendedFederatedService {
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
	public void select(TupleQueryResultHandler handler, Service service, Set<String> projectionVars, BindingSet bindings, String baseUri) throws QueryEvaluationException {
		try (SailConnection conn = sail.getConnection()) {
			if (conn instanceof ExtendedSailConnection) {
				((ExtendedSailConnection) conn).evaluate(new InsertBindingSetTupleQueryResultHandler(handler, bindings), ServiceRoot.create(service), null, bindings, true);
			} else {
				CloseableIteration<? extends BindingSet, QueryEvaluationException> iter = conn.evaluate(ServiceRoot.create(service), null, bindings, true);
				CloseableIteration<BindingSet, QueryEvaluationException> result = new InsertBindingSetCursor((CloseableIteration<BindingSet, QueryEvaluationException>) iter, bindings);
				if (service.isSilent()) {
					result = new SilentIteration<>(result);
				}
				Set<String> bindingNames = new HashSet<>(service.getBindingNames());
				bindingNames.addAll(bindings.getBindingNames());
				ExtendedSailConnection.report(bindingNames, result, handler);
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


	private static class InsertBindingSetTupleQueryResultHandler implements TupleQueryResultHandler {
		private final TupleQueryResultHandler delegate;
		private final BindingSet bindingSet;

		InsertBindingSetTupleQueryResultHandler(TupleQueryResultHandler delegate, BindingSet bs) {
			this.delegate = delegate;
			this.bindingSet = bs;
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
		public void startQueryResult(List<String> bindingNames) throws TupleQueryResultHandlerException {
			// preserve order!!!
			LinkedHashSet<String> allBindingNames = new LinkedHashSet<>(bindingNames);
			allBindingNames.addAll(bindingSet.getBindingNames());
			delegate.startQueryResult(new ArrayList<>(allBindingNames));
		}

		@Override
		public void handleSolution(BindingSet next) throws TupleQueryResultHandlerException {
			int size = bindingSet.size() + next.size();
			QueryBindingSet set = new QueryBindingSet(size);
			set.addAll(bindingSet);
			for (Binding binding : next) {
				set.setBinding(binding);
			}
			delegate.handleSolution(set);
		}

		@Override
		public void endQueryResult() throws TupleQueryResultHandlerException {
			delegate.endQueryResult();
		}
	}
}
