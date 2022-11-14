package com.msd.gin.halyard.federation;

import com.msd.gin.halyard.query.BindingSetPipe;

import java.util.Set;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;

public interface BindingSetPipeFederatedService extends FederatedService {
	void select(BindingSetPipe handler, Service service, Set<String> projectionVars,
			BindingSet bindings, String baseUri) throws QueryEvaluationException;

}
