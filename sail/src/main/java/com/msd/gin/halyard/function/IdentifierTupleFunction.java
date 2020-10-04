package com.msd.gin.halyard.function;

import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.vocab.HALYARD;

import java.util.Collections;
import java.util.List;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.SingletonIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;
import org.kohsuke.MetaInfServices;

@MetaInfServices(TupleFunction.class)
public class IdentifierTupleFunction implements TupleFunction {

	@Override
	public String getURI() {
		return HALYARD.IDENTIFIER_PROPERTY.toString();
	}

	@Override
	public CloseableIteration<? extends List<? extends Value>, QueryEvaluationException> evaluate(ValueFactory vf,
			Value... args)
		throws ValueExprEvaluationException
	{
		Namespace ns;
		byte[] id;
		if (args.length == 1) {
			ns = HALYARD.VALUE_ID_NS;
			id = HalyardTableUtils.id(args[0]);
		} else if (args.length == 3) {
			if (!(args[0] instanceof Resource)) {
				throw new ValueExprEvaluationException("First argument must be a subject");
			}
			if (!(args[1] instanceof IRI)) {
				throw new ValueExprEvaluationException("Second argument must be a predicate");
			}
			if (!(args[2] instanceof Value)) {
				throw new ValueExprEvaluationException("Third argument must be an object");
			}
			ns = HALYARD.TRIPLE_ID_NS;
			id = new byte[3 * HalyardTableUtils.ID_SIZE];
			HalyardTableUtils.writeTripleIdentifier((Resource) args[0], (IRI) args[1], args[2], id, 0);
		} else {
			throw new ValueExprEvaluationException(String.format("%s requires 1 or 3 arguments, got %d", getURI(), args.length));
		}

		return new SingletonIteration<>(Collections.singletonList(vf.createIRI(ns.getName(), HalyardTableUtils.encode(id))));
	}
}
