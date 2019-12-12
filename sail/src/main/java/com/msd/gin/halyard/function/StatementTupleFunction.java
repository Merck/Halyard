package com.msd.gin.halyard.function;

import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.vocab.HALYARD;

import java.util.Collections;
import java.util.List;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.SingletonIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;
import org.kohsuke.MetaInfServices;

@MetaInfServices(TupleFunction.class)
public class StatementTupleFunction implements TupleFunction {

	@Override
	public String getURI() {
		return HALYARD.STATEMENT_PROPERTY.toString();
	}

	@Override
	public CloseableIteration<? extends List<? extends Value>, QueryEvaluationException> evaluate(ValueFactory vf,
			Value... args)
		throws ValueExprEvaluationException
	{
		if (args.length < 3 || args.length > 4) {
			throw new ValueExprEvaluationException(String.format("%s requires 3 or 4 arguments, got %d", getURI(), args.length));
		}

		if (!(args[0] instanceof Resource)) {
			throw new ValueExprEvaluationException("First argument must be a subject");
		}
		if (!(args[1] instanceof IRI)) {
			throw new ValueExprEvaluationException("Second argument must be a predicate");
		}
		if (!(args[2] instanceof Value)) {
			throw new ValueExprEvaluationException("Third argument must be an object");
		}
		if (args.length == 4 && !(args[3] instanceof Resource)) {
			throw new ValueExprEvaluationException("Fourth argument must be a context");
		}

		byte[] stmtId = new byte[args.length* HalyardTableUtils.ID_SIZE];
		for (int i = 0; i < args.length; i++) {
			byte[] id = HalyardTableUtils.id(args[i]);
			System.arraycopy(id, 0, stmtId, i * HalyardTableUtils.ID_SIZE, HalyardTableUtils.ID_SIZE);
		}
		return new SingletonIteration<>(Collections.singletonList(vf.createIRI(HALYARD.STATEMENT_NS.getName(), HalyardTableUtils.encode(stmtId))));
	}
}
