package com.msd.gin.halyard.function;

import static com.msd.gin.halyard.vocab.HALYARD.TIMESTAMP_PROPERTY;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.SingletonIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryPreparer;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;
import org.eclipse.rdf4j.spin.function.AbstractSpinFunction;
import org.kohsuke.MetaInfServices;

import com.msd.gin.halyard.common.StatementValue;
import com.msd.gin.halyard.common.Timestamped;

@MetaInfServices(TupleFunction.class)
public class TimestampTupleFunction extends AbstractSpinFunction implements TupleFunction {

	public TimestampTupleFunction() {
		super(TIMESTAMP_PROPERTY.toString());
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

		Statement stmtPattern;
		if (args.length == 3) {
			stmtPattern = vf.createStatement((Resource)args[0], (IRI)args[1], (Value)args[2]);
		} else {
			stmtPattern = vf.createStatement((Resource)args[0], (IRI)args[1], (Value)args[3], (Resource)args[4]);
		}

		// check to see if we have the actual statement at hand
		for (Value arg : args) {
			if (arg instanceof StatementValue) {
				Statement stmt = ((StatementValue) arg).getStatement();
				if (stmt.equals(stmtPattern) && stmt instanceof Timestamped) {
					long ts = ((Timestamped) stmt).getTimestamp();
					return new SingletonIteration<>(Collections.singletonList(vf.createLiteral(new Date(ts))));
				}
			}
		}

		// fall-back to retrieving the actual statement
		QueryPreparer qp = getCurrentQueryPreparer();
		Resource[] contexts = (stmtPattern.getContext() != null) ? new Resource[] { stmtPattern.getContext() } : new Resource[0];
		try (CloseableIteration<? extends Statement, QueryEvaluationException> iter = qp.getTripleSource().getStatements(stmtPattern.getSubject(), stmtPattern.getPredicate(), stmtPattern.getObject(), contexts)) {
			if(iter.hasNext()) {
				Statement stmt = iter.next();
				if (stmt instanceof Timestamped) {
					long ts = ((Timestamped) stmt).getTimestamp();
					return new SingletonIteration<>(Collections.singletonList(vf.createLiteral(new Date(ts))));
				}
			}
		}

		return new EmptyIteration<>();
	}
}
