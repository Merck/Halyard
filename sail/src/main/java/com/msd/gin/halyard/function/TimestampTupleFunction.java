package com.msd.gin.halyard.function;

import static com.msd.gin.halyard.vocab.HALYARD.*;

import com.msd.gin.halyard.common.Timestamped;
import com.msd.gin.halyard.sail.HBaseTripleSource;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.ConvertingIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryPreparer;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;
import org.eclipse.rdf4j.spin.function.AbstractSpinFunction;
import org.kohsuke.MetaInfServices;

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
		QueryPreparer qp = getCurrentQueryPreparer();
		return evaluate(qp.getTripleSource(), vf, args);
	}

	public CloseableIteration<? extends List<? extends Value>, QueryEvaluationException> evaluate(TripleSource tripleSource, ValueFactory vf, Value... args) throws ValueExprEvaluationException {
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

		Resource[] contexts = (args.length == 4) ? new Resource[] { (Resource) args[3] } : new Resource[0];
		CloseableIteration<? extends Statement, QueryEvaluationException> iter = ((HBaseTripleSource) tripleSource).getTimestampedStatements((Resource) args[0], (IRI) args[1], args[2], contexts);
		return new ConvertingIteration<Statement, List<? extends Value>, QueryEvaluationException>(iter) {
			@Override
			protected List<? extends Value> convert(Statement stmt) throws QueryEvaluationException {
				long ts = ((Timestamped) stmt).getTimestamp();
				return Collections.singletonList(vf.createLiteral(new Date(ts)));
			}
		};
	}
}
