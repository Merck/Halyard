package com.msd.gin.halyard.function;

import com.msd.gin.halyard.common.IdentifiableValueIO;
import com.msd.gin.halyard.common.Timestamped;
import com.msd.gin.halyard.common.TimestampedValueFactory;
import com.msd.gin.halyard.sail.HBaseTripleSource;

import java.util.List;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.SingletonIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.junit.Test;

import static org.junit.Assert.*;

public class TimestampTupleFunctionTest {
	private static final IdentifiableValueIO valueIO = IdentifiableValueIO.create();

	@Test
	public void testTimestampedStatements() {
		long ts = System.currentTimeMillis();
		ValueFactory SVF = SimpleValueFactory.getInstance();
		Resource subj = SVF.createBNode();
		IRI pred = SVF.createIRI(":prop");
		Value obj = SVF.createBNode();
		TripleSource tripleSource = new HBaseTripleSource(null, SVF, valueIO, 0) {
			TimestampedValueFactory TVF = new TimestampedValueFactory(valueIO);

			@Override
			public CloseableIteration<? extends Statement, QueryEvaluationException> getTimestampedStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
				Statement stmt = TVF.createStatement(subj, pred, obj);
				((Timestamped) stmt).setTimestamp(ts);
				return new SingletonIteration<Statement, QueryEvaluationException>(stmt);
			}
		};
		CloseableIteration<? extends List<? extends Value>, QueryEvaluationException> iter = new TimestampTupleFunction().evaluate(tripleSource, SVF, subj, pred, obj);
		assertTrue(iter.hasNext());
		List<? extends Value> bindings = iter.next();
		assertEquals(1, bindings.size());
		assertEquals(ts, ((Literal) bindings.get(0)).calendarValue().toGregorianCalendar().getTimeInMillis());
		assertFalse(iter.hasNext());
		iter.close();
	}

	@Test
	public void testTripleTimestamp() {
		long ts = System.currentTimeMillis();
		ValueFactory SVF = SimpleValueFactory.getInstance();
		Resource subj = SVF.createBNode();
		IRI pred = SVF.createIRI(":prop");
		Value obj = SVF.createBNode();
		TripleSource tripleSource = new HBaseTripleSource(null, SVF, valueIO, 0) {
			TimestampedValueFactory TVF = new TimestampedValueFactory(valueIO);

			@Override
			public CloseableIteration<? extends Statement, QueryEvaluationException> getTimestampedStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
				Statement stmt = TVF.createStatement(subj, pred, obj);
				((Timestamped) stmt).setTimestamp(ts);
				return new SingletonIteration<Statement, QueryEvaluationException>(stmt);
			}
		};
		CloseableIteration<? extends List<? extends Value>, QueryEvaluationException> iter = new TimestampTupleFunction().evaluate(tripleSource, SVF, SVF.createTriple(subj, pred, obj));
		assertTrue(iter.hasNext());
		List<? extends Value> bindings = iter.next();
		assertEquals(1, bindings.size());
		assertEquals(ts, ((Literal) bindings.get(0)).calendarValue().toGregorianCalendar().getTimeInMillis());
		assertFalse(iter.hasNext());
		iter.close();
	}

	@Test(expected = ValueExprEvaluationException.class)
	public void testIncorrectArgs() {
		SimpleValueFactory SVF = SimpleValueFactory.getInstance();
		new TimestampTupleFunction().evaluate(null, SVF, SVF.createBNode());
	}
}
