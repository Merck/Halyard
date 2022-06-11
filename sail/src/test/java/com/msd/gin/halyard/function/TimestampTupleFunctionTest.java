package com.msd.gin.halyard.function;

import com.msd.gin.halyard.common.RDFFactory;
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
	private static final RDFFactory rdfFactory = RDFFactory.create();

	private TripleSource getStubTripleSource(long ts) {
		return new HBaseTripleSource(null, SimpleValueFactory.getInstance(), rdfFactory, 0) {
			@Override
			public TripleSource getTimestampedTripleSource() {
				return new TripleSource() {
					@Override
					public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
						Statement stmt = getValueFactory().createStatement(subj, pred, obj);
						((Timestamped) stmt).setTimestamp(ts);
						return new SingletonIteration<Statement, QueryEvaluationException>(stmt);
					}

					@Override
					public ValueFactory getValueFactory() {
						return TimestampedValueFactory.INSTANCE;
					}
				};
			}
		};
	}

	@Test
	public void testTimestampedStatements() {
		long ts = System.currentTimeMillis();
		TripleSource tripleSource = getStubTripleSource(ts);
		ValueFactory vf = tripleSource.getValueFactory();
		Resource subj = vf.createBNode();
		IRI pred = vf.createIRI(":prop");
		Value obj = vf.createBNode();
		CloseableIteration<? extends List<? extends Value>, QueryEvaluationException> iter = new TimestampTupleFunction().evaluate(tripleSource, vf, subj, pred, obj);
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
		TripleSource tripleSource = getStubTripleSource(ts);
		ValueFactory vf = tripleSource.getValueFactory();
		Resource subj = vf.createBNode();
		IRI pred = vf.createIRI(":prop");
		Value obj = vf.createBNode();
		CloseableIteration<? extends List<? extends Value>, QueryEvaluationException> iter = new TimestampTupleFunction().evaluate(tripleSource, vf, vf.createTriple(subj, pred, obj));
		assertTrue(iter.hasNext());
		List<? extends Value> bindings = iter.next();
		assertEquals(1, bindings.size());
		assertEquals(ts, ((Literal) bindings.get(0)).calendarValue().toGregorianCalendar().getTimeInMillis());
		assertFalse(iter.hasNext());
		iter.close();
	}

	@Test(expected = ValueExprEvaluationException.class)
	public void testIncorrectNumberOfArgs() {
		TripleSource ts = null;
		SimpleValueFactory SVF = SimpleValueFactory.getInstance();
		new TimestampTupleFunction().evaluate(ts, SVF, SVF.createBNode());
	}

	@Test(expected = ValueExprEvaluationException.class)
	public void testIncorrectNumberArgType1() {
		TripleSource ts = null;
		SimpleValueFactory SVF = SimpleValueFactory.getInstance();
		new TimestampTupleFunction().evaluate(ts, SVF, SVF.createLiteral("foo"), null, null);
	}

	@Test(expected = ValueExprEvaluationException.class)
	public void testIncorrectNumberArgType2() {
		TripleSource ts = null;
		SimpleValueFactory SVF = SimpleValueFactory.getInstance();
		new TimestampTupleFunction().evaluate(ts, SVF, SVF.createBNode(), SVF.createLiteral("foo"), null);
	}
}
