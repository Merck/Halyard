package com.msd.gin.halyard.function;

import com.msd.gin.halyard.common.Timestamped;
import com.msd.gin.halyard.common.TimestampedValueFactory;

import java.util.List;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.junit.Test;

import static org.junit.Assert.*;

public class TimestampTupleFunctionTest {

	@Test
	public void testTimestampedStatements() {
		long ts = System.currentTimeMillis();
		TimestampedValueFactory TVF = TimestampedValueFactory.getInstance();
		Statement stmt = TVF.createStatement(TVF.createBNode(), TVF.createIRI(":prop"), TVF.createBNode());
		((Timestamped) stmt).setTimestamp(ts);
		CloseableIteration<? extends List<? extends Value>, QueryEvaluationException> iter = new TimestampTupleFunction().evaluate(TVF, stmt.getSubject(), stmt.getPredicate(), stmt.getObject());
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
		new TimestampTupleFunction().evaluate(SVF, SVF.createBNode());
	}
}
