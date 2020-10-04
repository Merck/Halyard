package com.msd.gin.halyard.function;

import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.junit.Test;

public class TripleTupleFunctionTest {
	private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

	@Test(expected = ValueExprEvaluationException.class)
	public void testIncorrectArgs() {
		new TripleTupleFunction().evaluate(SVF, SVF.createBNode());
	}
}
