package com.msd.gin.halyard.optimizers;

import com.msd.gin.halyard.algebra.NestedLoops;

import org.eclipse.rdf4j.model.impl.BooleanLiteral;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleFunctionCall;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class JoinAlgorithmOptimizerTest {
	@Test
	public void testTupleFunction() {
        new JoinAlgorithmOptimizer().optimize(new TupleFunctionCall(), null, null);
	}

	@Test
	public void testSimpleJoinWithTupleFunction() {
		Join join = new Join(new StatementPattern(), new TupleFunctionCall());
        new JoinAlgorithmOptimizer().optimize(join, null, null);
        assertEquals(NestedLoops.NAME, join.getAlgorithmName());
	}

	@Test
	public void testMultiJoinWithTupleFunction() {
		Join join = new Join(new StatementPattern(), new Filter(new TupleFunctionCall(), new ValueConstant(BooleanLiteral.TRUE)));
		Join topJoin = new Join(new StatementPattern(), join);
        new JoinAlgorithmOptimizer().optimize(topJoin, null, null);
        assertNull(topJoin.getAlgorithmName());
        assertEquals(NestedLoops.NAME, join.getAlgorithmName());
	}
}
