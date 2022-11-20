package com.msd.gin.halyard.strategy;

import com.msd.gin.halyard.algebra.ServiceRoot;

import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HalyardEvaluationExecutorTest {
	private StatementPattern createStatementPattern(String s, String p, String o) {
		return new StatementPattern(new Var(s), new Var(p), new Var(o));
	}

	private StatementPattern createStatementPattern(int i) {
		return createStatementPattern("s"+i, "p"+i, "o"+i);
	}

	@Test
	public void testJoinPriority() {
		StatementPattern sp1 = createStatementPattern(1);
		StatementPattern sp2 = createStatementPattern(2);
		StatementPattern sp3 = createStatementPattern(3);
		Join j2 = new Join(sp2, sp3);
		Join j1 = new Join(sp1, j2);
		QueryRoot root = new QueryRoot(j1);
		HalyardEvaluationExecutor executor = new HalyardEvaluationExecutor(new Configuration());
		assertEquals(1, executor.getPriorityForNode(j1));
		assertEquals(2, executor.getPriorityForNode(sp1));
		assertEquals(6, executor.getPriorityForNode(j2));
		assertEquals(7, executor.getPriorityForNode(sp2));
		assertEquals(11, executor.getPriorityForNode(sp3));
	}

	@Test
	public void testServicePriority() {
		StatementPattern sp1 = createStatementPattern(1);
		StatementPattern sp2 = createStatementPattern(2);
		StatementPattern sp3 = createStatementPattern(3);
		StatementPattern sp4 = createStatementPattern(3);
		Join j3 = new Join(sp3, sp4);
		Service service = new Service(new Var("url"), j3, "?s ?p ?o", Collections.emptyMap(), null, false);
		Join j2 = new Join(sp2, service);
		Join j1 = new Join(sp1, j2);
		QueryRoot root = new QueryRoot(j1);
		HalyardEvaluationExecutor executor = new HalyardEvaluationExecutor(new Configuration());
		assertEquals(1, executor.getPriorityForNode(j1));
		assertEquals(2, executor.getPriorityForNode(sp1));
		assertEquals(6, executor.getPriorityForNode(j2));
		assertEquals(7, executor.getPriorityForNode(sp2));
		assertEquals(11, executor.getPriorityForNode(service));
		assertEquals(13, executor.getPriorityForNode(j3));
		assertEquals(14, executor.getPriorityForNode(sp3));
		assertEquals(18, executor.getPriorityForNode(sp4));

		ServiceRoot serviceRoot = ServiceRoot.create(service);
		Join serviceJoin = (Join) serviceRoot.getArg();
		assertEquals(13, executor.getPriorityForNode(serviceJoin));
		assertEquals(14, executor.getPriorityForNode(serviceJoin.getLeftArg()));
		assertEquals(18, executor.getPriorityForNode(serviceJoin.getRightArg()));
	}

	@Test
	public void testPrintQueryNode() {
		HalyardEvaluationExecutor.printQueryNode(createStatementPattern(1), EmptyBindingSet.getInstance());
	};

	@Test
	public void testThreadPool() {
		HalyardEvaluationExecutor executor = new HalyardEvaluationExecutor(new Configuration());
		int tasks = executor.getThreadPoolExecutor().getActiveCount();
		assertEquals(0, tasks);
		assertEquals(tasks, executor.getThreadPoolExecutor().getThreadDump().length);
		assertEquals(0, executor.getThreadPoolExecutor().getQueueDump().length);
	}
}
