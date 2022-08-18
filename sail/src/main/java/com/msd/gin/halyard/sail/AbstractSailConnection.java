package com.msd.gin.halyard.sail;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.helpers.QueryModelTreeToGenericPlanNode;
import org.eclipse.rdf4j.query.explanation.Explanation;
import org.eclipse.rdf4j.query.explanation.ExplanationImpl;
import org.eclipse.rdf4j.sail.SailConnection;

public abstract class AbstractSailConnection implements SailConnection {
	// Track the result sizes generated when evaluating a query, used by explain(...)
	protected boolean trackResultSize;

	// By default all tuple expressions are cloned before being optimized and executed. We don't want to do this for
	// .explain(...) since we need to retrieve the optimized or executed plan.
	protected boolean cloneTupleExpression = true;

	// Track the time used when evaluating a query, used by explain(...)
	protected boolean trackTime;

	@Override
	public Explanation explain(Explanation.Level level, TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred, int timeoutSeconds) {
		boolean queryTimedOut = false;

		try {

			switch (level) {
				case Timed:
					this.trackTime = true;
					this.trackResultSize = true;
					this.cloneTupleExpression = false;

					queryTimedOut = runQueryForExplain(tupleExpr, dataset, bindings, includeInferred, timeoutSeconds);
					break;

				case Executed:
					this.trackResultSize = true;
					this.cloneTupleExpression = false;

					queryTimedOut = runQueryForExplain(tupleExpr, dataset, bindings, includeInferred, timeoutSeconds);
					break;

				case Optimized:
					this.cloneTupleExpression = false;

					evaluate(tupleExpr, dataset, bindings, includeInferred).close();

					break;

				case Unoptimized:
					break;

				default:
					throw new UnsupportedOperationException("Unsupported query explanation level: " + level);

			}

		} finally {
			this.cloneTupleExpression = true;
			this.trackResultSize = false;
			this.trackTime = false;
		}

		QueryModelTreeToGenericPlanNode converter = new QueryModelTreeToGenericPlanNode(tupleExpr);
		tupleExpr.visit(converter);

		return new ExplanationImpl(converter.getGenericPlanNode(), queryTimedOut);

	}

	private boolean runQueryForExplain(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred, int timeoutSeconds) {

		AtomicBoolean timedOut = new AtomicBoolean(false);

		Thread currentThread = Thread.currentThread();

		// selfInterruptOnTimeoutThread will interrupt the current thread after a set timeout to stop the query
		// execution
		Thread selfInterruptOnTimeoutThread = new Thread(() -> {
			try {
				TimeUnit.SECONDS.sleep(timeoutSeconds);
				currentThread.interrupt();
				timedOut.set(true);
			} catch (InterruptedException ignored) {

			}
		});

		try {
			selfInterruptOnTimeoutThread.start();

			try (CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate = evaluate(tupleExpr, dataset, bindings, includeInferred)) {
				while (evaluate.hasNext()) {
					if (Thread.interrupted()) {
						break;
					}
					evaluate.next();
				}
			} catch (Exception e) {
				if (!timedOut.get()) {
					throw e;
				}
			}

			return timedOut.get();

		} finally {
			selfInterruptOnTimeoutThread.interrupt();
			try {
				// make sure selfInterruptOnTimeoutThread finishes
				selfInterruptOnTimeoutThread.join();
			} catch (InterruptedException ignored) {
			}

			// clear interrupted flag;
			Thread.interrupted();
		}

	}

}
