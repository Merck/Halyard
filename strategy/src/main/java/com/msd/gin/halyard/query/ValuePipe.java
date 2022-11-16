package com.msd.gin.halyard.query;

import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;

public abstract class ValuePipe {
	protected final ValuePipe parent;
	private final AtomicBoolean done = new AtomicBoolean();

	protected ValuePipe(ValuePipe parent) {
		this.parent = parent;
	}

	public final void push(Value v) {
		if (!done.compareAndSet(false, true)) {
			throw new IllegalStateException("Value has already been pushed");
		}
		next(v);
	}

	protected void next(Value v) {
		if (parent != null) {
			parent.push(v);
		}
	}

	public void handleValueError(String msg) {
		if (parent != null) {
			parent.handleValueError(msg);
		} else {
			throw new ValueExprEvaluationException(msg);
		}
	}
}
