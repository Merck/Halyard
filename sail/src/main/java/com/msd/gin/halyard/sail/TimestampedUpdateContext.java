package com.msd.gin.halyard.sail;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.UpdateExpr;
import org.eclipse.rdf4j.sail.UpdateContext;

import com.msd.gin.halyard.common.Timestamped;

public class TimestampedUpdateContext extends UpdateContext implements Timestamped {
	private final long defaultTs = System.currentTimeMillis();
	private long ts;

	public TimestampedUpdateContext(UpdateExpr updateExpr, Dataset dataset, BindingSet bindings,
			boolean includeInferred)
	{
		super(updateExpr, dataset, bindings, includeInferred);
	}

	@Override
	public long getTimestamp() {
		return ts;
	}

	@Override
	public void setTimestamp(long ts) {
		this.ts = ts;
	}

	public void useDefaultTimestamp() {
		this.ts = defaultTs;
	}
}
