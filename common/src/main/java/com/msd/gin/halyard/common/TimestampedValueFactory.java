package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.ContextStatement;
import org.eclipse.rdf4j.model.impl.SimpleStatement;

public class TimestampedValueFactory extends IdValueFactory {

	public TimestampedValueFactory(IdentifiableValueIO valueIO) {
		super(valueIO);
	}

	@Override
	public Statement createStatement(Resource subject, IRI predicate, Value object) {
		return new TimestampedStatement(subject, predicate, object);
	}

	@Override
	public Statement createStatement(Resource subject, IRI predicate, Value object, Resource context) {
		return new TimestampedContextStatement(subject, predicate, object, context);
	}


	static final class TimestampedStatement extends SimpleStatement implements Timestamped {
		private static final long serialVersionUID = -3767773807995225622L;
		private long ts;

		TimestampedStatement(Resource subject, IRI predicate, Value object) {
			super(subject, predicate, object);
		}

		@Override
		public long getTimestamp() {
			return ts;
		}

		@Override
		public void setTimestamp(long ts) {
			this.ts = ts;
		}

		public String toString() {
			return super.toString()+" {"+ts+"}";
		}
	}

	static final class TimestampedContextStatement extends ContextStatement implements Timestamped {
		private static final long serialVersionUID = 6631073994472700544L;
		private long ts;

		TimestampedContextStatement(Resource subject, IRI predicate, Value object, Resource context) {
			super(subject, predicate, object, context);
		}

		@Override
		public long getTimestamp() {
			return ts;
		}

		@Override
		public void setTimestamp(long ts) {
			this.ts = ts;
		}

		public String toString() {
			return super.toString()+" {"+ts+"}";
		}
	}
}
