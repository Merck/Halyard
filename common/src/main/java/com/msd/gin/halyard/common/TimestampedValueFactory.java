package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.AbstractValueFactory;
import org.eclipse.rdf4j.model.impl.ContextStatement;
import org.eclipse.rdf4j.model.impl.SimpleStatement;

public class TimestampedValueFactory extends AbstractValueFactory {
	private static final TimestampedValueFactory VF = new TimestampedValueFactory();

	public static TimestampedValueFactory getInstance() {
		return VF;
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

		public Resource getSubject() {
			Resource subj = super.getSubject();
			if (subj instanceof IRI) {
				return new StatementIRI((IRI) subj, this);
			} else if (subj instanceof BNode) {
				return new StatementBNode((BNode) subj, this);
			} else {
				throw new AssertionError();
			}
		}

		public IRI getPredicate() {
			IRI pred = super.getPredicate();
			return new StatementIRI(pred, this);
		}

		public Value getObject() {
			Value obj = super.getObject();
			if (obj instanceof IRI) {
				return new StatementIRI((IRI) obj, this);
			} else if (obj instanceof BNode) {
				return new StatementBNode((BNode) obj, this);
			} else if (obj instanceof Literal) {
				return new StatementLiteral((Literal) obj, this);
			} else {
				throw new AssertionError();
			}
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

		public Resource getSubject() {
			Resource subj = super.getSubject();
			if (subj instanceof IRI) {
				return new StatementIRI((IRI) subj, this);
			} else if (subj instanceof BNode) {
				return new StatementBNode((BNode) subj, this);
			} else {
				throw new AssertionError();
			}
		}

		public IRI getPredicate() {
			IRI pred = super.getPredicate();
			return new StatementIRI(pred, this);
		}

		public Value getObject() {
			Value obj = super.getObject();
			if (obj instanceof IRI) {
				return new StatementIRI((IRI) obj, this);
			} else if (obj instanceof BNode) {
				return new StatementBNode((BNode) obj, this);
			} else if (obj instanceof Literal) {
				return new StatementLiteral((Literal) obj, this);
			} else {
				throw new AssertionError();
			}
		}

		public Resource getContext() {
			Resource ctx = super.getContext();
			if (ctx != null) {
				if (ctx instanceof IRI) {
					return new StatementIRI((IRI) ctx, this);
				} else if (ctx instanceof BNode) {
					return new StatementBNode((BNode) ctx, this);
				} else {
					throw new AssertionError();
				}
			} else {
				return null;
			}
		}

		public String toString() {
			return super.toString()+" {"+ts+"}";
		}
	}

	static final class StatementIRI extends IdentifiableIRI implements StatementValue {
		private static final long serialVersionUID = 3191250345781815876L;
		private final Statement stmt;

		StatementIRI(IRI iri, Statement stmt) {
			super(iri);
			this.stmt = stmt;
		}

		public Statement getStatement() {
			return stmt;
		}
	}

	static final class StatementBNode extends IdentifiableBNode implements StatementValue {
		private static final long serialVersionUID = 2844469156112345508L;
		private final Statement stmt;

		StatementBNode(BNode bnode, Statement stmt) {
			super(bnode);
			this.stmt = stmt;
		}

		public Statement getStatement() {
			return stmt;
		}
	}

	static final class StatementLiteral extends IdentifiableLiteral implements StatementValue {
		private static final long serialVersionUID = 8851736583549871759L;
		private final Statement stmt;

		StatementLiteral(Literal literal, Statement stmt) {
			super(literal);
			this.stmt = stmt;
		}

		public Statement getStatement() {
			return stmt;
		}
	}
}
