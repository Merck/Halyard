package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.AbstractValueFactory;
import org.eclipse.rdf4j.model.impl.BooleanLiteral;
import org.eclipse.rdf4j.model.impl.ContextStatement;
import org.eclipse.rdf4j.model.impl.SimpleStatement;

public class TimestampedValueFactory extends AbstractValueFactory {
	private static final TimestampedValueFactory VF = new TimestampedValueFactory();
	private static final Literal TRUE = new IdentifiableLiteral(HalyardTableUtils.id(BooleanLiteral.TRUE), BooleanLiteral.TRUE);
	private static final Literal FALSE = new IdentifiableLiteral(HalyardTableUtils.id(BooleanLiteral.FALSE), BooleanLiteral.FALSE);

	public static TimestampedValueFactory getInstance() {
		return VF;
	}

	@Override
	public IRI createIRI(String iri) {
		return new IdentifiableIRI(super.createIRI(iri));
	}

	@Override
	public BNode createBNode(String nodeID) {
		return new IdentifiableBNode(super.createBNode(nodeID));
	}

	@Override
	public Literal createLiteral(String value) {
		return new IdentifiableLiteral(super.createLiteral(value));
	}

	@Override
	public Literal createLiteral(String value, String language) {
		return new IdentifiableLiteral(super.createLiteral(value, language));
	}

	@Override
	public Literal createLiteral(String value, IRI datatype) {
		return new IdentifiableLiteral(super.createLiteral(value, datatype));
	}

	@Override
	public Literal createLiteral(boolean b) {
		return b ? TRUE : FALSE;
	}

	@Override
	protected Literal createNumericLiteral(Number number, IRI datatype) {
		return new IdentifiableLiteral(super.createNumericLiteral(number, datatype));
	}

	@Override
	public Triple createTriple(Resource subject, IRI predicate, Value object) {
		return new IdentifiableTriple(super.createTriple(unwrap(subject), unwrap(predicate), unwrap(object)));
	}

	@Override
	public Statement createStatement(Resource subject, IRI predicate, Value object) {
		return new TimestampedStatement(unwrap(subject), unwrap(predicate), unwrap(object));
	}

	@Override
	public Statement createStatement(Resource subject, IRI predicate, Value object, Resource context) {
		return new TimestampedContextStatement(unwrap(subject), unwrap(predicate), unwrap(object), unwrap(context));
	}


	@SuppressWarnings("unchecked")
	private static <T extends Value> T unwrap(T v) {
		return (v instanceof StatementValue<?>) ? ((StatementValue<T>)v).getValue() : v;
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
			} else if (subj instanceof Triple) {
				return new StatementTriple((Triple) subj, this);
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
			} else if (obj instanceof Triple) {
				return new StatementTriple((Triple) obj, this);
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
			} else if (subj instanceof Triple) {
				return new StatementTriple((Triple) subj, this);
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
			} else if (obj instanceof Triple) {
				return new StatementTriple((Triple) obj, this);
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

	static final class StatementIRI extends IRIWrapper implements StatementValue<IRI>, Identifiable {
		private static final long serialVersionUID = 3191250345781815876L;
		private final Statement stmt;

		StatementIRI(IRI iri, Statement stmt) {
			super(iri);
			this.stmt = stmt;
		}

		@Override
		public IRI getValue() {
			return iri;
		}

		@Override
		public Statement getStatement() {
			return stmt;
		}

		@Override
		public byte[] getId() {
			return (iri instanceof Identifiable) ? ((Identifiable)iri).getId() : null;
		}

		@Override
		public void setId(byte[] id) {
			if(iri instanceof Identifiable) {
				((Identifiable)iri).setId(id);
			}
		}
	}

	static final class StatementBNode extends BNodeWrapper implements StatementValue<BNode>, Identifiable {
		private static final long serialVersionUID = 2844469156112345508L;
		private final Statement stmt;

		StatementBNode(BNode bnode, Statement stmt) {
			super(bnode);
			this.stmt = stmt;
		}

		@Override
		public BNode getValue() {
			return bnode;
		}

		@Override
		public Statement getStatement() {
			return stmt;
		}

		@Override
		public byte[] getId() {
			return (bnode instanceof Identifiable) ? ((Identifiable)bnode).getId() : null;
		}

		@Override
		public void setId(byte[] id) {
			if(bnode instanceof Identifiable) {
				((Identifiable)bnode).setId(id);
			}
		}
	}

	static final class StatementLiteral extends LiteralWrapper implements StatementValue<Literal>, Identifiable {
		private static final long serialVersionUID = 8851736583549871759L;
		private final Statement stmt;

		StatementLiteral(Literal literal, Statement stmt) {
			super(literal);
			this.stmt = stmt;
		}

		@Override
		public Literal getValue() {
			return literal;
		}

		@Override
		public Statement getStatement() {
			return stmt;
		}

		@Override
		public byte[] getId() {
			return (literal instanceof Identifiable) ? ((Identifiable)literal).getId() : null;
		}

		@Override
		public void setId(byte[] id) {
			if(literal instanceof Identifiable) {
				((Identifiable)literal).setId(id);
			}
		}
	}

	static final class StatementTriple extends TripleWrapper implements StatementValue<Triple>, Identifiable {
		private static final long serialVersionUID = 5514584239151236972L;
		private final Statement stmt;

		StatementTriple(Triple triple, Statement stmt) {
			super(triple);
			this.stmt = stmt;
		}

		@Override
		public Triple getValue() {
			return triple;
		}

		@Override
		public Statement getStatement() {
			return stmt;
		}

		@Override
		public byte[] getId() {
			return (triple instanceof Identifiable) ? ((Identifiable)triple).getId() : null;
		}

		@Override
		public void setId(byte[] id) {
			if(triple instanceof Identifiable) {
				((Identifiable)triple).setId(id);
			}
		}
	}
}
