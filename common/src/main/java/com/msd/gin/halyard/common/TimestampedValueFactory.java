package com.msd.gin.halyard.common;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

import javax.xml.datatype.XMLGregorianCalendar;

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
			if (ctx instanceof IRI) {
				return new StatementIRI((IRI) ctx, this);
			} else if (ctx instanceof BNode) {
				return new StatementBNode((BNode) ctx, this);
			} else {
				throw new AssertionError();
			}
		}
	}

	static final class StatementIRI implements IRI, StatementValue {
		private static final long serialVersionUID = 3191250345781815876L;
		private final IRI iri;
		private final Statement stmt;

		StatementIRI(IRI iri, Statement stmt) {
			this.iri = iri;
			this.stmt = stmt;
		}

		public Statement getStatement() {
			return stmt;
		}

		@Override
		public String getLocalName() {
			return iri.getLocalName();
		}

		@Override
		public String getNamespace() {
			return iri.getNamespace();
		}

		@Override
		public String stringValue() {
			return iri.stringValue();
		}

		public String toString() {
			return iri.toString();
		}

		public int hashCode() {
			return iri.hashCode();
		}

		public boolean equals(Object o) {
			return iri.equals(o);
		}
	}

	static final class StatementBNode implements BNode, StatementValue {
		private static final long serialVersionUID = 2844469156112345508L;
		private final BNode bnode;
		private final Statement stmt;

		StatementBNode(BNode iri, Statement stmt) {
			this.bnode = iri;
			this.stmt = stmt;
		}

		public Statement getStatement() {
			return stmt;
		}

		@Override
		public String getID() {
			return bnode.getID();
		}

		@Override
		public String stringValue() {
			return bnode.stringValue();
		}

		public String toString() {
			return bnode.toString();
		}

		public int hashCode() {
			return bnode.hashCode();
		}

		public boolean equals(Object o) {
			return bnode.equals(o);
		}
	}

	static final class StatementLiteral implements Literal, StatementValue {
		private static final long serialVersionUID = 8851736583549871759L;
		private final Literal literal;
		private final Statement stmt;

		StatementLiteral(Literal literal, Statement stmt) {
			this.literal = literal;
			this.stmt = stmt;
		}

		public Statement getStatement() {
			return stmt;
		}

		@Override
		public String getLabel() {
			return literal.getLabel();
		}

		public Optional<String> getLanguage() {
			return literal.getLanguage();
		}

		public IRI getDatatype() {
			return literal.getDatatype();
		}

		public byte byteValue() {
			return literal.byteValue();
		}

		public short shortValue() {
			return literal.shortValue();
		}

		public int intValue() {
			return literal.intValue();
		}

		public long longValue() {
			return literal.longValue();
		}

		public BigInteger integerValue() {
			return literal.integerValue();
		}

		public BigDecimal decimalValue() {
			return literal.decimalValue();
		}

		public float floatValue() {
			return literal.floatValue();
		}

		public double doubleValue() {
			return literal.doubleValue();
		}

		public boolean booleanValue() {
			return literal.booleanValue();
		}

		public XMLGregorianCalendar calendarValue() {
			return literal.calendarValue();
		}

		@Override
		public String stringValue() {
			return literal.stringValue();
		}

		public String toString() {
			return literal.toString();
		}

		public int hashCode() {
			return literal.hashCode();
		}

		public boolean equals(Object o) {
			return literal.equals(o);
		}
	}
}
