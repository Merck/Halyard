package com.msd.gin.halyard.common;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;
import java.util.Date;
import java.util.UUID;

import javax.xml.datatype.XMLGregorianCalendar;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.AbstractValueFactory;

public class IdValueFactory implements ValueFactory {
	private static final ValueFactory DELEGATE = new AbstractValueFactory() {};
	private final RDFFactory rdfFactory;
	private final Literal TRUE;
	private final Literal FALSE;

	IdValueFactory(RDFFactory valueIO) {
		this.rdfFactory = valueIO;
		this.TRUE = new IdentifiableLiteral(DELEGATE.createLiteral(true), valueIO);
		this.FALSE = new IdentifiableLiteral(DELEGATE.createLiteral(false), valueIO);
	}

	@Override
	public IRI createIRI(String iri) {
		return new IdentifiableIRI(iri, rdfFactory);
	}

	@Override
	public IRI createIRI(String namespace, String localName) {
		return new IdentifiableIRI(namespace, localName, rdfFactory);
	}

	@Override
	public BNode createBNode() {
		return new IdentifiableBNode(DELEGATE.createBNode(UUID.randomUUID().toString()), rdfFactory);
	}

	@Override
	public BNode createBNode(String nodeID) {
		return new IdentifiableBNode(DELEGATE.createBNode(nodeID), rdfFactory);
	}

	@Override
	public Literal createLiteral(String value) {
		return new IdentifiableLiteral(DELEGATE.createLiteral(value), rdfFactory);
	}

	@Override
	public Literal createLiteral(String value, String language) {
		return new IdentifiableLiteral(DELEGATE.createLiteral(value, language), rdfFactory);
	}

	@Override
	public Literal createLiteral(String value, IRI datatype) {
		return new IdentifiableLiteral(DELEGATE.createLiteral(value, datatype), rdfFactory);
	}

	@Override
	public Literal createLiteral(boolean b) {
		return b ? TRUE : FALSE;
	}

	@Override
	public Literal createLiteral(byte value) {
		return new IdentifiableLiteral(DELEGATE.createLiteral(value), rdfFactory);
	}

	@Override
	public Literal createLiteral(short value) {
		return new IdentifiableLiteral(DELEGATE.createLiteral(value), rdfFactory);
	}

	@Override
	public Literal createLiteral(int value) {
		return new IdentifiableLiteral(DELEGATE.createLiteral(value), rdfFactory);
	}

	@Override
	public Literal createLiteral(long value) {
		return new IdentifiableLiteral(DELEGATE.createLiteral(value), rdfFactory);
	}

	@Override
	public Literal createLiteral(float value) {
		return new IdentifiableLiteral(DELEGATE.createLiteral(value), rdfFactory);
	}

	@Override
	public Literal createLiteral(double value) {
		return new IdentifiableLiteral(DELEGATE.createLiteral(value), rdfFactory);
	}

	@Override
	public Literal createLiteral(BigDecimal value) {
		return new IdentifiableLiteral(DELEGATE.createLiteral(value), rdfFactory);
	}

	@Override
	public Literal createLiteral(BigInteger value) {
		return new IdentifiableLiteral(DELEGATE.createLiteral(value), rdfFactory);
	}

	@Override
	public Literal createLiteral(XMLGregorianCalendar calendar) {
		return new IdentifiableLiteral(DELEGATE.createLiteral(calendar), rdfFactory);
	}

	@Override
	public Literal createLiteral(Date date) {
		return new IdentifiableLiteral(DELEGATE.createLiteral(date), rdfFactory);
	}

	public Literal createLiteral(TemporalAccessor value) {
		return new IdentifiableLiteral(DELEGATE.createLiteral(value), rdfFactory);
	}

	@Override
	public Literal createLiteral(TemporalAmount value) {
		return new IdentifiableLiteral(DELEGATE.createLiteral(value), rdfFactory);
	}

	@Override
	public Triple createTriple(Resource subject, IRI predicate, Value object) {
		return new IdentifiableTriple(DELEGATE.createTriple(subject, predicate, object), rdfFactory);
	}

	@Override
	public Statement createStatement(Resource subject, IRI predicate, Value object) {
		return DELEGATE.createStatement(subject, predicate, object);
	}

	@Override
	public Statement createStatement(Resource subject, IRI predicate, Value object, Resource context) {
		return DELEGATE.createStatement(subject, predicate, object, context);
	}
}
