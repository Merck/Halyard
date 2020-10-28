package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.AbstractValueFactory;
import org.eclipse.rdf4j.model.impl.BooleanLiteral;

public class IdValueFactory extends AbstractValueFactory {
	private static final IdValueFactory VF = new IdValueFactory();
	private static final Literal TRUE = new IdentifiableLiteral(Hashes.id(BooleanLiteral.TRUE), BooleanLiteral.TRUE);
	private static final Literal FALSE = new IdentifiableLiteral(Hashes.id(BooleanLiteral.FALSE), BooleanLiteral.FALSE);

	public static IdValueFactory getInstance() {
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
		return new IdentifiableTriple(super.createTriple(subject, predicate, object));
	}
}
