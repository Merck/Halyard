package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.StatementTest;
import org.eclipse.rdf4j.model.Value;

public class TimestampedStatementTest extends StatementTest {
    private static final RDFFactory rdfFactory = RDFFactory.create();

	@Override
	protected IRI iri(String iri) {
		return new IdentifiableIRI(iri, rdfFactory);
	}

	@Override
	protected Statement statement(Resource s, IRI p, Value o, Resource c) {
		if (c != null) {
			return rdfFactory.getTimestampedValueFactory().createStatement(s, p, o, c);
		} else {
			return rdfFactory.getTimestampedValueFactory().createStatement(s, p, o);
		}
	}

}
