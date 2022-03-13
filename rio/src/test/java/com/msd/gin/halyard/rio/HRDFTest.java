package com.msd.gin.halyard.rio;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.junit.Test;

import com.msd.gin.halyard.vocab.SCHEMA_ORG;

public class HRDFTest {
	@Test
	public void testWriteRead() throws IOException {
		ValueFactory vf = SimpleValueFactory.getInstance();
		IRI foo = vf.createIRI("http://whatever/foo");
		IRI bar = vf.createIRI("http://whatever/bar");

		List<Statement> stmts = new ArrayList<>();
		stmts.add(vf.createStatement(foo, RDFS.LABEL, vf.createLiteral("foo")));
		stmts.add(vf.createStatement(foo, RDFS.LABEL, vf.createLiteral("foobar")));
		stmts.add(vf.createStatement(bar, RDFS.LABEL, vf.createLiteral("foobar")));
		stmts.add(vf.createStatement(bar, vf.createIRI("http://whatever/count"), vf.createLiteral(3), foo));
		stmts.add(vf.createStatement(bar, vf.createIRI("http://whatever/average"), vf.createLiteral(2.3), foo));
		stmts.add(vf.createStatement(vf.createTriple(foo, RDF.VALUE, vf.createTriple(vf.createBNode(), RDF.TYPE, SCHEMA_ORG.THING)), SCHEMA_ORG.ABOUT, vf.createTriple(bar, SCHEMA_ORG.NAME, vf.createLiteral("stuff"))));

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		RDFWriter writer = Rio.createWriter(HRDF.FORMAT, bos);
		writer.startRDF();
		for (Statement stmt : stmts) {
			writer.handleStatement(stmt);
		}
		writer.endRDF();
		bos.close();

		StatementCollector actualStmts = new StatementCollector();
		RDFParser parser = Rio.createParser(HRDF.FORMAT);
		parser.setPreserveBNodeIDs(true);
		parser.setRDFHandler(actualStmts);
		parser.parse(new ByteArrayInputStream(bos.toByteArray()));
		assertEquals(stmts, actualStmts.getStatements());
	}
}
