package com.msd.gin.halyard.strategy;

import static junit.framework.TestCase.assertTrue;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConstrainedValueStrategyTest {

    private Repository repo;
    private RepositoryConnection con;

    @BeforeEach
    public void setUp() throws Exception {
        repo = new SailRepository(new MemoryStoreWithConstraintsStrategy());
        repo.init();
        con = repo.getConnection();
    }

    @AfterEach
    public void tearDown() throws Exception {
        con.close();
        repo.shutDown();
    }

    @Test
    public void testFilterDatatype() {
        ValueFactory vf = con.getValueFactory();
        con.add(vf.createIRI("http://whatever/a"), vf.createIRI("http://whatever/val"), vf.createLiteral(1));
        con.add(vf.createIRI("http://whatever/b"), vf.createIRI("http://whatever/val"), vf.createLiteral("foo"));
        con.add(vf.createIRI("http://whatever/c"), vf.createIRI("http://whatever/val"), vf.createLiteral("bar", "en"));
    	String q ="SELECT ?s { ?s ?p ?o filter(datatype(?o)=xsd:int) }";
        try (TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SPARQL, q).evaluate()) {
	        assertTrue(res.hasNext());
	        BindingSet bs = res.next();
	        assertEquals("a", ((IRI)bs.getValue("s")).getLocalName());
        }
    }

    @Test
    public void testFilterDatatypeNotEqual() {
        ValueFactory vf = con.getValueFactory();
        con.add(vf.createIRI("http://whatever/a"), vf.createIRI("http://whatever/val"), vf.createLiteral(1));
        con.add(vf.createIRI("http://whatever/b"), vf.createIRI("http://whatever/val"), vf.createLiteral("foo"));
    	String q ="SELECT ?s { ?s ?p ?o filter(datatype(?o)!=xsd:int) }";
        try (TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SPARQL, q).evaluate()) {
	        assertTrue(res.hasNext());
	        BindingSet bs = res.next();
	        assertEquals("b", ((IRI)bs.getValue("s")).getLocalName());
        }
    }

    @Test
    public void testFilterLangs() {
        ValueFactory vf = con.getValueFactory();
        con.add(vf.createIRI("http://whatever/a"), vf.createIRI("http://whatever/val"), vf.createLiteral(1));
        con.add(vf.createIRI("http://whatever/b"), vf.createIRI("http://whatever/val"), vf.createLiteral("foo"));
        con.add(vf.createIRI("http://whatever/c"), vf.createIRI("http://whatever/val"), vf.createLiteral("bar", "en"));
    	String q ="SELECT ?s { ?s ?p ?o filter(lang(?o)='en') }";
        try (TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SPARQL, q).evaluate()) {
	        assertTrue(res.hasNext());
	        BindingSet bs = res.next();
	        assertEquals("c", ((IRI)bs.getValue("s")).getLocalName());
        }
    }

    @Test
    public void testFilterLangsNotEqual() {
        ValueFactory vf = con.getValueFactory();
        con.add(vf.createIRI("http://whatever/a"), vf.createIRI("http://whatever/val"), vf.createLiteral(1));
        con.add(vf.createIRI("http://whatever/b"), vf.createIRI("http://whatever/val"), vf.createLiteral("bar", "fr"));
        con.add(vf.createIRI("http://whatever/c"), vf.createIRI("http://whatever/val"), vf.createLiteral("bar", "en"));
    	String q ="SELECT ?s { ?s ?p ?o filter(lang(?o)>'en') }";
        try (TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SPARQL, q).evaluate()) {
	        assertTrue(res.hasNext());
	        BindingSet bs = res.next();
	        assertEquals("b", ((IRI)bs.getValue("s")).getLocalName());
        }
    }

    @Test
    public void testFilterIRIs() {
        ValueFactory vf = con.getValueFactory();
        con.add(vf.createIRI("http://whatever/a"), vf.createIRI("http://whatever/val"), vf.createLiteral(1));
        con.add(vf.createBNode(), vf.createIRI("http://whatever/val"), vf.createLiteral("foo"));
    	String q ="SELECT ?s { ?s ?p ?o filter(isIRI(?s)) }";
        try (TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SPARQL, q).evaluate()) {
	        assertTrue(res.hasNext());
	        BindingSet bs = res.next();
	        assertTrue(bs.getValue("s").isIRI());
        }
    }

    @Test
    public void testFilterBNodes() {
        ValueFactory vf = con.getValueFactory();
        con.add(vf.createIRI("http://whatever/a"), vf.createIRI("http://whatever/val"), vf.createLiteral(1));
        con.add(vf.createBNode(), vf.createIRI("http://whatever/val"), vf.createLiteral("foo"));
    	String q ="SELECT ?s { ?s ?p ?o filter(isBlank(?s)) }";
        try (TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SPARQL, q).evaluate()) {
	        assertTrue(res.hasNext());
	        BindingSet bs = res.next();
	        assertTrue(bs.getValue("s").isBNode());
        }
    }

    @Test
    public void testFilterIsLiteral() {
        ValueFactory vf = con.getValueFactory();
        con.add(vf.createIRI("http://whatever/a"), vf.createIRI("http://whatever/val"), vf.createBNode());
        con.add(vf.createIRI("http://whatever/b"), vf.createIRI("http://whatever/val"), vf.createLiteral("foo", GEO.WKT_LITERAL));
    	String q ="SELECT ?s { ?s ?p ?o filter(isLiteral(?o)) }";
        try (TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SPARQL, q).evaluate()) {
	        assertTrue(res.hasNext());
	        BindingSet bs = res.next();
	        assertEquals("b", ((IRI)bs.getValue("s")).getLocalName());
        }
    }

    @Test
    public void testFilterIsNumeric() {
        ValueFactory vf = con.getValueFactory();
        con.add(vf.createIRI("http://whatever/a"), vf.createIRI("http://whatever/val"), vf.createLiteral(1));
        con.add(vf.createIRI("http://whatever/b"), vf.createIRI("http://whatever/val"), vf.createLiteral("foo"));
    	String q ="SELECT ?s { ?s ?p ?o filter(isNumeric(?o)) }";
        try (TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SPARQL, q).evaluate()) {
	        assertTrue(res.hasNext());
	        BindingSet bs = res.next();
	        assertEquals("a", ((IRI)bs.getValue("s")).getLocalName());
        }
    }

    @Test
    public void testFilterTriples() {
        ValueFactory vf = con.getValueFactory();
        con.add(vf.createIRI("http://whatever/a"), vf.createIRI("http://whatever/val"), vf.createTriple(vf.createIRI("http://whatever/b"), vf.createIRI("http://whatever/val"), vf.createLiteral(1)));
    	String q = "SELECT ?s { <http://whatever/a> <http://whatever/val> << ?s ?p ?o >> }";
        try (TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SPARQL, q).evaluate()) {
	        assertTrue(res.hasNext());
	        BindingSet bs = res.next();
	        assertEquals("b", ((IRI)bs.getValue("s")).getLocalName());
        }
    }

    @Test
    public void testFilterComparison() {
        ValueFactory vf = con.getValueFactory();
        con.add(vf.createIRI("http://whatever/a"), vf.createIRI("http://whatever/val"), vf.createLiteral(1));
        con.add(vf.createIRI("http://whatever/b"), vf.createIRI("http://whatever/val"), vf.createLiteral(10));
    	String q ="SELECT ?s { ?s ?p ?o filter(?o > 5) }";
        try (TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SPARQL, q).evaluate()) {
	        assertTrue(res.hasNext());
	        BindingSet bs = res.next();
	        assertEquals("b", ((IRI)bs.getValue("s")).getLocalName());
        }
    }
}
