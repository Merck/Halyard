package com.msd.gin.halyard.strategy;

import static junit.framework.TestCase.assertTrue;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LiteralStrategyTest {

    private Repository repo;
    private RepositoryConnection con;

    @Before
    public void setUp() throws Exception {
        repo = new SailRepository(new MemoryStoreWithLiteralStrategy());
        repo.init();
        con = repo.getConnection();
    }

    @After
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
        TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SPARQL, q).evaluate();
        assertTrue(res.hasNext());
        BindingSet bs = res.next();
        assertEquals("a", ((IRI)bs.getValue("s")).getLocalName());
    }

    @Test
    public void testFilterLangs() {
        ValueFactory vf = con.getValueFactory();
        con.add(vf.createIRI("http://whatever/a"), vf.createIRI("http://whatever/val"), vf.createLiteral(1));
        con.add(vf.createIRI("http://whatever/b"), vf.createIRI("http://whatever/val"), vf.createLiteral("foo"));
        con.add(vf.createIRI("http://whatever/c"), vf.createIRI("http://whatever/val"), vf.createLiteral("bar", "en"));
    	String q ="SELECT ?s { ?s ?p ?o filter(lang(?o)='en') }";
        TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SPARQL, q).evaluate();
        assertTrue(res.hasNext());
        BindingSet bs = res.next();
        assertEquals("c", ((IRI)bs.getValue("s")).getLocalName());
    }
}
