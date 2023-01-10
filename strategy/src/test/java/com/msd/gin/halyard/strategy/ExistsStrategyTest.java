package com.msd.gin.halyard.strategy;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExistsStrategyTest {
    private Repository repo;
    private RepositoryConnection con;

    @BeforeEach
    public void setUp() throws Exception {
        repo = new SailRepository(new MemoryStoreWithExistsStrategy());
        repo.init();
        con = repo.getConnection();
    }

    @Test
    public void testS() {
        ValueFactory vf = con.getValueFactory();
        con.add(vf.createIRI("http://whatever/subj"), vf.createIRI("http://whatever/pred"), vf.createIRI("http://whatever/obj"));
        assertTrue(con.prepareBooleanQuery("ask {filter exists {<http://whatever/subj> ?p ?o}}").evaluate());
    }

    @Test
    public void testPO() {
        ValueFactory vf = con.getValueFactory();
        con.add(vf.createIRI("http://whatever/subj"), vf.createIRI("http://whatever/pred"), vf.createIRI("http://whatever/obj"));
        assertTrue(con.prepareBooleanQuery("ask {filter exists {?s <http://whatever/pred> <http://whatever/obj>}}").evaluate());
    }

    @Test
    public void testSPO() {
        ValueFactory vf = con.getValueFactory();
        con.add(vf.createIRI("http://whatever/subj"), vf.createIRI("http://whatever/pred"), vf.createIRI("http://whatever/obj"));
        assertTrue(con.prepareBooleanQuery("ask {filter exists {<http://whatever/subj> <http://whatever/pred> <http://whatever/obj>}}").evaluate());
    }

    @Test
    public void testNotExist() {
        ValueFactory vf = con.getValueFactory();
        con.add(vf.createIRI("http://whatever/subj"), vf.createIRI("http://whatever/pred"), vf.createIRI("http://whatever/obj"));
        assertFalse(con.prepareBooleanQuery("ask {filter exists {<http://whatever/subj1> ?p ?o}}").evaluate());
    }

    @AfterEach
    public void tearDown() throws Exception {
        con.close();
        repo.shutDown();
    }
}
