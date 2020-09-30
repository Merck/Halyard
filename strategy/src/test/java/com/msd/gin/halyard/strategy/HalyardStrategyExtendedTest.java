/*
 * Copyright 2016 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
 * Inc., Kenilworth, NJ, USA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.msd.gin.halyard.strategy;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SESAME;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Adam Sotona (MSD)
 */
public class HalyardStrategyExtendedTest {

    private Repository repo;
    private RepositoryConnection con;

    @Before
    public void setUp() throws Exception {
        repo = new SailRepository(new MemoryStoreWithHalyardStrategy());
        repo.init();
        con = repo.getConnection();
    }

    @After
    public void tearDown() throws Exception {
        con.close();
        repo.shutDown();
    }

    @Test
    public void testIntersect() throws Exception {
        ValueFactory vf = con.getValueFactory();
        con.add(vf.createIRI("http://whatever/subj1"), vf.createIRI("http://whatever/pred1"), vf.createIRI("http://whatever/val1"));
        con.add(vf.createIRI("http://whatever/subj1"), vf.createIRI("http://whatever/pred2"), vf.createIRI("http://whatever/val1"));
        con.add(vf.createIRI("http://whatever/subj2"), vf.createIRI("http://whatever/pred1"), vf.createIRI("http://whatever/val1"));
        con.add(vf.createIRI("http://whatever/subj2"), vf.createIRI("http://whatever/pred2"), vf.createIRI("http://whatever/val2"));
        String serql = "SELECT val FROM {subj} <http://whatever/pred1> {val} INTERSECT SELECT val FROM {subj} <http://whatever/pred2> {val}";
        TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SERQL, serql).evaluate();
        assertTrue(res.hasNext());
        assertEquals(vf.createIRI("http://whatever/val1"), res.next().getBinding("val").getValue());
        if (res.hasNext()) {
            assertEquals(vf.createIRI("http://whatever/val1"), res.next().getBinding("val").getValue());
        }
    }

    @Test
    public void testLikeAndNamespace() throws Exception {
        ValueFactory vf = con.getValueFactory();
        con.add(vf.createIRI("urn:test/subjx"), vf.createIRI("urn:test/pred"), vf.createLiteral("valuex"));
        con.add(vf.createIRI("urn:test/xsubj"), vf.createIRI("urn:test/pred"), vf.createLiteral("xvalue"));
        String serql = "SELECT * FROM {s} test:pred {o} WHERE s LIKE \"urn:test/subj*\" USING NAMESPACE test = <urn:test/>";
        TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SERQL, serql).evaluate();
        assertTrue(res.hasNext());
        res.next();
        assertFalse(res.hasNext());
        serql = "SELECT * FROM {s} test:pred {o} WHERE o LIKE \"value*\" USING NAMESPACE test = <urn:test/>";
        res = con.prepareTupleQuery(QueryLanguage.SERQL, serql).evaluate();
        assertTrue(res.hasNext());
        res.next();
        assertFalse(res.hasNext());
    }

    @Test
    public void testSubqueries() throws Exception {
        ValueFactory vf = con.getValueFactory();
        con.add(vf.createIRI("http://whatever/a"), vf.createIRI("http://whatever/val"), vf.createLiteral(1));
        con.add(vf.createIRI("http://whatever/b"), vf.createIRI("http://whatever/val"), vf.createLiteral(2));
        con.add(vf.createIRI("http://whatever/b"), vf.createIRI("http://whatever/attrib"), vf.createLiteral("muhehe"));
        String serql = "SELECT higher FROM {node} <http://whatever/val> {higher} WHERE higher > ANY (SELECT value FROM {} <http://whatever/val> {value})";
        TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SERQL, serql).evaluate();
        assertTrue(res.hasNext());
        assertEquals(2, ((Literal) res.next().getValue("higher")).intValue());
        serql = "SELECT lower FROM {node} <http://whatever/val> {lower} WHERE lower < ANY (SELECT value FROM {} <http://whatever/val> {value})";
        res = con.prepareTupleQuery(QueryLanguage.SERQL, serql).evaluate();
        assertTrue(res.hasNext());
        assertEquals(1, ((Literal) res.next().getValue("lower")).intValue());
        con.add(vf.createBNode("http://whatever/c"), vf.createIRI("http://whatever/val"), vf.createLiteral(3));
        serql = "SELECT highest FROM {node} <http://whatever/val> {highest} WHERE highest >= ALL (SELECT value FROM {} <http://whatever/val> {value})";
        res = con.prepareTupleQuery(QueryLanguage.SERQL, serql).evaluate();
        assertTrue(res.hasNext());
        assertEquals(3, ((Literal) res.next().getValue("highest")).intValue());
        serql = "SELECT val FROM {node} <http://whatever/val> {val} WHERE val IN (SELECT value FROM {} <http://whatever/val> {value}; <http://whatever/attrib> {\"muhehe\"})";
        res = con.prepareTupleQuery(QueryLanguage.SERQL, serql).evaluate();
        assertTrue(res.hasNext());
        assertEquals(2, ((Literal) res.next().getValue("val")).intValue());
    }

    @Test(expected = QueryEvaluationException.class)
    public void testService() throws Exception {
        String sparql = "SELECT * WHERE {SERVICE <http://whatever/> { ?s ?p ?o . }}";
        TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SPARQL, sparql).evaluate();
        res.hasNext();
    }

    @Test
    public void testServiceSilent() throws Exception {
        String sparql = "SELECT * WHERE {SERVICE SILENT <http://whatever/> { ?s ?p ?o . }}";
        TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SPARQL, sparql).evaluate();
        res.hasNext();
    }

    @Test
    public void testReduced() throws Exception {
        String sparql = "SELECT REDUCED ?a WHERE {VALUES ?a {0 0 1 1 0 0 1 1}}";
        TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SPARQL, sparql).evaluate();
        assertTrue(res.hasNext());
        assertEquals(0, ((Literal) res.next().getValue("a")).intValue());
        assertTrue(res.hasNext());
        assertEquals(1, ((Literal) res.next().getValue("a")).intValue());
        assertTrue(res.hasNext());
        assertEquals(0, ((Literal) res.next().getValue("a")).intValue());
        assertTrue(res.hasNext());
        assertEquals(1, ((Literal) res.next().getValue("a")).intValue());
        assertFalse(res.hasNext());
    }

    @Test
    public void testSES2154SubselectOptional() throws Exception {
        SimpleValueFactory vf = SimpleValueFactory.getInstance();
        IRI person = vf.createIRI("http://schema.org/Person");
        for (char c = 'a'; c < 'k'; c++) {
            con.add(vf.createIRI("http://example.com/" + c), RDF.TYPE, person);
        }
        TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SPARQL, "PREFIX : <http://example.com/>\n" + "PREFIX schema: <http://schema.org/>\n" + "\n" + "SELECT (COUNT(*) AS ?count)\n" + "WHERE {\n" + "  {\n" + "    SELECT ?person\n" + "    WHERE {\n" + "      ?person a schema:Person .\n" + "    }\n" + "    LIMIT 5\n" + "  }\n" + "  OPTIONAL {\n" + "    [] :nonexistent [] .\n" + "  }\n" + "}").evaluate();
        assertEquals(5, ((Literal) res.next().getBinding("count").getValue()).intValue());
    }

    @Test (expected = QueryEvaluationException.class)
    public void testInvalidFunction() {
        con.prepareTupleQuery(QueryLanguage.SPARQL, "PREFIX fn: <http://example.com/>\nSELECT ?whatever\nWHERE {\nBIND (fn:whatever(\"foo\") AS ?whatever)\n}").evaluate().hasNext();
    }

    @Test
    public void testSesameNil() throws Exception {
        SimpleValueFactory vf = SimpleValueFactory.getInstance();
        con.add(vf.createIRI("http://a"), vf.createIRI("http://b"), vf.createIRI("http://c"));
        con.add(vf.createIRI("http://a"), vf.createIRI("http://d"), vf.createIRI("http://e"), vf.createIRI("http://f"));
        TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SPARQL, "PREFIX sesame: <" + SESAME.NAMESPACE + ">\nSELECT (COUNT(*) AS ?count)\n" + "FROM sesame:nil WHERE {?s ?p ?o}").evaluate();
        assertEquals(1, ((Literal) res.next().getBinding("count").getValue()).intValue());
    }

    @Test
    public void testEmptyOptional() throws Exception {
        String q = "SELECT * WHERE {" +
            "  OPTIONAL {<https://nonexisting> <https://nonexisting> <https://nonexisting> .}" +
            "}";
        TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SPARQL, q).evaluate();
        assertTrue(res.hasNext());
    }

    @Test
    public void testEmptyOptionalSubselect() throws Exception {
        String q = "SELECT * WHERE {" +
            "  OPTIONAL { SELECT * WHERE {<https://nonexisting> <https://nonexisting> <https://nonexisting> .}}" +
            "}";
        TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SPARQL, q).evaluate();
        assertFalse(res.hasNext());
    }

    @Test
    public void testAggregates() {
    	String q ="SELECT (MAX(?x) as ?maxx) (MIN(?x) as ?minx) (AVG(?x) as ?avgx) (SUM(?x) as ?sumx) (COUNT(?x) as ?countx) (SAMPLE(?x) as ?samplex) (GROUP_CONCAT(?x) as ?concatx) { VALUES ?x {1 2 2 3} }";
        TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SPARQL, q).evaluate();
        assertTrue(res.hasNext());
        BindingSet bs = res.next();
        assertEquals(3, ((Literal) bs.getValue("maxx")).intValue());
        assertEquals(1, ((Literal) bs.getValue("minx")).intValue());
        assertEquals(2, ((Literal) bs.getValue("avgx")).intValue());
        assertEquals(4, ((Literal) bs.getValue("countx")).intValue());
        assertEquals(8, ((Literal) bs.getValue("sumx")).intValue());
        assertNotNull(bs.getValue("samplex"));
        assertEquals("1 2 2 3", ((Literal) bs.getValue("concatx")).getLabel());
    }

    @Test
    public void testDistinctAggregates() {
    	String q ="SELECT (MAX(distinct ?x) as ?maxx) (MIN(distinct ?x) as ?minx) (AVG(distinct ?x) as ?avgx) (SUM(distinct ?x) as ?sumx) (COUNT(distinct ?x) as ?countx) (SAMPLE(distinct ?x) as ?samplex) (GROUP_CONCAT(distinct ?x) as ?concatx) { VALUES ?x {1 2 2 3} }";
        TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SPARQL, q).evaluate();
        assertTrue(res.hasNext());
        BindingSet bs = res.next();
        assertEquals(3, ((Literal) bs.getValue("maxx")).intValue());
        assertEquals(1, ((Literal) bs.getValue("minx")).intValue());
        assertEquals(2, ((Literal) bs.getValue("avgx")).intValue());
        assertEquals(3, ((Literal) bs.getValue("countx")).intValue());
        assertEquals(6, ((Literal) bs.getValue("sumx")).intValue());
        assertNotNull(bs.getValue("samplex"));
        assertEquals("1 2 3", ((Literal) bs.getValue("concatx")).getLabel());
    }

    @Test
    public void testEmptyAggregate() {
    	String q ="SELECT (MAX(?x) as ?maxx) {}";
        TupleQueryResult res = con.prepareTupleQuery(QueryLanguage.SPARQL, q).evaluate();
        assertTrue(res.hasNext());
        BindingSet bs = res.next();
        assertEquals(0, bs.size());
    }
}
