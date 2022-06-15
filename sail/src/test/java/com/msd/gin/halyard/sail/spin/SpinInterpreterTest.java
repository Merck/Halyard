package com.msd.gin.halyard.sail.spin;

import java.io.IOException;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.AbstractFederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ExtendedEvaluationStrategyFactory;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.sail.evaluation.SailTripleSource;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.spin.SpinParser;
import org.junit.Test;

import static org.junit.Assert.*;

public class SpinInterpreterTest {

	@Test
	public void testNativeInterpreter() throws RDFParseException, RepositoryException, IOException {
		MemoryStore sail = new MemoryStore();
		sail.setEvaluationStrategyFactory(new ExtendedEvaluationStrategyFactory());
		SailRepository repo = new SailRepository(sail);
		repo.init();
		SailRepositoryConnection repoConn = repo.getConnection();
		SpinInferencing.insertSchema(repoConn);

		TripleSource tripleSource = new SailTripleSource(repoConn.getSailConnection(), true, repoConn.getValueFactory());
		SpinMagicPropertyInterpreter interpreter = new SpinMagicPropertyInterpreter(new SpinParser(), tripleSource, new TupleFunctionRegistry(), null);
		ParsedTupleQuery q = QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL, "prefix spif: <http://spinrdf.org/spif#> " + "select ?str {?str spif:split (\"Hello World\" \" \")}", null);
		interpreter.optimize(q.getTupleExpr(), null, null);
		try (CloseableIteration<? extends BindingSet, QueryEvaluationException> iter = repoConn.getSailConnection().evaluate(q.getTupleExpr(), null, new QueryBindingSet(), true)) {
			assertEquals("Hello", iter.next().getValue("str").stringValue());
			assertEquals("World", iter.next().getValue("str").stringValue());
		}
	}

	@Test
	public void testSpinServiceInterpreter() throws RDFParseException, RepositoryException, IOException {
		MemoryStore sail = new MemoryStore();
		SailRepository repo = new SailRepository(sail);
		repo.init();
		SailRepositoryConnection repoConn = repo.getConnection();
		SpinInferencing.insertSchema(repoConn);

		TripleSource tripleSource = new SailTripleSource(repoConn.getSailConnection(), true, repoConn.getValueFactory());
		SpinMagicPropertyInterpreter interpreter = new SpinMagicPropertyInterpreter(new SpinParser(), tripleSource, new TupleFunctionRegistry(), (AbstractFederatedServiceResolver) sail.getFederatedServiceResolver());
		ParsedTupleQuery q = QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL, "prefix spif: <http://spinrdf.org/spif#> " + "select ?str {?str spif:split (\"Hello World\" \" \")}", null);
		interpreter.optimize(q.getTupleExpr(), null, null);
		try (CloseableIteration<? extends BindingSet, QueryEvaluationException> iter = repoConn.getSailConnection().evaluate(q.getTupleExpr(), null, new QueryBindingSet(), true)) {
			assertEquals("Hello", iter.next().getValue("str").stringValue());
			assertEquals("World", iter.next().getValue("str").stringValue());
		}
	}
}
