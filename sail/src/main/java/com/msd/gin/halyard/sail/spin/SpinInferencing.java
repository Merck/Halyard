/*******************************************************************************
 * Copyright (c) 2015 Eclipse RDF4J contributors, Aduna, and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *******************************************************************************/
package com.msd.gin.halyard.sail.spin;

import java.io.IOException;
import java.net.URL;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.Operation;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryPreparer;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.parser.ParsedBooleanQuery;
import org.eclipse.rdf4j.query.parser.ParsedGraphQuery;
import org.eclipse.rdf4j.query.parser.ParsedOperation;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.ParsedUpdate;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.sail.SailConnectionListener;
import org.eclipse.rdf4j.sail.inferencer.InferencerConnection;
import org.eclipse.rdf4j.sail.inferencer.fc.SchemaCachingRDFSInferencer;
import org.eclipse.rdf4j.sail.inferencer.util.RDFInferencerInserter;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.spin.ConstraintViolation;
import org.eclipse.rdf4j.spin.ConstraintViolationRDFHandler;
import org.eclipse.rdf4j.spin.MalformedSpinException;
import org.eclipse.rdf4j.spin.ParsedTemplate;
import org.eclipse.rdf4j.spin.SpinParser;

/**
 * Collection of useful utilities for doing SPIN inferencing.
 */
public class SpinInferencing {

	private static final String THIS_VAR = "this";

	private SpinInferencing() {
	}

	public static void insertSchema(RepositoryConnection target) throws RDFParseException, RepositoryException, IOException {
		String[] schemas = { "/schema/owl-basic.ttl", "/schema/sp.ttl", "/schema/spin.ttl", "/schema/spl.spin.ttl", "/schema/spif.ttl" };
		Repository repo = new SailRepository(new SchemaCachingRDFSInferencer(new MemoryStore()));
		try (RepositoryConnection conn = repo.getConnection()) {
			for (String path : schemas) {
				URL url = SpinInferencing.class.getResource(path);
				conn.add(url, RDFFormat.TURTLE);
			}
			target.add(conn.getStatements(null, null, null, true));
		}
	}

	public static int executeRule(Resource subj, Resource rule, QueryPreparer queryPreparer, SpinParser parser,
			InferencerConnection conn) {
		int nofInferred;
		TripleSource tripleSource = queryPreparer.getTripleSource();
		ParsedOperation parsedOp = parser.parse(rule, tripleSource);
		if (parsedOp instanceof ParsedGraphQuery) {
			ParsedGraphQuery graphQuery = (ParsedGraphQuery) parsedOp;
			GraphQuery queryOp = queryPreparer.prepare(graphQuery);
			addBindings(subj, rule, graphQuery, queryOp, tripleSource, parser);
			CountingRDFInferencerInserter handler = new CountingRDFInferencerInserter(conn,
					tripleSource.getValueFactory());
			queryOp.evaluate(handler);
			nofInferred = handler.getStatementCount();
		} else if (parsedOp instanceof ParsedUpdate) {
			ParsedUpdate graphUpdate = (ParsedUpdate) parsedOp;
			Update updateOp = queryPreparer.prepare(graphUpdate);
			addBindings(subj, rule, graphUpdate, updateOp, tripleSource, parser);
			UpdateCountListener listener = new UpdateCountListener();
			conn.addConnectionListener(listener);
			updateOp.execute();
			conn.removeConnectionListener(listener);
			// number of statement changes
			nofInferred = listener.getAddedStatementCount() + listener.getRemovedStatementCount();
		} else {
			throw new MalformedSpinException("Invalid rule: " + rule);
		}
		return nofInferred;
	}

	public static ConstraintViolation checkConstraint(Resource subj, Resource constraint, QueryPreparer queryPreparer,
			SpinParser parser) {
		ConstraintViolation violation;
		TripleSource tripleSource = queryPreparer.getTripleSource();
		ParsedQuery parsedQuery = parser.parseQuery(constraint, tripleSource);
		if (parsedQuery instanceof ParsedBooleanQuery) {
			ParsedBooleanQuery askQuery = (ParsedBooleanQuery) parsedQuery;
			BooleanQuery queryOp = queryPreparer.prepare(askQuery);
			addBindings(subj, constraint, askQuery, queryOp, tripleSource, parser);
			if (queryOp.evaluate()) {
				violation = parser.parseConstraintViolation(constraint, tripleSource);
			} else {
				violation = null;
			}
		} else if (parsedQuery instanceof ParsedGraphQuery) {
			ParsedGraphQuery graphQuery = (ParsedGraphQuery) parsedQuery;
			GraphQuery queryOp = queryPreparer.prepare(graphQuery);
			addBindings(subj, constraint, graphQuery, queryOp, tripleSource, parser);
			ConstraintViolationRDFHandler handler = new ConstraintViolationRDFHandler();
			queryOp.evaluate(handler);
			violation = handler.getConstraintViolation();
		} else {
			throw new MalformedSpinException("Invalid constraint: " + constraint);
		}
		return violation;
	}

	private static void addBindings(Resource subj, Resource opResource, ParsedOperation parsedOp, Operation op,
			TripleSource tripleSource, SpinParser parser) {
		if (!parser.isThisUnbound(opResource, tripleSource)) {
			op.setBinding(THIS_VAR, subj);
		}
		if (parsedOp instanceof ParsedTemplate) {
			for (Binding b : ((ParsedTemplate) parsedOp).getBindings()) {
				op.setBinding(b.getName(), b.getValue());
			}
		}
	}

	private static class UpdateCountListener implements SailConnectionListener {

		private int addedCount;

		private int removedCount;

		@Override
		public void statementAdded(Statement st) {
			addedCount++;
		}

		@Override
		public void statementRemoved(Statement st) {
			removedCount++;
		}

		public int getAddedStatementCount() {
			return addedCount;
		}

		public int getRemovedStatementCount() {
			return removedCount;
		}
	}

	private static class CountingRDFInferencerInserter extends RDFInferencerInserter {

		private int stmtCount;

		public CountingRDFInferencerInserter(InferencerConnection con, ValueFactory vf) {
			super(con, vf);
		}

		@Override
		protected void addStatement(Resource subj, IRI pred, Value obj, Resource ctxt) {
			super.addStatement(subj, pred, obj, ctxt);
			stmtCount++;
		}

		public int getStatementCount() {
			return stmtCount;
		}
	}
}
