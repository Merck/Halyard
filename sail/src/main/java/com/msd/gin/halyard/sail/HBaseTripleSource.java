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
package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.algebra.evaluation.ConstrainedTripleSourceFactory;
import com.msd.gin.halyard.algebra.evaluation.ExtendedTripleSource;
import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.common.KeyspaceConnection;
import com.msd.gin.halyard.common.RDFContext;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.RDFObject;
import com.msd.gin.halyard.common.RDFPredicate;
import com.msd.gin.halyard.common.RDFSubject;
import com.msd.gin.halyard.common.StatementIndices;
import com.msd.gin.halyard.common.TimestampedValueFactory;
import com.msd.gin.halyard.common.ValueConstraint;
import com.msd.gin.halyard.common.ValueIO;
import com.msd.gin.halyard.vocab.HALYARD;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.ConvertingIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.ExceptionConvertingIteration;
import org.eclipse.rdf4j.common.iteration.FilterIteration;
import org.eclipse.rdf4j.common.iteration.TimeLimitIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SPIN;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.RDFStarTripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTripleSource implements ExtendedTripleSource, RDFStarTripleSource, ConstrainedTripleSourceFactory, Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(HBaseTripleSource.class);

	protected final KeyspaceConnection keyspaceConn;
	protected final StatementIndices stmtIndices;
	protected final RDFFactory rdfFactory;
	private final ValueIO.Reader valueReader;
	private final long timeoutSecs;
	private final HBaseSail.ScanSettings settings;
	private final HBaseSail.Ticker ticker;

	public HBaseTripleSource(KeyspaceConnection table, ValueFactory vf, StatementIndices stmtIndices, long timeoutSecs) {
		this(table, vf, stmtIndices, timeoutSecs, null, null);
	}

	HBaseTripleSource(KeyspaceConnection table, ValueFactory vf, StatementIndices stmtIndices, long timeoutSecs, HBaseSail.ScanSettings settings, HBaseSail.Ticker ticker) {
		this(table, stmtIndices.createTableReader(vf, table), stmtIndices, timeoutSecs, settings, ticker);
	}

	private HBaseTripleSource(KeyspaceConnection table, ValueIO.Reader valueReader, StatementIndices stmtIndices, long timeoutSecs, HBaseSail.ScanSettings settings, HBaseSail.Ticker ticker) {
		this.keyspaceConn = table;
		this.stmtIndices = stmtIndices;
		this.rdfFactory = stmtIndices.getRDFFactory();
		this.valueReader = valueReader;
		this.timeoutSecs = timeoutSecs;
		this.settings = settings;
		this.ticker = ticker;
	}

	@Override
	public final CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
		if (RDF.TYPE.equals(pred) && SPIN.MAGIC_PROPERTY_CLASS.equals(obj)) {
			// cache magic property definitions here
			return new EmptyIteration<>();
		} else {
			return getStatementsInternal(subj, pred, obj, contexts, valueReader);
		}
	}

	@Override
	public final boolean hasStatement(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
		if (RDF.TYPE.equals(pred) && SPIN.MAGIC_PROPERTY_CLASS.equals(obj)) {
			// cache magic property definitions here
			return false;
		} else {
			return hasStatementsInternal(subj, pred, obj, contexts);
		}
	}

	public TripleSource getTimestampedTripleSource() {
		ValueIO.Reader tsValueReader = stmtIndices.createTableReader(TimestampedValueFactory.INSTANCE, keyspaceConn);
		return new HBaseTripleSource(keyspaceConn, tsValueReader, stmtIndices, timeoutSecs, settings, ticker);
	}

	@Override
	public TripleSource getTripleSource(ValueConstraint subjConstraint, ValueConstraint objConstraints) {
		return new HBaseTripleSource(keyspaceConn, valueReader, stmtIndices, timeoutSecs, settings, ticker) {
			@Override
			protected Scan scan(RDFSubject subj, RDFPredicate pred, RDFObject obj, RDFContext ctx, StatementIndices indices) {
				return HalyardTableUtils.scanWithConstraints(subj, subjConstraint, pred, obj, objConstraints, ctx, indices);
			}
		};
	}

	private CloseableIteration<? extends Statement, QueryEvaluationException> getStatementsInternal(Resource subj, IRI pred, Value obj, Resource[] contexts, ValueIO.Reader reader) {
		List<Resource> contextsToScan;
		if (contexts == null || contexts.length == 0) {
			// if all contexts then scan the default context
			contextsToScan = Collections.singletonList(null);
		} else if (Arrays.stream(contexts).anyMatch(Objects::isNull)) {
			// if any context is the default context then just scan the default context (everything)
			contextsToScan = Collections.singletonList(null);
		} else {
			contextsToScan = Arrays.asList(contexts);
		}
		CloseableIteration<? extends Statement, QueryEvaluationException> iter = timeLimit(new ExceptionConvertingIteration<Statement, QueryEvaluationException>(createStatementScanner(subj, pred, obj, contextsToScan, reader)) {
			@Override
			protected QueryEvaluationException convert(Exception e) {
				return new QueryEvaluationException(e);
			}
		}, timeoutSecs);
		if (contexts != null && Arrays.stream(contexts).anyMatch(Objects::isNull)) {
			// filter out any scan that includes the default context (everything) to the specified contexts
			final Set<Resource> ctxSet = new HashSet<>();
			Collections.addAll(ctxSet, contexts);
			iter = new FilterIteration<Statement, QueryEvaluationException>(iter) {
				@Override
				protected boolean accept(Statement st) {
					return ctxSet.contains(st.getContext());
				}
			};
		}
		return iter;
	}

	protected CloseableIteration<? extends Statement, IOException> createStatementScanner(Resource subj, IRI pred, Value obj, List<Resource> contexts, ValueIO.Reader reader) {
		return new StatementScanner(subj, pred, obj, contexts, reader);
	}

	private boolean hasStatementsInternal(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
		try (CloseableIteration<? extends Statement, QueryEvaluationException> iter = getStatements(subj, pred, obj, contexts)) {
			return iter.hasNext();
		}
	}

	protected Scan scan(RDFSubject subj, RDFPredicate pred, RDFObject obj, RDFContext ctx, StatementIndices indices) {
		return HalyardTableUtils.scan(subj, pred, obj, ctx, indices);
	}

	@Override
	public final ValueFactory getValueFactory() {
		return valueReader.getValueFactory();
	}

	@Override
	public final void close() throws IOException {
		keyspaceConn.close();
	}

	@Override
	public String toString() {
		return super.toString() + "[keyspace = " + keyspaceConn.toString() + "]";
	}

	protected class StatementScanner extends AbstractStatementScanner {

		protected List<Resource> contextsList;
		protected Iterator<Resource> contexts;
		private ResultScanner rs = null;

		public StatementScanner(Resource subj, IRI pred, Value obj, List<Resource> contextsList, ValueIO.Reader reader) {
			super(reader, HBaseTripleSource.this.stmtIndices);
			this.subj = rdfFactory.createSubject(subj);
			this.pred = rdfFactory.createPredicate(pred);
			this.obj = rdfFactory.createObject(obj);
			this.contextsList = contextsList;
			this.contexts = contextsList.iterator();
			LOG.trace("New StatementScanner {} {} {} {}", subj, pred, obj, contextsList);
		}

		protected Result nextResult() throws IOException { // gets the next result to consider from the HBase Scan
			while (true) {
				if (rs == null) {
					if (contexts.hasNext()) {

						// build a ResultScanner from an HBase Scan that finds potential matches
						ctx = rdfFactory.createContext(contexts.next());
						Scan scan = scan(subj, pred, obj, ctx, stmtIndices);
						if (settings != null) {
							scan.setTimeRange(settings.minTimestamp, settings.maxTimestamp);
							scan.readVersions(settings.maxVersions);
						}
						rs = keyspaceConn.getScanner(scan);
					} else {
						return null;
					}
				}
				Result res = rs.next();
				if (ticker != null) {
					ticker.tick(); // sends a tick for keep alive purposes
				}
				if (res == null) { // no more results from this ResultScanner, close and clean up.
					rs.close();
					rs = null;
				} else {
					return res;
				}
			}
		}

		@Override
		protected void handleClose() throws IOException {
			if (rs != null) {
				rs.close();
				rs = null;
			}
		}
	}

	@Override
	public final CloseableIteration<? extends Triple, QueryEvaluationException> getRdfStarTriples(Resource subj, IRI pred, Value obj) throws QueryEvaluationException {
		CloseableIteration<? extends Triple, QueryEvaluationException> iter = new ConvertingIteration<Statement, Triple, QueryEvaluationException>(
				new ExceptionConvertingIteration<Statement, QueryEvaluationException>(createStatementScanner(subj, pred, obj, Collections.singletonList(HALYARD.TRIPLE_GRAPH_CONTEXT), valueReader)) {
				@Override
				protected QueryEvaluationException convert(Exception e) {
					return new QueryEvaluationException(e);
				}
			}) {
			final ValueFactory vf = valueReader.getValueFactory();

			@Override
			protected Triple convert(Statement stmt) {
				return vf.createTriple(stmt.getSubject(), stmt.getPredicate(), stmt.getObject());
			}
		};
		return timeLimit(iter, timeoutSecs);
	}

	private static <X, E extends Exception> CloseableIteration<X, E> timeLimit(CloseableIteration<X, E> iter, long timeoutSecs) {
		if (timeoutSecs > 0) {
			return new TimeLimitIteration<X, E>(iter, TimeUnit.SECONDS.toMillis(timeoutSecs)) {
				@Override
				protected void throwInterruptedException() {
					throw new QueryEvaluationException(String.format("Statements scanning exceeded specified timeout %ds", timeoutSecs));
				}
			};
		} else {
			return iter;
		}
	}
}
