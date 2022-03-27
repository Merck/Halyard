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

import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.common.HalyardTableUtils.TableTripleReader;
import com.msd.gin.halyard.common.IdentifiableValueIO;
import com.msd.gin.halyard.common.RDFContext;
import com.msd.gin.halyard.common.RDFObject;
import com.msd.gin.halyard.common.RDFPredicate;
import com.msd.gin.halyard.common.RDFSubject;
import com.msd.gin.halyard.common.TimestampedValueFactory;
import com.msd.gin.halyard.common.ValueIO;
import com.msd.gin.halyard.vocab.HALYARD;

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
import org.apache.hadoop.hbase.client.Table;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTripleSource implements RDFStarTripleSource {
	private static final Logger LOG = LoggerFactory.getLogger(HBaseTripleSource.class);

	private final Table table;
	private final ValueFactory valueFactory;
	protected final IdentifiableValueIO valueIO;
	private final ValueIO.Reader valueReader;
	private final ValueIO.Reader tsValueReader;
	private final long timeoutSecs;
	private final HBaseSail.ScanSettings settings;
	private final HBaseSail.Ticker ticker;

	public HBaseTripleSource(Table table, ValueFactory vf, IdentifiableValueIO valueIO, long timeoutSecs) {
		this(table, vf, valueIO, timeoutSecs, null, null);
	}

	public HBaseTripleSource(Table table, ValueFactory vf, IdentifiableValueIO valueIO, long timeoutSecs, HBaseSail.ScanSettings settings, HBaseSail.Ticker ticker) {
		this.table = table;
		this.valueFactory = vf;
		this.valueIO = valueIO;
		TableTripleReader tf = new TableTripleReader(table);
		this.valueReader = valueIO.createReader(vf, tf);
		this.tsValueReader = valueIO.createReader(new TimestampedValueFactory(valueIO), tf);
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

	public CloseableIteration<? extends Statement, QueryEvaluationException> getTimestampedStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
		return getStatementsInternal(subj, pred, obj, contexts, tsValueReader);
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

	@Override
	public final ValueFactory getValueFactory() {
		return valueFactory;
	}

	protected class StatementScanner extends AbstractStatementScanner {

		protected List<Resource> contextsList;
		protected Iterator<Resource> contexts;
		private ResultScanner rs = null;

		public StatementScanner(Resource subj, IRI pred, Value obj, List<Resource> contextsList, ValueIO.Reader reader) {
			super(reader);
			this.subj = RDFSubject.create(subj, valueIO);
			this.pred = RDFPredicate.create(pred, valueIO);
			this.obj = RDFObject.create(obj, valueIO);
			this.contextsList = contextsList;
			this.contexts = contextsList.iterator();
			LOG.trace("New StatementScanner {} {} {} {}", subj, pred, obj, contextsList);
		}

		protected Result nextResult() throws IOException { // gets the next result to consider from the HBase Scan
			while (true) {
				if (rs == null) {
					if (contexts.hasNext()) {

						// build a ResultScanner from an HBase Scan that finds potential matches
						ctx = RDFContext.create(contexts.next(), valueIO);
						Scan scan = HalyardTableUtils.scan(subj, pred, obj, ctx);
						if (settings != null) {
							scan.setTimeRange(settings.minTimestamp, settings.maxTimestamp);
							scan.readVersions(settings.maxVersions);
						}
						rs = table.getScanner(scan);
					} else {
						return null;
					}
				}
				Result res = rs.next();
				if (ticker != null)
					ticker.tick(); // sends a tick for keep alive purposes
				if (res == null) { // no more results from this ResultScanner, close and clean up.
					rs.close();
					rs = null;
				} else {
					return res;
				}
			}
		}

		@Override
		public void close() throws IOException {
			if (rs != null) {
				rs.close();
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
			@Override
			protected Triple convert(Statement stmt) {
				return valueFactory.createTriple(stmt.getSubject(), stmt.getPredicate(), stmt.getObject());
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
