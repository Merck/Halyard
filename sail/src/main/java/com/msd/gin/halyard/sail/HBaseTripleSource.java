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
import com.msd.gin.halyard.common.RDFContext;
import com.msd.gin.halyard.common.RDFObject;
import com.msd.gin.halyard.common.RDFPredicate;
import com.msd.gin.halyard.common.RDFSubject;
import com.msd.gin.halyard.strategy.TimeoutTracker;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTripleSource implements TripleSource {
	private static final Logger LOG = LoggerFactory.getLogger(HBaseTripleSource.class);

	private final Table table;
	private final ValueFactory vf;
	private final long startTime;
	private final long timeout;
	private final HBaseSail.ScanSettings settings;
	private final HBaseSail.Ticker ticker;

	public HBaseTripleSource(Table table, ValueFactory vf, long startTime, long timeout, HBaseSail.ScanSettings settings, HBaseSail.Ticker ticker) {
		this.table = table;
		this.vf = vf;
		this.startTime = startTime;
		this.timeout = timeout;
		this.settings = settings;
		this.ticker = ticker;
	}

	@Override
	public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
		return new StatementScanner(startTime, subj, pred, obj, contexts);
	}

	@Override
	public ValueFactory getValueFactory() {
		return vf;
	}

	protected class StatementScanner implements CloseableIteration<Statement, QueryEvaluationException> {

		private final RDFSubject subj;
		private final RDFPredicate pred;
		protected RDFObject obj;
		private RDFContext ctx;
		protected final List<Resource> contextsList;
		protected Iterator<Resource> contexts;
		private ResultScanner rs = null;
		private Statement next = null;
		private Iterator<Statement> iter = null;
		private final TimeoutTracker timeoutTracker;

		public StatementScanner(long startTime, Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
			this.subj = RDFSubject.create(subj);
			this.pred = RDFPredicate.create(pred);
			this.obj = RDFObject.create(obj);
			this.contextsList = Arrays.asList(normalizeContexts(contexts));
			this.contexts = contextsList.iterator();
			this.timeoutTracker = new TimeoutTracker(startTime, timeout);
			LOG.trace("New StatementScanner {} {} {} {}", subj, pred, obj, contextsList);
		}

		protected Result nextResult() throws IOException { // gets the next result to consider from the HBase Scan
			while (true) {
				if (rs == null) {
					if (contexts.hasNext()) {

						// build a ResultScanner from an HBase Scan that finds potential matches
						ctx = RDFContext.create(contexts.next());
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
		public void close() throws SailException {
			if (rs != null) {
				rs.close();
			}
		}

		@Override
		public synchronized boolean hasNext() throws SailException {
			if (timeoutTracker.checkTimeout()) {
				throw new SailException("Statements scanning exceeded specified timeout " + timeout + "s");
			}

			if (next == null) {
				try { // try and find the next result
					while (true) {
						if (iter == null) {
							Result res = nextResult();
							if (res == null) {
								return false; // no more Results
							} else {
								iter = HalyardTableUtils.parseStatements(subj, pred, obj, ctx, res, vf).iterator();
							}
						}
						while (iter.hasNext()) {
							Statement s = iter.next();
							next = s; // cache the next statement which will be returned with a call to next().
							return true; // there is another statement
						}
						iter = null;
					}
				} catch (IOException e) {
					throw new SailException(e);
				}
			} else {
				return true;
			}
		}

		@Override
		public synchronized Statement next() throws SailException {
			if (hasNext()) { // return the next statement and set next to null so it can be refilled by the next call to hasNext()
				Statement st = next;
				next = null;
				return st;
			} else {
				throw new NoSuchElementException();
			}
		}

		@Override
		public void remove() throws SailException {
			throw new UnsupportedOperationException();
		}
	}

	// generates a Resource[] from 0 or more Resources
	static Resource[] normalizeContexts(Resource... contexts) {
		if (contexts == null || contexts.length == 0) {
			return new Resource[] { null };
		} else {
			return contexts;
		}
	}
}
