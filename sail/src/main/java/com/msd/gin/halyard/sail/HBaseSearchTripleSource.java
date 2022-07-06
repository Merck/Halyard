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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.msd.gin.halyard.common.KeyspaceConnection;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.RDFObject;
import com.msd.gin.halyard.common.ValueIO;
import com.msd.gin.halyard.sail.search.SearchDocument;
import com.msd.gin.halyard.sail.search.SearchClient;
import com.msd.gin.halyard.vocab.HALYARD;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.client.Result;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.sail.SailException;

import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;

public class HBaseSearchTripleSource extends HBaseTripleSource {
	private final SearchClient searchClient;

	public HBaseSearchTripleSource(KeyspaceConnection table, ValueFactory vf, RDFFactory rdfFactory, long timeoutSecs, HBaseSail.ScanSettings settings, SearchClient searchClient, HBaseSail.Ticker ticker) {
		super(table, vf, rdfFactory, timeoutSecs, settings, ticker);
		this.searchClient = searchClient;
	}

	@Override
	protected CloseableIteration<? extends Statement, IOException> createStatementScanner(Resource subj, IRI pred, Value obj, List<Resource> contexts, ValueIO.Reader reader) throws QueryEvaluationException {
		if (obj != null && obj.isLiteral() && (HALYARD.SEARCH.equals(((Literal) obj).getDatatype()))) {
			if (searchClient == null) {
				throw new QueryEvaluationException("Search index not configured");
			}
			return new LiteralSearchStatementScanner(subj, pred, obj.stringValue(), contexts, reader);
		} else {
			return super.createStatementScanner(subj, pred, obj, contexts, reader);
		}
	}

	private static final Cache<String, List<RDFObject>> SEARCH_CACHE = CacheBuilder.newBuilder().maximumSize(25).expireAfterAccess(1, TimeUnit.MINUTES).build();

	// Scans the Halyard table for statements that match the specified pattern
	private class LiteralSearchStatementScanner extends StatementScanner {

		Iterator<RDFObject> objects = null;
		private final String literalSearchQuery;

		public LiteralSearchStatementScanner(Resource subj, IRI pred, String literalSearchQuery, List<Resource> contexts, ValueIO.Reader reader) throws SailException {
			super(subj, pred, null, contexts, reader);
			this.literalSearchQuery = literalSearchQuery;
		}

		@Override
		protected Result nextResult() throws IOException {
			while (true) {
				if (obj == null) {
					if (objects == null) { // perform ES query and parse results
						try {
							List<RDFObject> objectList = SEARCH_CACHE.get(literalSearchQuery, () -> {
								ArrayList<RDFObject> objList = new ArrayList<>();
								SearchResponse<SearchDocument> response = searchClient.search(literalSearchQuery, SearchClient.MAX_RESULT_SIZE);
								for (Hit<SearchDocument> hit : response.hits().hits()) {
									SearchDocument source = hit.source();
									Literal literal = source.createLiteral(vf);
									objList.add(rdfFactory.createObject(literal));
								}
								objList.trimToSize();
								return objList;
							});
							objects = objectList.iterator();
						} catch (ExecutionException ex) {
							throw new IOException(ex.getCause());
						}
					}
					if (objects.hasNext()) {
						obj = objects.next();
					} else {
						return null;
					}
					contexts = contextsList.iterator(); // reset iterator over contexts
				}
				Result res = super.nextResult();
				if (res == null) {
					obj = null;
				} else {
					return res;
				}
			}
		}
	}
}
