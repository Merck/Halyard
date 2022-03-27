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
import com.msd.gin.halyard.common.IdentifiableValueIO;
import com.msd.gin.halyard.common.RDFObject;
import com.msd.gin.halyard.common.ValueIO;
import com.msd.gin.halyard.vocab.HALYARD;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.sail.SailException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseSearchTripleSource extends HBaseTripleSource {
	private static final Logger LOG = LoggerFactory.getLogger(HBaseSearchTripleSource.class);
	private static final int ELASTIC_RESULT_SIZE = 10000;

	private final String elasticSearchURL;

	public HBaseSearchTripleSource(Table table, ValueFactory vf, IdentifiableValueIO valueIO, long timeoutSecs, HBaseSail.ScanSettings settings, String elasticSearchURL, HBaseSail.Ticker ticker) {
		super(table, vf, valueIO, timeoutSecs, settings, ticker);
		this.elasticSearchURL = elasticSearchURL;
	}

	@Override
	protected CloseableIteration<? extends Statement, IOException> createStatementScanner(Resource subj, IRI pred, Value obj, List<Resource> contexts, ValueIO.Reader reader) throws QueryEvaluationException {
		if (obj != null && obj.isLiteral() && (HALYARD.SEARCH_TYPE.equals(((Literal) obj).getDatatype()))) {
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
			if (elasticSearchURL == null || elasticSearchURL.length() == 0) {
				throw new SailException("ElasticSearch Index URL is not properly configured.");
			}
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
								HttpURLConnection http = (HttpURLConnection) (new URL(elasticSearchURL + "/_search").openConnection());
								try {
									http.setRequestMethod("POST");
									http.setDoOutput(true);
									http.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
									http.connect();
									try (PrintStream out = new PrintStream(http.getOutputStream(), true, "UTF-8")) {
										out.print("{\"query\":{\"query_string\":{\"query\":" + JSONObject.quote(literalSearchQuery) + "}},\"size\":" + ELASTIC_RESULT_SIZE + "}");
									}
									int response = http.getResponseCode();
									String msg = http.getResponseMessage();
									if (response != 200) {
										LOG.info("ElasticSearch error response: " + msg + " on query: " + literalSearchQuery);
									} else {
										try (InputStreamReader isr = new InputStreamReader(http.getInputStream(), "UTF-8")) {
											JSONArray hits = new JSONObject(new JSONTokener(isr)).getJSONObject("hits").getJSONArray("hits");
											for (int i = 0; i < hits.length(); i++) {
												JSONObject source = hits.getJSONObject(i).getJSONObject("_source");
												if (source.has("lang")) {
													objList.add(RDFObject.create(vf.createLiteral(source.getString("label"), source.getString("lang")), valueIO));
												} else {
													objList.add(RDFObject.create(vf.createLiteral(source.getString("label"), vf.createIRI(source.getString("datatype"))), valueIO));
												}
											}
										}
									}
								} finally {
									http.disconnect();
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
