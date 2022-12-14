package com.msd.gin.halyard.sail.search;

import com.google.common.collect.Lists;
import com.msd.gin.halyard.common.ObjectLiteral;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.StatementIndices;
import com.msd.gin.halyard.sail.HBaseSailConnection;
import com.msd.gin.halyard.vocab.HALYARD;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.CloseableIteratorIteration;
import org.eclipse.rdf4j.common.iteration.ConvertingIteration;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryContext;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;
import org.kohsuke.MetaInfServices;

import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;

@MetaInfServices(TupleFunction.class)
public class SearchTupleFunction implements TupleFunction {

	@Override
	public String getURI() {
		return HALYARD.SEARCH.stringValue();
	}

	@Override
	public CloseableIteration<? extends List<? extends Value>, QueryEvaluationException> evaluate(ValueFactory valueFactory, Value... args) throws QueryEvaluationException {
		if (args.length != 6) {
			throw new QueryEvaluationException("Missing arguments");
		}

		if (!args[0].isLiteral()) {
			throw new QueryEvaluationException("Invalid query value");
		}
		int argPos = 0;
		String query = ((Literal) args[argPos++]).getLabel();
		int queryHash = query.hashCode();
		int limit = ((Literal) args[argPos++]).intValue();
		double minScore = ((Literal) args[argPos++]).doubleValue();
		int fuzziness = ((Literal) args[argPos++]).intValue();
		int phraseSlop = ((Literal) args[argPos++]).intValue();
		List<SearchInterpreter.SearchParams.MatchParams> matches = ((ObjectLiteral<List<SearchInterpreter.SearchParams.MatchParams>>) args[argPos++]).objectValue();
		StatementIndices indices = (StatementIndices) QueryContext.getQueryContext().getAttribute(HBaseSailConnection.QUERY_CONTEXT_INDICES_ATTRIBUTE);
		RDFFactory rdfFactory = indices.getRDFFactory();
		SearchClient searchClient = (SearchClient) QueryContext.getQueryContext().getAttribute(HBaseSailConnection.QUERY_CONTEXT_SEARCH_ATTRIBUTE);
		if (searchClient == null) {
			throw new QueryEvaluationException("Search index not configured");
		}

		try {
			SearchResponse<SearchDocument> searchResults = searchClient.search(query, limit, minScore, fuzziness, phraseSlop);
			List<List<Hit<SearchDocument>>> results;
			final int numMatchValues = matches.size();
			if (numMatchValues == 1) {
				results = Lists.transform(searchResults.hits().hits(), doc -> Collections.singletonList(doc));
			} else {
				results = Lists.cartesianProduct(Collections.nCopies(numMatchValues, searchResults.hits().hits()));
			}
			return new ConvertingIteration<List<Hit<SearchDocument>>, List<Value>, QueryEvaluationException>(
					new CloseableIteratorIteration<List<Hit<SearchDocument>>, QueryEvaluationException>(results.iterator())) {
				int outputSize = 2;
				@Override
				protected List<Value> convert(List<Hit<SearchDocument>> matchValues) throws QueryEvaluationException {
					List<Value> values = new ArrayList<>(outputSize);
					for (int i = 0; i < numMatchValues; i++) {
						Hit<SearchDocument> matchValue = matchValues.get(i);
						SearchInterpreter.SearchParams.MatchParams matchParams = matches.get(i);
						if (matchParams.matchVar != null) {
							String bnodeId = "es" + queryHash + "_" + matchValue.index() + "_" + matchValue.id();
							values.add(valueFactory.createBNode(bnodeId));
						}
						if (!matchParams.valueVars.isEmpty()) {
							Literal value = matchValue.source().createLiteral(valueFactory, rdfFactory);
							for (int k = 0; k < matchParams.valueVars.size(); k++) {
								values.add(value);
							}
						}
						if (!matchParams.scoreVars.isEmpty()) {
							Literal score = valueFactory.createLiteral(matchValue.score());
							for (int k = 0; k < matchParams.scoreVars.size(); k++) {
								values.add(score);
							}
						}
						if (!matchParams.indexVars.isEmpty()) {
							Literal index = valueFactory.createLiteral(matchValue.index());
							for (int k = 0; k < matchParams.indexVars.size(); k++) {
								values.add(index);
							}
						}
					}
					outputSize = values.size();
					return values;
				}
			};
		} catch (IOException e) {
			throw new QueryEvaluationException(e);
		}
	}
}
