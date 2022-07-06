package com.msd.gin.halyard.sail.search;

import com.google.common.collect.Lists;
import com.msd.gin.halyard.common.ObjectLiteral;
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
		if (args.length != 3) {
			throw new QueryEvaluationException("Missing arguments");
		}

		if (!args[0].isLiteral()) {
			throw new QueryEvaluationException("Invalid query value");
		}
		String query = ((Literal) args[0]).getLabel();
		int queryHash = query.hashCode();
		int limit = ((Literal) args[1]).intValue();
		SearchInterpreter.SearchParams searchParams = ((ObjectLiteral<SearchInterpreter.SearchParams>) args[2]).objectValue();
		SearchClient searchClient = (SearchClient) QueryContext.getQueryContext().getAttribute(HBaseSailConnection.QUERY_CONTEXT_SEARCH_ATTRIBUTE);
		try {
			SearchResponse<SearchDocument> searchResults = searchClient.search(query, limit);
			List<List<Hit<SearchDocument>>> results;
			final int numMatchValues = searchParams.matches.size();
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
						SearchInterpreter.SearchParams.MatchParams matchParams = searchParams.matches.get(i);
						if (matchParams.matchVar != null) {
							String bnodeId = "es" + queryHash + "_" + matchValue.index() + "_" + matchValue.id();
							values.add(valueFactory.createBNode(bnodeId));
						}
						if (!matchParams.valueVars.isEmpty()) {
							Literal value = matchValue.source().createLiteral(valueFactory);
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
