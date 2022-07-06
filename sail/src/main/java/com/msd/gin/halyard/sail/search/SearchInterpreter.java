package com.msd.gin.halyard.sail.search;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.msd.gin.halyard.common.InternalObjectLiteral;
import com.msd.gin.halyard.vocab.HALYARD;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.EmptySet;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.TupleFunctionCall;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.BGPCollector;

/**
 * [] a halyard:Query; halyard:query 'what'; halyard:limit 5; halyard:matches [rdf:value ?v; halyard:score ?score; halyard:index ?index]
 */
public class SearchInterpreter implements QueryOptimizer {

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		tupleExpr.visit(new SearchScanner());
	}

	static final class SearchScanner extends AbstractQueryModelVisitor<RuntimeException> {
		private void processGraphPattern(BGPCollector<RuntimeException> bgp) {
			ListMultimap<String, StatementPattern> stmtsBySubj = Multimaps.newListMultimap(new HashMap<>(), () -> new ArrayList<>(8));
			Map<Var, SearchCall> searchCallsBySubj = new HashMap<>();
			for (StatementPattern sp : bgp.getStatementPatterns()) {
				Var subjVar = sp.getSubjectVar();
				Var predVar = sp.getPredicateVar();
				Var objVar = sp.getObjectVar();
				if (RDF.TYPE.equals((IRI) predVar.getValue()) && HALYARD.QUERY_CLASS.equals(objVar.getValue())) {
					SearchCall searchCall = new SearchCall();
					searchCallsBySubj.put(subjVar, searchCall);
					sp.replaceWith(searchCall.tfc);
				} else {
					stmtsBySubj.put(subjVar.getName(), sp);
				}
			}

			for (Map.Entry<Var, SearchCall> entry : searchCallsBySubj.entrySet()) {
				String searchVarName = entry.getKey().getName();
				SearchCall searchCall = entry.getValue();
				List<StatementPattern> sps = stmtsBySubj.get(searchVarName);
				if (sps != null) {
					for (StatementPattern querySP : sps) {
						IRI queryPred = (IRI) querySP.getPredicateVar().getValue();
						Var queryObjVar = querySP.getObjectVar();
						if (HALYARD.QUERY_PROPERTY.equals(queryPred)) {
							searchCall.params.setQueryVar(queryObjVar);
							querySP.replaceWith(new SingletonSet());
						} else if (HALYARD.LIMIT_PROPERTY.equals(queryPred)) {
							searchCall.params.setLimitVar(queryObjVar);
							querySP.replaceWith(new SingletonSet());
						} else if (HALYARD.MATCHES_PROPERTY.equals(queryPred)) {
							SearchParams.MatchParams matchParams = searchCall.params.newMatchParams(!queryObjVar.isAnonymous() ? queryObjVar : null);
							querySP.replaceWith(new SingletonSet());
							for (StatementPattern matchSP : stmtsBySubj.get(queryObjVar.getName())) {
								IRI matchPred = (IRI) matchSP.getPredicateVar().getValue();
								Var matchObjVar = matchSP.getObjectVar();
								if (!matchObjVar.isAnonymous()) {
									if (RDF.VALUE.equals(matchPred)) {
										matchParams.valueVars.add(matchObjVar);
										matchSP.replaceWith(new SingletonSet());
									} else if (HALYARD.SCORE_PROPERTY.equals(matchPred)) {
										matchParams.scoreVars.add(matchObjVar);
										matchSP.replaceWith(new SingletonSet());
									} else if (HALYARD.INDEX_PROPERTY.equals(matchPred)) {
										matchParams.indexVars.add(matchObjVar);
										matchSP.replaceWith(new SingletonSet());
									}
								}
							}
						}
					}
				}
			}

			for (SearchCall searchCall : searchCallsBySubj.values()) {
				if (!searchCall.initCall()) { // if invalid
					searchCall.tfc.replaceWith(new EmptySet());
				}
			}
		}

		@Override
		public void meet(Join join) {
			BGPCollector<RuntimeException> collector = new BGPCollector<>(this);
			join.visit(collector);
			processGraphPattern(collector);
		}
	}


	static final class SearchCall {
		static final ValueFactory VF = SimpleValueFactory.getInstance();
		final TupleFunctionCall tfc = new TupleFunctionCall();
		final SearchParams params = new SearchParams();

		SearchCall() {
			tfc.setURI(HALYARD.SEARCH.stringValue());
		}

		boolean initCall() {
			if (params.invalid) {
				return false;
			}
			tfc.addArg(params.queryVar != null ? params.queryVar : new ValueConstant(VF.createLiteral("")));
			tfc.addArg(params.limitVar != null ? params.limitVar : new ValueConstant(VF.createLiteral(SearchClient.MAX_RESULT_SIZE)));
			tfc.addArg(new ValueConstant(new InternalObjectLiteral<>(params)));
			for (SearchParams.MatchParams matchParams : params.matches) {
				if (matchParams.matchVar != null) {
					tfc.addResultVar(matchParams.matchVar);
				}
				for (Var valueVar : matchParams.valueVars) {
					tfc.addResultVar(valueVar);
				}
				for (Var scoreVar : matchParams.scoreVars) {
					tfc.addResultVar(scoreVar);
				}
				for (Var indexVar : matchParams.indexVars) {
					tfc.addResultVar(indexVar);
				}
			}
			return true;
		}
	}

	static final class SearchParams {
		ValueExpr queryVar;
		ValueExpr limitVar;
		final List<MatchParams> matches = new ArrayList<>(1);
		boolean invalid;

		void setQueryVar(Var var) {
			if (queryVar == null) {
				queryVar = var;
			} else {
				invalid = true;
			}
		}

		void setLimitVar(Var var) {
			if (limitVar == null) {
				limitVar = var;
			} else {
				invalid = true;
			}
		}

		MatchParams newMatchParams(Var var) {
			MatchParams matchParams = new MatchParams(var);
			matches.add(matchParams);
			return matchParams;
		}


		static final class MatchParams {
			final Var matchVar;
			final List<Var> valueVars = new ArrayList<>(1);
			final List<Var> scoreVars = new ArrayList<>(1);
			final List<Var> indexVars = new ArrayList<>(1);

			MatchParams(Var var) {
				this.matchVar = var;
			}
		}
	}
}
