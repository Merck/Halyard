/*
 * Copyright 2018 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
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

import com.msd.gin.halyard.common.Hashes;
import com.msd.gin.halyard.optimizers.HalyardEvaluationStatistics;
import com.msd.gin.halyard.vocab.HALYARD;
import com.msd.gin.halyard.vocab.VOID_EXT;

import java.util.Collection;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Adam Sotona (MSD)
 */
public final class HalyardStatsBasedStatementPatternCardinalityCalculator implements HalyardEvaluationStatistics.StatementPatternCardinalityCalculator {
	private static final Logger LOG = LoggerFactory.getLogger(HalyardStatsBasedStatementPatternCardinalityCalculator.class);

	private final TripleSource statsSource;

	public HalyardStatsBasedStatementPatternCardinalityCalculator(TripleSource statsSource) {
		this.statsSource = statsSource;
    }

    @Override
    public Double getCardinality(StatementPattern sp, Collection<String> boundVars) { //get the cardinality of the Statement form VOID statistics
        final Var contextVar = sp.getContextVar();
        final IRI graphNode = contextVar == null || !contextVar.hasValue() ? HALYARD.STATS_ROOT_NODE : (IRI) contextVar.getValue();
        final long triples = getTriplesCount(graphNode, -1l);
        if (triples > 0) { //stats are present
            final double card;
            boolean sv = hasValue(sp.getSubjectVar(), boundVars);
            boolean pv = hasValue(sp.getPredicateVar(), boundVars);
            boolean ov = hasValue(sp.getObjectVar(), boundVars);
            long defaultCardinality = Math.round(Math.sqrt(triples));
            if (sv) {
                if (pv) {
                    if (ov) {
                        card = 1.0;
                    } else {
                        card = (double) subsetTriplesPart(graphNode, VOID_EXT.SUBJECT, sp.getSubjectVar(), defaultCardinality) * subsetTriplesPart(graphNode, VOID.PROPERTY, sp.getPredicateVar(), defaultCardinality) / triples;
                    }
                } else if (ov) {
                    card = (double) subsetTriplesPart(graphNode, VOID_EXT.SUBJECT, sp.getSubjectVar(), defaultCardinality) * subsetTriplesPart(graphNode, VOID_EXT.OBJECT, sp.getObjectVar(), defaultCardinality) / triples;
                } else {
                    card = subsetTriplesPart(graphNode, VOID_EXT.SUBJECT, sp.getSubjectVar(), defaultCardinality);
                }
            } else if (pv) {
                if (ov) {
                    card = (double) subsetTriplesPart(graphNode, VOID.PROPERTY, sp.getPredicateVar(), defaultCardinality) * subsetTriplesPart(graphNode, VOID_EXT.OBJECT, sp.getObjectVar(), defaultCardinality) / triples;
                } else {
                    card = subsetTriplesPart(graphNode, VOID.PROPERTY, sp.getPredicateVar(), defaultCardinality);
                }
            } else if (ov) {
                card = subsetTriplesPart(graphNode, VOID_EXT.OBJECT, sp.getObjectVar(), defaultCardinality);
            } else {
                card = triples;
            }
			LOG.debug("cardinality of {} = {}", sp, card);
            return card;
        } else { // stats are not present
            return null;
        }
    }

    //get the Triples count for a giving subject from VOID statistics or return the default value
    private long getTriplesCount(IRI subjectNode, long defaultValue) {
		try (CloseableIteration<? extends Statement, QueryEvaluationException> ci = statsSource.getStatements(subjectNode, VOID.TRIPLES, null, HALYARD.STATS_GRAPH_CONTEXT)) {
            if (ci.hasNext()) {
                Value v = ci.next().getObject();
                if (v instanceof Literal) {
                    try {
                        long l = ((Literal) v).longValue();
						LOG.trace("triple stats for {} = {}", subjectNode, l);
                        return l;
                    } catch (NumberFormatException ignore) {
                    }
                }
				LOG.warn("Invalid statistics for: {}", subjectNode);
            }
        }
		LOG.trace("triple stats for {} are not available", subjectNode);
        return defaultValue;
    }

    private boolean hasValue(Var partitionVar, Collection<String> boundVars) {
        return partitionVar == null || partitionVar.hasValue() || boundVars.contains(partitionVar.getName());
    }

    //calculate a multiplier for the triple count for this sub-part of the graph
    private long subsetTriplesPart(IRI graph, IRI partitionType, Var partitionVar, long defaultCardinality) {
        if (partitionVar == null || !partitionVar.hasValue()) {
            return defaultCardinality;
        }
		return getTriplesCount(statsSource.getValueFactory().createIRI(graph.stringValue() + "_" + partitionType.getLocalName() + "_" + Hashes.encode(Hashes.id(partitionVar.getValue()))), 100l);
    }
}
