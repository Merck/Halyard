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

import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.optimizers.SimpleStatementPatternCardinalityCalculator;
import com.msd.gin.halyard.vocab.HALYARD;
import com.msd.gin.halyard.vocab.VOID_EXT;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

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
public final class HalyardStatsBasedStatementPatternCardinalityCalculator extends SimpleStatementPatternCardinalityCalculator {
	private static final Logger LOG = LoggerFactory.getLogger(HalyardStatsBasedStatementPatternCardinalityCalculator.class);

	private final TripleSource statsSource;
	private final RDFFactory rdfFactory;
	private final Map<IRI, Long> tripleCountCache = new HashMap<>(64);

	public HalyardStatsBasedStatementPatternCardinalityCalculator(TripleSource statsSource, RDFFactory rdfFactory) {
		this.statsSource = statsSource;
		this.rdfFactory = rdfFactory;
    }

    @Override
	public double getCardinality(StatementPattern sp, Collection<String> boundVars) { // get the cardinality of the Statement form VOID statistics
		final double card;
        final Var contextVar = sp.getContextVar();
        final IRI graphNode = contextVar == null || !contextVar.hasValue() ? HALYARD.STATS_ROOT_NODE : (IRI) contextVar.getValue();
        final long triples = getTriplesCount(graphNode, -1l);
        if (triples > 0) { //stats are present
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
			LOG.debug("cardinality of {} = {} (sampled)", sp, card);
        } else { // stats are not present
			card = super.getCardinality(sp, boundVars);
			LOG.debug("cardinality of {} = {} (preset)", sp, card);
        }
		return card;
    }

    //get the Triples count for a giving subject from VOID statistics or return the default value
    private long getTriplesCount(IRI subjectNode, long defaultValue) {
		return tripleCountCache.computeIfAbsent(subjectNode, node -> {
			try (CloseableIteration<? extends Statement, QueryEvaluationException> ci = statsSource.getStatements(node, VOID.TRIPLES, null, HALYARD.STATS_GRAPH_CONTEXT)) {
				if (ci.hasNext()) {
					Value v = ci.next().getObject();
					if (v.isLiteral()) {
						try {
							long l = ((Literal) v).longValue();
							LOG.trace("triple stats for {} = {}", node, l);
							return l;
						} catch (NumberFormatException ignore) {
							LOG.warn("Invalid statistics for {}: {}", node, v, ignore);
						}
					}
					LOG.warn("Invalid statistics for {}: {}", node, v);
				}
			}
			LOG.trace("triple stats for {} are not available", node);
			return defaultValue;
		});
    }

    //calculate a multiplier for the triple count for this sub-part of the graph
    private long subsetTriplesPart(IRI graph, IRI partitionType, Var partitionVar, long defaultCardinality) {
        if (partitionVar == null || !partitionVar.hasValue()) {
            return defaultCardinality;
        }
		return getTriplesCount(statsSource.getValueFactory().createIRI(graph.stringValue() + "_" + partitionType.getLocalName() + "_" + rdfFactory.id(partitionVar.getValue())), defaultCardinality);
    }

	@Override
	public void close() throws IOException {
		if (statsSource instanceof Closeable) {
			((Closeable) statsSource).close();
		}
	}
}
