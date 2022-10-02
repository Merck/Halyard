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
package com.msd.gin.halyard.strategy;

import com.msd.gin.halyard.optimizers.HalyardEvaluationStatistics;
import com.msd.gin.halyard.optimizers.JoinAlgorithmOptimizer;
import com.msd.gin.halyard.optimizers.SimpleStatementPatternCardinalityCalculator;

import java.util.LinkedList;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.FilterIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.RDFStarTripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.memory.MemoryStoreConnection;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class MemoryStoreWithHalyardStrategy extends MemoryStore {

	private final LinkedList<TupleExpr> queryHistory = new LinkedList<>();
	private final int optHashJoinLimit;
	private final int evalHashJoinLimit;
	private final float cardinalityRatio;

	MemoryStoreWithHalyardStrategy() {
		this(0, 0, Float.MAX_VALUE);
	}

	MemoryStoreWithHalyardStrategy(int optHashJoinLimit, int evalHashJoinLimit, float cardinalityRatio) {
		this.optHashJoinLimit = optHashJoinLimit;
		this.evalHashJoinLimit = evalHashJoinLimit;
		this.cardinalityRatio = cardinalityRatio;
	}

	LinkedList<TupleExpr> getQueryHistory() {
		return queryHistory;
	}

	@Override
    protected NotifyingSailConnection getConnectionInternal() throws SailException {
        return new MemoryStoreConnection(this) {

            @Override
            protected EvaluationStrategy getEvaluationStrategy(Dataset dataset, final TripleSource tripleSource) {
            	HalyardEvaluationStatistics stats = new HalyardEvaluationStatistics(SimpleStatementPatternCardinalityCalculator.FACTORY, null);
            	HalyardEvaluationStrategy evalStrat = new HalyardEvaluationStrategy(new MockTripleSource(tripleSource), dataset, null, stats) {
            		@Override
            		public QueryEvaluationStep precompile(TupleExpr expr) {
            			queryHistory.add(expr);
            			return super.precompile(expr);
            		}
            		@Override
            		protected JoinAlgorithmOptimizer getJoinAlgorithmOptimizer() {
            			return new JoinAlgorithmOptimizer(stats, evalHashJoinLimit, cardinalityRatio);
            		}
            	};
                evalStrat.setOptimizerPipeline(new HalyardQueryOptimizerPipeline(evalStrat, tripleSource.getValueFactory(), stats, optHashJoinLimit, cardinalityRatio));
                evalStrat.setTrackResultSize(true);
                return evalStrat;
            }

        };
    }


    static class MockTripleSource implements RDFStarTripleSource {
        private final TripleSource tripleSource;

        MockTripleSource(TripleSource tripleSource) {
            this.tripleSource = tripleSource;
        }

        @Override
        public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
            return new FilterIteration<Statement, QueryEvaluationException>(tripleSource.getStatements(subj, pred, obj, contexts)){
                boolean first = true;

                @Override
                public boolean hasNext() throws QueryEvaluationException {
                    if (first) {
                        // emulate time taken to perform a HBase scan.
                        first = false;
                        try {
                            Thread.sleep(20);
                        } catch (InterruptedException ex) {
                            //ignore
                        }
                    }
                    return super.hasNext();
                }

                @Override
                protected boolean accept(Statement stmt) throws QueryEvaluationException {
                    return true;
                }
            };
        }

        @Override
        public ValueFactory getValueFactory() {
            return tripleSource.getValueFactory();
        }

		@Override
		public CloseableIteration<? extends Triple, QueryEvaluationException> getRdfStarTriples(Resource subj, IRI pred, Value obj) throws QueryEvaluationException {
			return ((RDFStarTripleSource)tripleSource).getRdfStarTriples(subj, pred, obj);
		}
    }
}
