package com.msd.gin.halyard.strategy;

import com.msd.gin.halyard.algebra.evaluation.ConstrainedTripleSourceFactory;
import com.msd.gin.halyard.common.ValueConstraint;
import com.msd.gin.halyard.optimizers.HalyardEvaluationStatistics;
import com.msd.gin.halyard.optimizers.SimpleStatementPatternCardinalityCalculator;

import org.apache.hadoop.conf.Configuration;
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
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.RDFStarTripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.memory.MemoryStoreConnection;
import org.junit.Assert;

class MemoryStoreWithConstraintsStrategy extends MemoryStore {

    @Override
    protected NotifyingSailConnection getConnectionInternal() throws SailException {
        return new MemoryStoreConnection(this) {

            @Override
            protected EvaluationStrategy getEvaluationStrategy(Dataset dataset, final TripleSource tripleSource) {
            	HalyardEvaluationStatistics stats = new HalyardEvaluationStatistics(SimpleStatementPatternCardinalityCalculator.FACTORY, null);
            	Configuration conf = new Configuration();
            	conf.setInt(StrategyConfig.HASH_JOIN_LIMIT, 0);
            	conf.setFloat(StrategyConfig.HASH_JOIN_COST_RATIO, Float.MAX_VALUE);
                HalyardEvaluationStrategy evalStrat = new HalyardEvaluationStrategy(conf, new MockTripleSource(tripleSource), dataset, null, stats);
                evalStrat.setOptimizerPipeline(new HalyardQueryOptimizerPipeline(evalStrat, tripleSource.getValueFactory(), stats));
                return evalStrat;
            }

        };
    }


    static class MockTripleSource implements RDFStarTripleSource, ConstrainedTripleSourceFactory {
        private final TripleSource tripleSource;

        MockTripleSource(TripleSource tripleSource) {
            this.tripleSource = tripleSource;
        }

        @Override
        public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
        	Assert.fail("Non-optimal strategy");
        	return null;
        }

        @Override
        public ValueFactory getValueFactory() {
            return tripleSource.getValueFactory();
        }

		@Override
		public CloseableIteration<? extends Triple, QueryEvaluationException> getRdfStarTriples(Resource subj, IRI pred, Value obj) throws QueryEvaluationException {
			return ((RDFStarTripleSource)tripleSource).getRdfStarTriples(subj, pred, obj);
		}

		@Override
		public TripleSource getTripleSource(ValueConstraint subjConstraint, ValueConstraint objConstraint) {
			return new TripleSource() {
		        @Override
		        public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
		            return new FilterIteration<Statement, QueryEvaluationException>(tripleSource.getStatements(subj, pred, obj, contexts)){
		                @Override
		                protected boolean accept(Statement stmt) throws QueryEvaluationException {
		                    return (subjConstraint == null || subjConstraint.test(stmt.getSubject())) && (objConstraint == null || objConstraint.test(stmt.getObject()));
		                }
		            };
		        }

		        @Override
		        public ValueFactory getValueFactory() {
		            return tripleSource.getValueFactory();
		        }
			};
		}
    }
}
