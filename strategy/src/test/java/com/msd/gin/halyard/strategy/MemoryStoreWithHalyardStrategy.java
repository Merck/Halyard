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

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.FilterIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.memory.MemoryStoreConnection;

/**
 *
 * @author Adam Sotona (MSD)
 */
class MemoryStoreWithHalyardStrategy extends MemoryStore {

    @Override
    protected NotifyingSailConnection getConnectionInternal() throws SailException {
        return new MemoryStoreConnection(this) {

            @Override
            protected EvaluationStrategy getEvaluationStrategy(Dataset dataset, final TripleSource tripleSource) {
                return new HalyardEvaluationStrategy(new TripleSource() {
                    @Override
                    public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
                        return new FilterIteration<Statement, QueryEvaluationException>(tripleSource.getStatements(subj, pred, obj, contexts)){
                            boolean first = true;

                            @Override
                            public boolean hasNext() throws QueryEvaluationException {
                                if (first) try {
                                    first = false;
                                    Thread.sleep(20);
                                } catch (InterruptedException ex) {
                                    //ignore
                                }
                                return super.hasNext(); //To change body of generated methods, choose Tools | Templates.
                            }

                            @Override
                            protected boolean accept(Statement object) throws QueryEvaluationException {
                                return true;
                            }
                        };
                    }

                    @Override
                    public ValueFactory getValueFactory() {
                        return tripleSource.getValueFactory();
                    }
                }, dataset, null, -1);
            }

        };
    }

}
