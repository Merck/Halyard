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
package com.msd.gin.halyard.strategy;

import java.util.Set;
import static junit.framework.TestCase.*;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.EmptySet;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.junit.Test;

/**
 * @author Adam Sotona (MSD)
 */
public class HalyardStrategyServiceTest {

    @Test
    public void testGetService() {
        assertNull(new HalyardEvaluationStrategy(getTripleSource(), null, new FederatedServiceResolver() {
            @Override
            public FederatedService getService(String serviceUrl) throws QueryEvaluationException {
                return null;
            }
        }, 0).getService(null));
    }

    @Test
    public void testServiceEvaluate() {
        CloseableIteration<BindingSet, QueryEvaluationException> bindings1 = new EmptyIteration<>();
        CloseableIteration<BindingSet, QueryEvaluationException> bindings2 = new EmptyIteration<>();
        assertSame(bindings1, new HalyardEvaluationStrategy(getTripleSource(), null, getFederatedServiceResolver(bindings1), 0).evaluate(new Service(null, new EmptySet(), "", null, null, false), null, bindings2));
    }

    @Test (expected = QueryEvaluationException.class)
    public void testServiceEvaluateFail() {
        CloseableIteration<BindingSet, QueryEvaluationException> bindings = new EmptyIteration<>();
        new HalyardEvaluationStrategy(getTripleSource(), null, getFederatedServiceResolver(null), 0).evaluate(new Service(null, new EmptySet(), "", null, null, false), null, bindings);
    }

    @Test
    public void testServiceEvaluateFailSilent() {
        CloseableIteration<BindingSet, QueryEvaluationException> bindings = new EmptyIteration<>();
        assertSame(bindings, new HalyardEvaluationStrategy(getTripleSource(), null, getFederatedServiceResolver(null), 0).evaluate(new Service(null, new EmptySet(), "", null, null, true), null, bindings));
    }

    @Test (expected = QueryEvaluationException.class)
    public void testNoServiceEvaluateFail() {
        CloseableIteration<BindingSet, QueryEvaluationException> bindings = new EmptyIteration<>();
        new HalyardEvaluationStrategy(getTripleSource(), null, null, 0).evaluate(new Service(null, new EmptySet(), "", null, null, false), null, bindings);
    }

    @Test
    public void testServiceEvaluate2() {
        CloseableIteration<BindingSet, QueryEvaluationException> bindings = new EmptyIteration<>();
        try (CloseableIteration<BindingSet, QueryEvaluationException> res = new HalyardEvaluationStrategy(getTripleSource(), null, getFederatedServiceResolver(bindings), 0)
            .evaluate(new Service(new Var("service", SimpleValueFactory.getInstance().createLiteral("http://whatever")), new EmptySet(), "", null, null, true), new MapBindingSet())) {
            assertTrue(res.hasNext());
            BindingSet resB = res.next();
            Value val = resB.getValue("service");
            assertNotNull(val);
            assertEquals("http://whatever", val.stringValue());
            assertFalse(res.hasNext());
        }
    }

    @Test
    public void testNoServiceEvaluateFailSilent() {
        CloseableIteration<BindingSet, QueryEvaluationException> bindings = new EmptyIteration<>();
        assertSame(bindings, new HalyardEvaluationStrategy(getTripleSource(), null, null, 0).evaluate(new Service(null, new EmptySet(), "", null, null, true), null, bindings));
    }

    private TripleSource getTripleSource() {
        return new TripleSource() {
            @Override
            public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
                throw new QueryEvaluationException();
            }

            @Override
            public ValueFactory getValueFactory() {
                return SimpleValueFactory.getInstance();
            }
        };
    }

    private FederatedServiceResolver getFederatedServiceResolver(final CloseableIteration<BindingSet, QueryEvaluationException> defaultBindings) {
        return new FederatedServiceResolver() {
            @Override
            public FederatedService getService(String serviceUrl) throws QueryEvaluationException {
                return new FederatedService() {
                    @Override
                    public boolean ask(Service service, BindingSet bindings, String baseUri) throws QueryEvaluationException {
                        throw new QueryEvaluationException();
                    }
                    @Override
                    public CloseableIteration<BindingSet, QueryEvaluationException> select(Service service, Set<String> projectionVars, BindingSet bindings, String baseUri) throws QueryEvaluationException {
                        throw new QueryEvaluationException();
                    }
                    @Override
                    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Service service, CloseableIteration<BindingSet, QueryEvaluationException> bindings, String baseUri) throws QueryEvaluationException {
                        if (defaultBindings != null) return defaultBindings;
                        else throw new QueryEvaluationException();
                    }
                    @Override
                    public boolean isInitialized() {
                        return false;
                    }
                    @Override
                    public void initialize() throws QueryEvaluationException {
                        throw new QueryEvaluationException();
                    }
                    @Override
                    public void shutdown() throws QueryEvaluationException {
                        throw new QueryEvaluationException();
                    }
                };
            }
        };
    }
}
