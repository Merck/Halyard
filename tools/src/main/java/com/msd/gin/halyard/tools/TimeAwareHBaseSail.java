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
package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.sail.HBaseSail;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.FilterIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.UpdateContext;

/**
 * @author Adam Sotona (MSD)
 */
public class TimeAwareHBaseSail extends HBaseSail {

    static final String TIMESTAMP_BINDING_NAME = "HALYARD_TIMESTAMP_SPECIAL_VARIABLE";
    static final String TIMESTAMP_CALLBACK_BINDING_NAME = "HALYARD_TIMESTAMP_SPECIAL_CALLBACK_BINDING";

    /**
     * TimestampCallbackBinding is a special binding implementation that allows to modify the value to propagate actually evaluated timestamp back to the global
     * bindings
     */
    public static class TimestampCallbackBinding implements Binding {

        private static final long serialVersionUID = 9149803395418843143L;

        Value v = SimpleValueFactory.getInstance().createLiteral(System.currentTimeMillis());

        @Override
        public String getName() {
            return TIMESTAMP_CALLBACK_BINDING_NAME;
        }

        @Override
        public Value getValue() {
            return v;
        }

    }
    public TimeAwareHBaseSail(Configuration config, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, String elasticIndexURL, Ticker ticker) {
        super(config, tableName, create, splitBits, pushStrategy, evaluationTimeout, elasticIndexURL, ticker);
    }

    private static long longValueOfTimeStamp(Literal ts) {
        if (XMLDatatypeUtil.isCalendarDatatype(ts.getDatatype())) {
            return ts.calendarValue().toGregorianCalendar().getTimeInMillis();
        }
        return ts.longValue();
    }

    @Override
    public void addStatement(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
        BindingSet bs;
        Binding b;
        Value v;
        long timestamp = (op != null && (bs = op.getBindingSet()) != null && (b = bs.getBinding(TIMESTAMP_CALLBACK_BINDING_NAME)) != null) ? longValueOfTimeStamp((Literal) b.getValue()) : System.currentTimeMillis();
        for (Resource ctx : normalizeContexts(contexts)) {
            addStatementInternal(subj, pred, obj, ctx, timestamp);
        }
    }

    @Override
    public void removeStatement(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
        BindingSet bs;
        Binding b;
        Value v;
        long timestamp = (op != null && (bs = op.getBindingSet()) != null && (b = bs.getBinding(TIMESTAMP_CALLBACK_BINDING_NAME)) != null) ? longValueOfTimeStamp((Literal) b.getValue()) : System.currentTimeMillis();
        try {
            for (Resource ctx : normalizeContexts(contexts)) {
                for (KeyValue kv : HalyardTableUtils.toKeyValues(subj, pred, obj, ctx, true, timestamp)) { //calculate the kv's corresponding to the quad (or triple)
                    delete(kv);
                }
            }
        } catch (IOException e) {
            throw new SailException(e);
        }
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred) throws SailException {
        Binding b = bindings.getBinding(TIMESTAMP_CALLBACK_BINDING_NAME);
        final TimestampCallbackBinding timestampBinding = (b instanceof TimestampCallbackBinding) ? (TimestampCallbackBinding) b : null;
        CloseableIteration<BindingSet, QueryEvaluationException> iter = super.evaluate(tupleExpr, dataset, bindings, includeInferred);
        //push back the actual timestamp binding to the callback binding if requested
        if (timestampBinding != null) {
            iter = new FilterIteration<BindingSet, QueryEvaluationException>(iter) {
                @Override
                protected boolean accept(BindingSet bindings) throws QueryEvaluationException {
                    Binding b = bindings.getBinding(TIMESTAMP_BINDING_NAME);
                    //push back actual time if the timestamp binding is not provided
                    timestampBinding.v = b == null ? SimpleValueFactory.getInstance().createLiteral(System.currentTimeMillis()) : b.getValue();
                    return true;
                }
            };
        }
        return iter;
    }
}
