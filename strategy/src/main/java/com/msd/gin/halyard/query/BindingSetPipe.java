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
package com.msd.gin.halyard.query;

import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;

/**
 * Binding set pipes instances hold {@BindingSet}s (set of evaluated, un-evaluated and intermediate variables) that
 * form part of the query evaluation (a query generates an evaluation tree).
 */
public abstract class BindingSetPipe {

    protected final BindingSetPipe parent;
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Create a pipe
     * @param parent the parent of this part of the evaluation chain
     */
    protected BindingSetPipe(BindingSetPipe parent) {
        this.parent = parent;
    }

    /**
     * Pushes BindingSet up the pipe, use pushLast() to indicate end of data. In case you need to interrupt the tree data flow
     * (when for example just a Slice of data is expected), it is necessary to indicate that no more data is expected down the tree
     * (to stop feeding this pipe) by returning false and
     * also to indicate up the tree that this is the end of data
     * (by calling pushLast() on the parent pipe in the evaluation tree).
     * Must be thread-safe.
     *
     * @param bs BindingSet
     * @return boolean indicating if more data are expected from the caller
     */
    public final boolean push(BindingSet bs) {
    	if (!closed.get()) {
        	return next(bs);
    	} else {
    		return false;
    	}
    }

    /**
     * 
     * @param bs BindingSet
     * @return boolean indicating if more data are expected from the caller
     */
    protected boolean next(BindingSet bs) {
    	if (parent != null) {
    		return parent.push(bs);
    	} else {
    		return true;
    	}
    }

    public final void close() {
    	if (closed.compareAndSet(false, true)) {
    		doClose();
    	}
    }

    protected void doClose() {
    	if (parent != null) {
    		parent.close();
    	}
    }

    public final void pushLast(BindingSet bs) {
    	push(bs);
    	close();
    }

    public boolean handleException(Throwable e) {
        if (parent != null) {
            return parent.handleException(e);
        } else if (e instanceof QueryEvaluationException) {
        	throw (QueryEvaluationException) e;
        } else {
        	throw new QueryEvaluationException(e);
        }
    }

    public final boolean isClosed() {
    	return closed.get();
    }
}
