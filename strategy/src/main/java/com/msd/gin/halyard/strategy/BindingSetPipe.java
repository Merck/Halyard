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

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;

/**
 * Binding set pipes instances hold {@BindingSet}s (set of evaluated, un-evaluated and intermediate variables) that
 * form part of the query evaluation (a query generates an evaluation tree).
 */
abstract class BindingSetPipe {

    protected final BindingSetPipe parent;

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
     * @throws InterruptedException
     * @throws QueryEvaluationException
     */
    public final boolean push(BindingSet bs) throws InterruptedException {
    	if (!isClosed()) {
        	return next(bs);
    	} else {
    		return false;
    	}
    }

    protected boolean next(BindingSet bs) throws InterruptedException {
    	return parent.push(bs);
    }

    public void close() throws InterruptedException {
    	parent.close();
    }

    public final boolean pushLast(BindingSet bs) throws InterruptedException {
    	if (push(bs)) {
    		// push() returned true indicating more BindingSets are expected
    		// so need to call close() to indicate that there are no more
    		close();
    	}
    	return false;
    }

    protected boolean handleException(Throwable e) {
        if (parent != null) {
            parent.handleException(e);
        }
        return false;
    }

    protected boolean isClosed() {
        if (parent != null) {
            return parent.isClosed();
        } else {
            return false;
        }
    }

    public final void empty() {
        try {
            close();
        } catch (InterruptedException e) {
            handleException(e);
        }
    }
}
