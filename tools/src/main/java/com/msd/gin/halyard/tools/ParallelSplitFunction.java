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
package com.msd.gin.halyard.tools;

import java.util.Arrays;
import java.util.List;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.UpdateExpr;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import static com.msd.gin.halyard.sail.HALYARD.PARALLEL_SPLIT_FUNCTION;

/**
 *
 * @author Adam Sotona (MSD)
 */
public final class ParallelSplitFunction implements Function {

    private final int forkIndex;

    public ParallelSplitFunction(int forkIndex) {
        this.forkIndex = forkIndex;
    }

    @Override
    public String getURI() {
        return PARALLEL_SPLIT_FUNCTION.toString();
    }

    @Override
    public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
        if (args == null || args.length < 2) {
            throw new ValueExprEvaluationException(PARALLEL_SPLIT_FUNCTION.getLocalName() + " function has at least two mandatory arguments: <constant number of parallel forks> and <binding variable(s) to filter by>");
        }
        for (Value v : args) {
            if (v == null) {
                throw new ValueExprEvaluationException(PARALLEL_SPLIT_FUNCTION.getLocalName() + " function does not allow null values");
            }
        }
        try {
            int forks = Integer.parseInt(args[0].stringValue());
            if (forks < 1) {
                throw new ValueExprEvaluationException(PARALLEL_SPLIT_FUNCTION.getLocalName() + " function first argument must be > 0");
            }
            return valueFactory.createLiteral(Math.floorMod(Arrays.hashCode(args), forks) == forkIndex);
        } catch (NumberFormatException e) {
            throw new ValueExprEvaluationException(PARALLEL_SPLIT_FUNCTION.getLocalName() + " function first argument must be numeric constant");
        }
    }

    public static int getNumberOfForksFromFunctionArgument(String query, boolean sparqlUpdate, int stage) throws IllegalArgumentException{
        ParallelSplitFunctionVisitor psfv = new ParallelSplitFunctionVisitor();
        if (sparqlUpdate) {
            List<UpdateExpr> exprs = QueryParserUtil.parseUpdate(QueryLanguage.SPARQL, query, null).getUpdateExprs();
            if (exprs.size() > stage) {
                exprs.get(stage).visit(psfv);
            }
        } else {
            QueryParserUtil.parseQuery(QueryLanguage.SPARQL, query, null).getTupleExpr().visit(psfv);
        }
        return psfv.forks;
    }

    private static final class ParallelSplitFunctionVisitor extends AbstractQueryModelVisitor<IllegalArgumentException> {
        int forks = 0;
        @Override
        public void meet(FunctionCall node) throws IllegalArgumentException {
            if (PARALLEL_SPLIT_FUNCTION.toString().equals(node.getURI())) {
                List<ValueExpr> args = node.getArgs();
                if (args.size() < 2) throw new IllegalArgumentException(PARALLEL_SPLIT_FUNCTION.getLocalName() + " function has at least two mandatory arguments: <constant number of parallel forks> and <binding variable(s) to filter by>");
                try {
                    int num = Integer.parseInt(((ValueConstant)args.get(0)).getValue().stringValue());
                    if (num < 1) throw new NullPointerException();
                    if (forks == 0) {
                        forks = num;
                    } else if (forks != num) {
                        throw new IllegalArgumentException(PARALLEL_SPLIT_FUNCTION.getLocalName() + " function is used twice with different first argument (number of forks)");
                    }
                } catch (ClassCastException | NullPointerException | NumberFormatException ex) {
                    throw new IllegalArgumentException(PARALLEL_SPLIT_FUNCTION.getLocalName() + " function first argument (number of forks) must be integer constant >0");
                }
            }
        }
    }
}