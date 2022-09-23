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

import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.sail.HBaseSailConnection;

import java.io.IOException;
import java.net.URL;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.SailException;

/**
 * Command line tool for various Halyard profiling
 * @author Adam Sotona (MSD)
 */
public final class HalyardProfile extends AbstractHalyardTool {

    public HalyardProfile() {
        super(
            "profile",
            "Halyard Profile is a command-line tool designed to profile SPARQL query performance within the actual Halyard environment. Actually it is limited to the statical analysis only.",
            "Example: halyard profile -s my_dataset -q 'select * where {?s owl:sameAs ?o}'"
        );
        addOption("s", "source-dataset", "dataset_table", "Source HBase table with Halyard RDF store", true, true);
        addOption("q", "query", "sparql_query", "SPARQL query to profile", true, true);
        addOption("i", "elastic-index", "elastic_index_url", "Optional ElasticSearch index URL", false, true);
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
    	String queryString = cmd.getOptionValue('q');
    	String elasticIndexURL = cmd.getOptionValue('i');
		SailRepository repo = new SailRepository(new HBaseSail(getConf(), cmd.getOptionValue('s'), false, 0, true, 0, elasticIndexURL != null ? new URL(elasticIndexURL) : null, null, new HBaseSail.SailConnectionFactory() {
			@Override
			public HBaseSailConnection createConnection(HBaseSail sail) throws IOException {
				return new HBaseSailConnection(sail) {
					private final NumberFormat cardinalityFormatter = DecimalFormat.getNumberInstance();

					@Override
		            public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred) throws SailException {
		                System.out.println(toMessage("Original query:", tupleExpr));
		                return super.evaluate(tupleExpr, dataset, bindings, includeInferred);
		            }
		            @Override
		            protected CloseableIteration<BindingSet, QueryEvaluationException> evaluateInternal(PreparedQuery preparedQuery) {
		                System.out.println(toMessage("Optimized query:", preparedQuery.getTupleExpression()));
		                return new EmptyIteration<>();
		            }
		            private String toMessage(String msg, TupleExpr expr) {
		                final Map<TupleExpr, Double> cardMap = new HashMap<>();
		                if (expr instanceof QueryRoot) {
		                    expr = ((QueryRoot)expr).getArg();
		                }
						getStatistics().updateCardinalityMap(expr, Collections.emptySet(), Collections.emptySet(), cardMap);
		                final StringBuilder buf = new StringBuilder(256);
		                buf.append(msg).append(System.lineSeparator());
		                expr.visit(new AbstractQueryModelVisitor<RuntimeException>() {
		                    private int indentLevel = 0;
		                    @Override
		                    protected void meetNode(QueryModelNode node) {
		                        for (int i = 0; i < indentLevel; i++) {
		                                buf.append("    ");
		                        }
		                        buf.append(node.getSignature());
		                        StringBuilder statsBuf = new StringBuilder(100);
		                        Double card = cardMap.get(node);
		                        String sep = "";
		                        if (card != null) {
		                            statsBuf.append(sep).append("cardinality = ").append(cardinalityFormatter.format(card));
		                            sep = ", ";
		                        }
		                        double sizeEstimate = node.getResultSizeEstimate();
		                        if (sizeEstimate >= 0.0) {
		                        	statsBuf.append(sep).append("size = ").append(cardinalityFormatter.format(sizeEstimate));
		                            sep = ", ";
		                        }
		                        double costEstimate = node.getCostEstimate();
		                        if (costEstimate >= 0.0) {
		                        	statsBuf.append(sep).append("cost = ").append(cardinalityFormatter.format(costEstimate));
		                            sep = ", ";
		                        }
		                        String algorithm = (node instanceof BinaryTupleOperator) ? ((BinaryTupleOperator)node).getAlgorithmName() : null;
		                        if (algorithm != null) {
		                        	statsBuf.append(sep).append("algorithm = ").append(algorithm);
		                            sep = ", ";
		                        }
		                        if (statsBuf.length() > 0) {
		                            buf.append(" [").append(statsBuf).append("]");
		                        }
	                            buf.append(System.lineSeparator());
		                        indentLevel++;
		                        super.meetNode(node);
		                        indentLevel--;
		                    }
		                });
		                return buf.toString();
		            }
				};
			}

		}));
        repo.init();
        try {
        	try(RepositoryConnection conn = repo.getConnection()) {
	            Query q = conn.prepareQuery(QueryLanguage.SPARQL, queryString, null);
	        	System.out.println(queryString);
	        	System.out.println();
	            if (q instanceof BooleanQuery) {
	                ((BooleanQuery)q).evaluate();
	            } else if (q instanceof TupleQuery) {
	                ((TupleQuery)q).evaluate();
	            } else if (q instanceof GraphQuery) {
	                ((GraphQuery)q).evaluate();
	            }
        	}
        } finally {
            repo.shutDown();
        }
        return 0;
    }
}
