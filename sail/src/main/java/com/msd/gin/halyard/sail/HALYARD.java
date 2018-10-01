/*
 * Copyright © 2014 Merck Sharp & Dohme Corp., a subsidiary of Merck & Co., Inc.
 * All rights reserved.
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

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * IRI constants used by Halyard.
 * @author Adam Sotona (MSD)
 */
public final class HALYARD {

    HALYARD() {}

    private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    public static final String PREFIX = "halyard";

    public static final String NAMESPACE = "http://merck.github.io/Halyard/ns#";

    public static final IRI STATS_ROOT_NODE = SVF.createIRI(NAMESPACE, "statsRoot");

    public static final IRI STATS_GRAPH_CONTEXT = SVF.createIRI(NAMESPACE, "statsContext");

    public static final IRI SYSTEM_GRAPH_CONTEXT = SVF.createIRI(NAMESPACE, "system");

    public static final IRI NAMESPACE_PREFIX_PROPERTY = HALYARD.SVF.createIRI(NAMESPACE, "namespacePrefix");

    public final static IRI TABLE_NAME_PROPERTY = SVF.createIRI(NAMESPACE, "tableName");

    public final static IRI SPLITBITS_PROPERTY = SVF.createIRI(NAMESPACE, "splitBits");

    public final static IRI CREATE_TABLE_PROPERTY = SVF.createIRI(NAMESPACE, "createTable");

    public final static IRI PUSH_STRATEGY_PROPERTY = SVF.createIRI(NAMESPACE, "pushStrategy");

    public final static IRI EVALUATION_TIMEOUT_PROPERTY = SVF.createIRI(NAMESPACE, "evaluationTimeout");

    public final static IRI ELASTIC_INDEX_URL_PROPERTY = SVF.createIRI(NAMESPACE, "elasticIndexURL");

    public final static IRI SEARCH_TYPE = SVF.createIRI(NAMESPACE, "search");

    public final static IRI PARALLEL_SPLIT_FUNCTION = SVF.createIRI(NAMESPACE, "forkAndFilterBy");

}
