/*
 * Copyright © 2014 Merck Sharp & Dohme Corp., a subsidiary of Merck & Co., Inc.
 * All rights reserved.
 */
package com.msd.gin.halyard.sail;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HALYARD {

    private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    public static final String PREFIX = "halyard";

    public static final String NAMESPACE = "http://merck.github.io/Halyard/ns#";

    public static final IRI STATS_ROOT_NODE = SVF.createIRI(NAMESPACE, "statsRoot");

    public static final IRI STATS_GRAPH_CONTEXT = SVF.createIRI(NAMESPACE, "statsContext");

    public static final IRI NAMESPACE_PREFIX_PROPERTY = HALYARD.SVF.createIRI(NAMESPACE, "namespacePrefix");

    public final static IRI TABLE_NAME_PROPERTY = SVF.createIRI(NAMESPACE, "tableName");

    public final static IRI SPLITBITS_PROPERTY = SVF.createIRI(NAMESPACE, "splitBits");

    public final static IRI CREATE_TABLE_PROPERTY = SVF.createIRI(NAMESPACE, "createTable");

    public final static IRI PUSH_STRATEGY_PROPERTY = SVF.createIRI(NAMESPACE, "pushStrategy");

    public final static IRI EVALUATION_TIMEOUT_PROPERTY = SVF.createIRI(NAMESPACE, "evaluationTimeout");

    public final static IRI ELASTIC_INDEX_URL_PROPERTY = SVF.createIRI(NAMESPACE, "elasticIndexUrl");

    public final static IRI SEARCH_TYPE = SVF.createIRI(NAMESPACE, "search");
}
