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
package com.msd.gin.halyard.sail;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.repository.config.RepositoryConfigSchema;
import org.eclipse.rdf4j.repository.sail.config.SailRepositorySchema;
import org.eclipse.rdf4j.sail.config.AbstractSailImplConfig;
import org.eclipse.rdf4j.sail.config.SailConfigException;
import org.eclipse.rdf4j.sail.config.SailConfigSchema;

/**
 * Configuration information for the HBase SAIL and methods to serialize/ deserialize the configuration.
 * @author Adam Sotona (MSD)
 */
public final class HBaseSailConfig extends AbstractSailImplConfig {

    private static final Map<IRI, IRI> BACK_COMPATIBILITY_MAP = new HashMap<>();
    private static final String OLD_NAMESPACE = "http://gin.msd.com/halyard/sail/hbase#";

    static {
        ValueFactory factory = SimpleValueFactory.getInstance();
        BACK_COMPATIBILITY_MAP.put(HALYARD.TABLE_NAME_PROPERTY, factory.createIRI(OLD_NAMESPACE, "tablespace"));
        BACK_COMPATIBILITY_MAP.put(HALYARD.SPLITBITS_PROPERTY, factory.createIRI(OLD_NAMESPACE, "splitbits"));
        BACK_COMPATIBILITY_MAP.put(HALYARD.CREATE_TABLE_PROPERTY, factory.createIRI(OLD_NAMESPACE, "create"));
        BACK_COMPATIBILITY_MAP.put(HALYARD.PUSH_STRATEGY_PROPERTY, factory.createIRI(OLD_NAMESPACE, "pushstrategy"));
        BACK_COMPATIBILITY_MAP.put(HALYARD.EVALUATION_TIMEOUT_PROPERTY, factory.createIRI(OLD_NAMESPACE, "evaluationtimeout"));
    }

    private String tablespace = null;
    private int splitBits = 0;
    private boolean create = true;
    private boolean push = true;
    private int evaluationTimeout = 180; //3 min
    private String elasticIndexURL = "";

    /**
     * Sets HBase table name
     * @param tablespace String HBase table name
     */
    public void setTablespace(String tablespace) {
        this.tablespace = tablespace;
    }

    /**
     * Gets HBase table name
     * @return String table name
     */
    public String getTablespace() {
        return tablespace;
    }

    /**
     * Sets number of bits used for HBase table region pre-split
     * @param splitBits int number of bits used for HBase table region pre-split
     */
    public void setSplitBits(int splitBits) {
        this.splitBits = splitBits;
    }

    /**
     * Gets number of bits used for HBase table region pre-split
     * @return int number of bits used for HBase table region pre-split
     */
    public int getSplitBits() {
        return splitBits;
    }

    /**
     * Gets flag if the HBase table should be created
     * @return boolean flag if the HBase table should be created
     */
    public boolean isCreate() {
        return create;
    }

    /**
     * Sets flag if the HBase table should be created
     * @param create boolean flag if the HBase table should be created
     */
    public void setCreate(boolean create) {
        this.create = create;
    }

    /**
     * Gets flag to use {@link com.msd.gin.halyard.strategy.HalyardEvaluationStrategy} instead of {@link org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategy}
     * @return boolean flag to use HalyardEvaluationStrategy instead of StrictEvaluationStrategy
     */
    public boolean isPush() {
        return push;
    }

    /**
     * Sets flag to use {@link com.msd.gin.halyard.strategy.HalyardEvaluationStrategy} instead of {@link org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategy}
     * @param push boolean flag to use HalyardEvaluationStrategy instead of StrictEvaluationStrategy
     */
    public void setPush(boolean push) {
        this.push = push;
    }

    /**
     * Gets timeout in seconds for each query evaluation, negative values mean no timeout
     * @return int timeout in seconds for each query evaluation, negative values mean no timeout
     */
    public int getEvaluationTimeout() {
        return evaluationTimeout;
    }

    /**
     * Sets timeout in seconds for each query evaluation, negative values mean no timeout
     * @param evaluationTimeout int timeout in seconds for each query evaluation, negative values mean no timeout
     */
    public void setEvaluationTimeout(int evaluationTimeout) {
        this.evaluationTimeout = evaluationTimeout;
    }

    /**
     * Sets ElasticSearch index URL
     * @param elasticIndexURL String ElasticSearch index URL
     */
    public void setElasticIndexURL(String elasticIndexURL) {
        this.elasticIndexURL = elasticIndexURL;
    }

    /**
     * Gets ElasticSearch index URL
     * @return String ElasticSearch index URL
     */
    public String getElasticIndexURL() {
        return elasticIndexURL;
    }

    /**
     * Default constructor of HBaseSailConfig
     */
    public HBaseSailConfig() {
        super(HBaseSailFactory.SAIL_TYPE);
    }

    /**
     * Stores configuration into the given Model
     * @param graph Model to store configuration into
     * @return Resource node with the configuration within the Model
     */
    @Override
    public Resource export(Model graph) {
        Resource implNode = super.export(graph);
        ValueFactory vf = SimpleValueFactory.getInstance();
        if (tablespace != null) graph.add(implNode, HALYARD.TABLE_NAME_PROPERTY, vf.createLiteral(tablespace));
        graph.add(implNode, HALYARD.SPLITBITS_PROPERTY, vf.createLiteral(splitBits));
        graph.add(implNode, HALYARD.CREATE_TABLE_PROPERTY, vf.createLiteral(create));
        graph.add(implNode, HALYARD.PUSH_STRATEGY_PROPERTY, vf.createLiteral(push));
        graph.add(implNode, HALYARD.EVALUATION_TIMEOUT_PROPERTY, vf.createLiteral(evaluationTimeout));
        graph.add(implNode, HALYARD.ELASTIC_INDEX_URL_PROPERTY, vf.createLiteral(elasticIndexURL));
        return implNode;
    }

    /**
     * Retrieves configuration from the given Model
     * @param graph Model to retrieve the configuration from
     * @param implNode Resource node with the configuration within the Model
     * @throws SailConfigException throws SailConfigException in case of parsing or retrieval problems
     */
    @Override
    public void parse(Model graph, Resource implNode) throws SailConfigException {
        super.parse(graph, implNode);
        Optional<Literal> tablespaceValue = backCompatibilityFilterObjectLiteral(graph, implNode, HALYARD.TABLE_NAME_PROPERTY);
        if (tablespaceValue.isPresent() && tablespaceValue.get().stringValue().length() > 0) {
            setTablespace(tablespaceValue.get().stringValue());
        } else {
            Optional<Resource> delegate = Models.subject(graph.filter(null, SailConfigSchema.DELEGATE, implNode));
            Optional<Resource> sailImpl = Models.subject(graph.filter(null, SailRepositorySchema.SAILIMPL, delegate.isPresent() ? delegate.get(): implNode));
            if (sailImpl.isPresent()) {
                Optional<Resource> repoImpl = Models.subject(graph.filter(null, RepositoryConfigSchema.REPOSITORYIMPL, sailImpl.get()));
                if (repoImpl.isPresent()) {
                    Optional<Literal> idValue = Models.objectLiteral(graph.filter(repoImpl.get(), RepositoryConfigSchema.REPOSITORYID, null));
                    if (idValue.isPresent()) {
                        setTablespace(idValue.get().stringValue());
                    }
                }
            }

        }
        Optional<Literal> splitBitsValue = backCompatibilityFilterObjectLiteral(graph, implNode, HALYARD.SPLITBITS_PROPERTY);
        if (splitBitsValue.isPresent()) try {
            setSplitBits(splitBitsValue.get().intValue());
        } catch (NumberFormatException e) {
            throw new SailConfigException(e);
        }
        Optional<Literal> createValue = backCompatibilityFilterObjectLiteral(graph, implNode, HALYARD.CREATE_TABLE_PROPERTY);
        if (createValue.isPresent()) try {
            setCreate(createValue.get().booleanValue());
        } catch (IllegalArgumentException e) {
            throw new SailConfigException(e);
        }
        Optional<Literal> pushValue = backCompatibilityFilterObjectLiteral(graph, implNode, HALYARD.PUSH_STRATEGY_PROPERTY);
        if (pushValue.isPresent()) try {
            setPush(pushValue.get().booleanValue());
        } catch (IllegalArgumentException e) {
            throw new SailConfigException(e);
        }
        Optional<Literal> timeoutValue = backCompatibilityFilterObjectLiteral(graph, implNode, HALYARD.EVALUATION_TIMEOUT_PROPERTY);
        if (timeoutValue.isPresent()) try {
            setEvaluationTimeout(timeoutValue.get().intValue());
        } catch (NumberFormatException e) {
            throw new SailConfigException(e);
        }
        Optional<Literal> elasticIndexValue = backCompatibilityFilterObjectLiteral(graph, implNode, HALYARD.ELASTIC_INDEX_URL_PROPERTY);
        if (elasticIndexValue.isPresent()) {
            setElasticIndexURL(elasticIndexValue.get().stringValue());
        }
    }

    private static Optional<Literal> backCompatibilityFilterObjectLiteral(Model graph, Resource subject, IRI predicate) {
        Optional<Literal> value = Models.objectLiteral(graph.filter(subject, predicate, null));
        return value.isPresent() ? value : Models.objectLiteral(graph.filter(subject, BACK_COMPATIBILITY_MAP.get(predicate), null));
    }
}
