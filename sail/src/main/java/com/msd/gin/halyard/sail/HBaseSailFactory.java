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

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.config.SailConfigException;
import org.eclipse.rdf4j.sail.config.SailFactory;
import org.eclipse.rdf4j.sail.config.SailImplConfig;

/**
 * Factory for constructing an HBaseSail instance.
 * @author Adam Sotona (MSD)
 */
public final class HBaseSailFactory implements SailFactory {

    /**
     * String HBaseSail type identification
     */
    public static final String SAIL_TYPE = "openrdf:HBaseStore";

    @Override
    public String getSailType() {
        return SAIL_TYPE;
    }

    /**
     * Factory method for instantiating an HBaseSailConfig
     * @return new HBaseSailConfig instance
     */
    @Override
    public SailImplConfig getConfig() {
        return new HBaseSailConfig();
    }

    @Override
    public Sail getSail(SailImplConfig config) throws SailConfigException {
        if (!SAIL_TYPE.equals(config.getType())) {
            throw new SailConfigException("Invalid Sail type: " + config.getType());
        }
        if (config instanceof HBaseSailConfig) {
            HBaseSailConfig hconfig = (HBaseSailConfig) config;
            //instantiate the sail
            HBaseSail sail = new HBaseSail(HBaseConfiguration.create(), hconfig.getTablespace(), hconfig.isCreate(), hconfig.getSplitBits(), hconfig.isPush(), hconfig.getEvaluationTimeout(), hconfig.getElasticIndexURL(), null);
            return sail;
        } else {
            throw new SailConfigException("Invalid configuration: " + config);
        }
    }

}
