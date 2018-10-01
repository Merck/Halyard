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

import org.apache.commons.cli.CommandLine;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.sail.SailRepository;

/**
 * Command line tool executing SPARQL Update query on Halyard dataset directly
 * @author Adam Sotona (MSD)
 */
public final class HalyardUpdate extends AbstractHalyardTool {

    public HalyardUpdate() {
        super(
            "update",
            "Halyard Update is a command-line application designed to run SPARQL Update operations to transform data in an HBase Halyard dataset",
            "Example: halyard update -s my_dataset -q 'insert {?o owl:sameAs ?s} where {?s owl:sameAs ?o}'"
        );
        addOption("s", "source-dataset", "dataset_table", "Source HBase table with Halyard RDF store", true, true);
        addOption("q", "update-operation", "sparql_update_operation", "SPARQL update operation to be executed", true, true);
        addOption("e", "elastic-index", "elastic_index_url", "Optional ElasticSearch index URL", false, true);
    }


    public int run(CommandLine cmd) throws Exception {
        SailRepository rep = new SailRepository(new TimeAwareHBaseSail(getConf(), cmd.getOptionValue('s'), false, 0, true, 0, cmd.getOptionValue('e'), null));
        rep.initialize();
        try {
            Update u = rep.getConnection().prepareUpdate(QueryLanguage.SPARQL, cmd.getOptionValue('q'));
            ((MapBindingSet)u.getBindings()).addBinding(new TimeAwareHBaseSail.TimestampCallbackBinding());
            LOG.info("Update execution started");
            u.execute();
            LOG.info("Update finished");
        } finally {
            rep.shutDown();
        }
        return 0;
    }
}
