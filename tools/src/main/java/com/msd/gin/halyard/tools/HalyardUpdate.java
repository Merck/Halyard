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

import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.sail.SailRepository;

/**
 * Command line tool executing SPARQL Update query on Halyard dataset directly
 * @author Adam Sotona (MSD)
 */
public final class HalyardUpdate {

    private static final Logger LOG = Logger.getLogger(HalyardUpdate.class.getName());

    static Configuration conf = null; // this is a hook to pass cuustom configuration in tests

    private static Option newOption(String opt, String argName, String description) {
        Option o = new Option(opt, null, argName != null, description);
        o.setArgName(argName);
        return o;
    }

    private static void printHelp(Options options) {
        new HelpFormatter().printHelp(100, "update", "Updates Halyard RDF store based on provided SPARQL update query", options, "Example: update -s my_dataset -q 'insert {?o owl:sameAs ?s} where {?s owl:sameAs ?o}'", true);
    }

    /**
     * Main of the HalyardUpdate
     * @param args String command line arguments
     * @throws Exception throws Exception in case of any problem
     */
    public static void main(final String args[]) throws Exception {
        if (conf == null) conf = new Configuration();
        Options options = new Options();
        options.addOption(newOption("h", null, "Prints this help"));
        options.addOption(newOption("v", null, "Prints version"));
        options.addOption(newOption("s", "source_htable", "Source HBase table with Halyard RDF store"));
        options.addOption(newOption("q", "sparql_query", "SPARQL tuple or graph query executed to export the data"));
        options.addOption(newOption("e", "elastic_index_url", "Optional ElasticSearch index URL"));
        try {
            CommandLine cmd = new PosixParser().parse(options, args);
            if (args.length == 0 || cmd.hasOption('h')) {
                printHelp(options);
                return;
            }
            if (cmd.hasOption('v')) {
                Properties p = new Properties();
                try (InputStream in = HalyardUpdate.class.getResourceAsStream("/META-INF/maven/com.msd.gin.halyard/halyard-tools/pom.properties")) {
                    if (in != null) p.load(in);
                }
                System.out.println("Halyard Update version " + p.getProperty("version", "unknown"));
                return;
            }
            if (!cmd.getArgList().isEmpty()) throw new ParseException("Unknown arguments: " + cmd.getArgList().toString());
            for (char c : "sq".toCharArray()) {
                if (!cmd.hasOption(c))  throw new ParseException("Missing mandatory option: " + c);
            }
            for (char c : "sqe".toCharArray()) {
                String s[] = cmd.getOptionValues(c);
                if (s != null && s.length > 1)  throw new ParseException("Multiple values for option: " + c);
            }

            SailRepository rep = new SailRepository(new TimeAwareHBaseSail(conf, cmd.getOptionValue('s'), false, 0, true, 0, cmd.getOptionValue('e'), null));
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

        } catch (Exception exp) {
            System.out.println(exp.getMessage());
            printHelp(options);
            throw exp;
        }
    }
}
