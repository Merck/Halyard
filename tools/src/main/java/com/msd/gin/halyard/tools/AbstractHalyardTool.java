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

import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.common.IdValueFactory;
import com.msd.gin.halyard.common.Keyspace;
import com.msd.gin.halyard.common.KeyspaceConnection;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.SSLSettings;
import com.msd.gin.halyard.common.StatementIndices;
import com.msd.gin.halyard.common.ValueIO;
import com.msd.gin.halyard.sail.HBaseSail;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Adam Sotona (MSD)
 */
public abstract class AbstractHalyardTool implements Tool {

    static final Logger LOG = LoggerFactory.getLogger(AbstractHalyardTool.class);

    protected static String confProperty(String tool, String key) {
        return "halyard-tools."+tool+"."+key;
    }

    private static final String SOURCE_PROPERTIES = "source";
    protected static final String SOURCE_PATHS_PROPERTY = confProperty(SOURCE_PROPERTIES, "paths");
    protected static final String SOURCE_NAME_PROPERTY = confProperty(SOURCE_PROPERTIES, "name");
    protected static final String SNAPSHOT_PATH_PROPERTY = confProperty(SOURCE_PROPERTIES, "snapshot");

    /**
     * Property defining optional ElasticSearch index URL
     */
    protected static final String ELASTIC_INDEX_URL = "halyard.elastic.index.url";

    private Configuration conf;
    final String name, header, footer;
    private final Options options = new Options();
    private final List<String> singleOptions = new ArrayList<>();
    private int opts = 0;
    /**
     * Allow to pass additional unspecified arguments via command line. By default this functionality is disabled.
     * This functionality is used by HalyardEndpoint tool to pass additional arguments for an inner process.
     */
    protected boolean cmdMoreArgs = false;

    protected AbstractHalyardTool(String name, String header, String footer) {
        this.name = name;
        this.header = header;
        this.footer = footer;
        addOption("h", "help", null, "Prints this help", false, false);
        addOption("v", "version", null, "Prints version", false, false);
    }

    protected final void printHelp() {
        HelpFormatter hf = new HelpFormatter();
        hf.setOptionComparator(new Comparator<Option>() {
            @Override
            public int compare(Option o1, Option o2) {
                if (o1 instanceof OrderedOption && o2 instanceof OrderedOption) {
                	return ((OrderedOption)o1).order - ((OrderedOption)o2).order;
                } else {
                	return 0;
                }
            }
        });
        hf.printHelp(100, "halyard " + name, header, options, footer, true);
    }

    @Override
    public final Configuration getConf() {
        return this.conf;
    }

    @Override
    public final void setConf(final Configuration c) {
        this.conf = c;
    }

    protected static String[] validateIRIs(String... iris) throws ParseException {
    	for (String iri : iris) {
    		try {
				new URI(iri).isAbsolute();
			} catch (URISyntaxException e) {
				throw (ParseException) new ParseException("Invalid IRI: "+iri).initCause(e);
			}
    	}
    	return iris;
    }

    protected static HBaseSail.ElasticSettings getElasticSettings(Configuration conf) throws MalformedURLException {
        HBaseSail.ElasticSettings esSettings;
    	String elasticIndexURL = conf.get(ELASTIC_INDEX_URL);
        if (elasticIndexURL != null) {
        	URL url = new URL(elasticIndexURL);
        	SSLSettings sslSettings = "https".equals(url.getProtocol()) ? SSLSettings.from(conf) : null;
        	esSettings = HBaseSail.ElasticSettings.from(url, sslSettings);
        } else {
        	esSettings = null;
        }
        return esSettings;
    }

    protected void configureIRI(CommandLine cmd, char opt, String defaultValue) throws ParseException {
    	OrderedOption option = (OrderedOption) options.getOption(Character.toString(opt));
    	// command line args always override
    	if (cmd.hasOption(opt)) {
    		String value = cmd.getOptionValue(opt);
    		validateIRIs(value);
    		conf.set(option.confProperty, value);
    	} else if (defaultValue != null) {
    		conf.setIfUnset(option.confProperty, String.valueOf(defaultValue));
    	}
    }

    protected void configureIRIPattern(CommandLine cmd, char opt, String defaultValue) throws ParseException {
    	configureString(cmd, opt, defaultValue);
    }

    protected void configureString(CommandLine cmd, char opt, String defaultValue) {
    	OrderedOption option = (OrderedOption) options.getOption(Character.toString(opt));
    	// command line args always override
    	if (cmd.hasOption(opt)) {
    		conf.set(option.confProperty, cmd.getOptionValue(opt));
    	} else if (defaultValue != null) {
    		conf.setIfUnset(option.confProperty, String.valueOf(defaultValue));
    	}
    }

    protected void configureBoolean(CommandLine cmd, char opt) {
    	OrderedOption option = (OrderedOption) options.getOption(Character.toString(opt));
    	// command line args always override
    	if (cmd.hasOption(opt)) {
    		conf.setBoolean(option.confProperty, true);
    	}
    }

    protected void configureInt(CommandLine cmd, char opt, int defaultValue) {
    	OrderedOption option = (OrderedOption) options.getOption(Character.toString(opt));
    	// command line args always override
    	if (cmd.hasOption(opt)) {
    		conf.setInt(option.confProperty, Integer.parseInt(cmd.getOptionValue(opt)));
    	} else {
    		conf.setIfUnset(option.confProperty, String.valueOf(defaultValue));
    	}
    }

    protected void configureLong(CommandLine cmd, char opt, long defaultValue) {
    	OrderedOption option = (OrderedOption) options.getOption(Character.toString(opt));
    	// command line args always override
    	if (cmd.hasOption(opt)) {
    		conf.setLong(option.confProperty, Long.parseLong(cmd.getOptionValue(opt)));
    	} else {
    		conf.setIfUnset(option.confProperty, String.valueOf(defaultValue));
    	}
    }

    protected final void addOption(String opt, String longOpt, String argName, String description, boolean required, boolean single) {
    	addOption(opt, longOpt, argName, null, description, required, single);
    }

    protected final void addOption(String opt, String longOpt, String argName, String confProperty, String description, boolean required, boolean single) {
        Option o = new OrderedOption(opts++, opt, longOpt, argName, confProperty, description, required);
        options.addOption(o);
        if (single) {
            singleOptions.add(opt == null ? longOpt : opt);
        }
    }

    protected final Collection<Option> getOptions() {
        return options.getOptions();
    }

    protected final List<Option> getRequiredOptions() {
        List<?> optionNames = options.getRequiredOptions();
        List<Option> requiredOptions = new ArrayList<>(optionNames.size());
        for(Object name : optionNames) {
            requiredOptions.add(options.getOption((String) name));
        }
        return requiredOptions;
    }

    private static final class OrderedOption extends Option {
    	static String buildDescription(String desc, String confProperty) {
    		 return (confProperty != null) ? desc+" (configuration file property: "+confProperty+")" : desc;
    	}

    	final int order;
    	final String confProperty;
        public OrderedOption(int order, String opt, String longOpt, String argName, String confProperty, String description, boolean required) {
            super(opt, longOpt, argName != null, buildDescription(description, confProperty));
            setArgName(argName);
            setRequired(required);
            this.order = order;
            this.confProperty = confProperty;
        }
    }

    protected abstract int run(CommandLine cmd) throws Exception;

    static String getVersion() throws IOException {
        Properties p = new Properties();
        try (InputStream in = AbstractHalyardTool.class.getResourceAsStream("/META-INF/maven/com.msd.gin.halyard/halyard-tools/pom.properties")) {
            if (in != null) p.load(in);
        }
        return p.getProperty("version", "unknown");
    }

    @Override
    public final int run(String[] args) throws Exception {
        try {
            CommandLine cmd = new PosixParser(){
                @Override
                protected void checkRequiredOptions() throws MissingOptionException {
                    if (!cmd.hasOption('h') && !cmd.hasOption('v')) {
                        super.checkRequiredOptions();
                    }
                }
            }.parse(options, args, cmdMoreArgs);
            if (args.length == 0 || cmd.hasOption('h')) {
                printHelp();
                return -1;
            }
            if (cmd.hasOption('v')) {
                System.out.println(name + " version " + getVersion());
                return 0;
            }
            if (!cmdMoreArgs && !cmd.getArgList().isEmpty()) {
                throw new ParseException("Unknown arguments: " + cmd.getArgList().toString());
            }
            for (String opt : singleOptions) {
                String s[] = cmd.getOptionValues(opt);
                if (s != null && s.length > 1)  throw new ParseException("Multiple values for option: " + opt);
            }
            return run(cmd);
        } catch (Exception exp) {
            System.out.println(exp.getMessage());
            printHelp();
            throw exp;
        }
    }


    static class RdfTableMapper<K,V> extends TableMapper<K,V> {
        protected Keyspace keyspace;
        protected KeyspaceConnection keyspaceConn;
        protected RDFFactory rdfFactory;
        protected StatementIndices stmtIndices;
        protected ValueIO.Reader valueReader;

        protected final void openKeyspace(Configuration conf, String source, String restorePath) throws IOException {
            keyspace = HalyardTableUtils.getKeyspace(conf, source, restorePath);
            keyspaceConn = keyspace.getConnection();
            rdfFactory = RDFFactory.create(keyspaceConn);
            stmtIndices = new StatementIndices(conf, rdfFactory);
            valueReader = stmtIndices.createTableReader(IdValueFactory.INSTANCE, keyspaceConn);
        }

        protected void closeKeyspace() throws IOException {
            if (keyspaceConn != null) {
            	keyspaceConn.close();
            	keyspaceConn = null;
            }
            if (keyspace != null) {
                keyspace.close();
                keyspace = null;
            }
        }
    }


    static class RdfReducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
        protected Keyspace keyspace;
        protected KeyspaceConnection keyspaceConn;
        protected RDFFactory rdfFactory;
        protected StatementIndices stmtIndices;
        protected ValueIO.Reader valueReader;

        protected final void openKeyspace(Configuration conf, String source, String restorePath) throws IOException {
            keyspace = HalyardTableUtils.getKeyspace(conf, source, restorePath);
            keyspaceConn = keyspace.getConnection();
            rdfFactory = RDFFactory.create(keyspaceConn);
            stmtIndices = new StatementIndices(conf, rdfFactory);
            valueReader = stmtIndices.createTableReader(IdValueFactory.INSTANCE, keyspaceConn);
        }

        protected void closeKeyspace() throws IOException {
            if (keyspaceConn != null) {
            	keyspaceConn.close();
            	keyspaceConn = null;
            }
            if (keyspace != null) {
                keyspace.close();
                keyspace = null;
            }
        }
    }
}
