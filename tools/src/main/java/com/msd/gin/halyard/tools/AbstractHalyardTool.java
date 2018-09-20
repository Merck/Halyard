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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

/**
 *
 * @author Adam Sotona (MSD)
 */
public abstract class AbstractHalyardTool implements Tool {

    static final Logger LOG = Logger.getLogger(AbstractHalyardTool.class.getName());

    private Configuration conf;
    final String name, header, footer;
    private final Options options = new Options();
    private final List<String> singleOptions = new ArrayList<>();
    private int opts = 0;

    protected AbstractHalyardTool(String name, String header, String footer) {
        this.name = name;
        this.header = header;
        this.footer = footer;
        addOption("h", "help", null, "Prints this help", false, false);
        addOption("v", "version", null, "Prints version", false, false);
    }

    protected final void printHelp() {
        HelpFormatter hf = new HelpFormatter();
        hf.setOptionComparator(new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                if (o1 instanceof OrderedOption && o2 instanceof OrderedOption) return ((OrderedOption)o1).order - ((OrderedOption)o2).order;
                else return 0;
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

    protected final void addOption(String opt, String longOpt, String argName, String description, boolean required, boolean single) {
        Option o = new OrderedOption(opts++, opt, longOpt, argName, description, required);
        options.addOption(o);
        if (single) {
            singleOptions.add(opt == null ? longOpt : opt);
        }

    }

    private static final class OrderedOption extends Option {
        final int order;
        public OrderedOption(int order, String opt, String longOpt, String argName, String description, boolean required) {
            super(opt, longOpt, argName != null, description);
            setArgName(argName);
            setRequired(required);
            this.order = order;
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
            }.parse(options, args);
            if (args.length == 0 || cmd.hasOption('h')) {
                printHelp();
                return -1;
            }
            if (cmd.hasOption('v')) {
                System.out.println(name + " version " + getVersion());
                return 0;
            }
            if (!cmd.getArgList().isEmpty()) {
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
}
