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

import java.io.PrintWriter;
import java.util.Arrays;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Adam Sotona (MSD)
 */
public final class HalyardMain {

    public static void main(String args[]) throws Exception {
        String first = args.length > 0 ? args[0] : null;
        if ("-v".equals(first) || "--version".equals(first)) {
            System.out.println("halyard version " + AbstractHalyardTool.getVersion());
        } else {
            AbstractHalyardTool tools[] = new AbstractHalyardTool[] {
                new HalyardPreSplit(),
                new HalyardBulkLoad(),
                new HalyardHiveLoad(),
                new HalyardStats(),
                new HalyardElasticIndexer(),
                new HalyardUpdate(),
                new HalyardBulkUpdate(),
                new HalyardExport(),
                new HalyardParallelExport(),
                new HalyardBulkExport(),
                new HalyardBulkDelete()
            };
            for (AbstractHalyardTool tool : tools) {
                if (tool.name.equalsIgnoreCase(first)) {
                    System.exit(ToolRunner.run(tool, Arrays.copyOfRange(args, 1, args.length)));
                }
            }
            try {
                if (first != null && !"-h".equals(first) && !"--help".equals(first)) {
                    String msg = "Unrecognized command or option: " + first;
                    System.out.println(msg);
                    throw new UnrecognizedOptionException(msg);
                }
            } finally {
                PrintWriter pw = new PrintWriter(System.out);
                HelpFormatter hf = new HelpFormatter();
                hf.printWrapped(pw, 100, "usage: halyard [<command> [-h] ... | -h | -v ]\ncommands:\n----------------------------------------------------------------------------------------------------");
                for (AbstractHalyardTool tool : tools) {
                    hf.printWrapped(pw, 100, 11, tool.name + "           ".substring(tool.name.length()) + tool.header);
                }
                pw.flush();
            }
        }
    }
}
