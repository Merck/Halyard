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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;

/**
 *
 * @author Adam Sotona (MSD)
 */
final class QueryInputFormat extends InputFormat<NullWritable, Void> {

    public static final String QUERIES = "mapreduce.input.queryinputformat.queries";
    public static final String PREFIX = "mapreduce.input.queryinputformat.";
    public static final String QUERY_SUFFIX = ".query";
    public static final String REPEAT_SUFFIX = ".repeat";

    public static void addQuery(Configuration conf, String name, String query, int repeatCount) {
        Collection<String> qNames = conf.getStringCollection(QUERIES);
        qNames.add(name);
        conf.set(PREFIX + name + QUERY_SUFFIX, query);
        conf.setInt(PREFIX + name + REPEAT_SUFFIX, repeatCount);
        conf.setStrings(QUERIES, qNames.toArray(new String[qNames.size()]));
    }

    public static void addQuery(Configuration conf, FileStatus fileStatus, boolean sparqlUpdate, int stage) throws IOException {
        Path path = fileStatus.getPath();
        try (FSDataInputStream in = path.getFileSystem(conf).open(path)) {
            byte buffer[] = new byte[(int)fileStatus.getLen()];
            IOUtils.readFully(in, buffer);
            String name = path.getName();
            String query = new String(buffer, StandardCharsets.UTF_8);
            addQuery(conf, name, query, Math.max(1, ParallelSplitFunction.getNumberOfForksFromFunctionArgument(query, sparqlUpdate, stage)));
        }
    }

    public static void addQueryRecursively(Configuration conf, Path path, boolean sparqlUpdate, int stage)
        throws IOException {
        RemoteIterator<LocatedFileStatus> iter = path.getFileSystem(conf).listLocatedStatus(path);
        while (iter.hasNext()) {
            LocatedFileStatus stat = iter.next();
            if (stat.isDirectory()) {
                addQueryRecursively(conf, stat.getPath(), sparqlUpdate, stage);
            } else {
                addQuery(conf, stat, sparqlUpdate, stage);
            }
        }
    }

    public static void setQueriesFromDirRecursive(Configuration conf, String dirs, boolean sparqlUpdate, int stage) throws IOException {
        for (String dir : StringUtils.split(dirs)) {
            Path p = new Path(StringUtils.unEscapeString(dir));
            FileStatus[] matches = p.getFileSystem(conf).globStatus(p);
            if (matches == null) {
                throw new IOException("Input path does not exist: " + p);
            } else if (matches.length == 0) {
                throw new IOException("Input Pattern " + p + " matches 0 files");
            } else {
                for (FileStatus globStat : matches) {
                    if (globStat.isDirectory()) {
                        addQueryRecursively(conf, p, sparqlUpdate, stage);
                    } else {
                        addQuery(conf, globStat, sparqlUpdate, stage);
                    }
                }
            }
        }
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        ArrayList<InputSplit> splits = new ArrayList<>();
        Configuration conf = context.getConfiguration();
        for (String qName : conf.getStringCollection(QUERIES)) {
            int repeatCount = conf.getInt(PREFIX + qName + REPEAT_SUFFIX, 1);
            String query = conf.get(PREFIX + qName + QUERY_SUFFIX);
            for (int i=0; i<repeatCount; i++) {
                splits.add(new QueryInputSplit(qName, query , i));
            }
        }
        return splits;
    }

    @Override
    public RecordReader<NullWritable, Void> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new RecordReader<NullWritable, Void>() {
            @Override
            public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                return false;
            }

            @Override
            public NullWritable getCurrentKey() throws IOException, InterruptedException {
                return null;
            }

            @Override
            public Void getCurrentValue() throws IOException, InterruptedException {
                return null;
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return 0;
            }

            @Override
            public void close() throws IOException {
            }
        };
    }

    static final class QueryInputSplit extends InputSplit implements Writable {

        public static QueryInputSplit read(DataInput in) throws IOException {
            QueryInputSplit iis = new QueryInputSplit();
            iis.readFields(in);
            return iis;
        }

        private String queryName, query;
        private int repeatIndex;

        public QueryInputSplit() {
        }

        public QueryInputSplit(String queryName, String query, int repeatIndex) {
            this.queryName = queryName;
            this.query = query;
            this.repeatIndex = repeatIndex;
        }

        public String getQueryName() {
            return queryName;
        }

        public String getQuery() {
            return query;
        }

        public int getRepeatIndex() {
            return repeatIndex;
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return 0l;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[0];
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(queryName);
            out.writeUTF(query);
            out.writeInt(repeatIndex);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            queryName = in.readUTF();
            query = in.readUTF();
            repeatIndex = in.readInt();
        }
    }
}
