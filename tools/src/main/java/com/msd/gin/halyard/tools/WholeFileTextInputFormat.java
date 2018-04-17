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

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author Adam Sotona (MSD)
 */
final class WholeFileTextInputFormat extends FileInputFormat<NullWritable, Text> {

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public List<FileStatus> listStatus(JobContext job) throws IOException {
        return super.listStatus(job); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public RecordReader<NullWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new RecordReader<NullWritable, Text>() {
            private FileSplit fileSplit;
            private Configuration conf;
            private boolean processed = false;
            private final NullWritable key = NullWritable.get();
            private final Text value = new Text();

            @Override
            public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                this.fileSplit = (FileSplit) inputSplit;
                this.conf = taskAttemptContext.getConfiguration();
            }

            @Override
            public boolean nextKeyValue() throws IOException {
                if (!processed) {
                    byte[] contents = new byte[(int) fileSplit.getLength()];
                    Path file = fileSplit.getPath();
                    FileSystem fs = file.getFileSystem(conf);
                    try (FSDataInputStream in = fs.open(file)){
                        IOUtils.readFully(in, contents, 0, contents.length);
                        value.set(contents);
                    }
                    processed = true;
                    return true;
                }
                return false;
            }

            @Override
            public NullWritable getCurrentKey() throws IOException, InterruptedException {
                return key;
            }

            @Override
            public Text getCurrentValue() throws IOException, InterruptedException {
                return value;
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return processed ? 1.0f : 0.0f;
            }

            @Override
            public void close() throws IOException {
                // do nothing
            }
        };
    }

}
