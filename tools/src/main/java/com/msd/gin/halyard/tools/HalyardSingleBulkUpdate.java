package com.msd.gin.halyard.tools;

import static com.msd.gin.halyard.tools.HalyardBulkUpdate.*;

import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.tools.HalyardBulkUpdate.SPARQLUpdateMapper;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.helpers.NTriplesUtil;

/**
 * Command line tool executing SPARQL Update query using MapReduce.
 */
public final class HalyardSingleBulkUpdate extends AbstractHalyardTool {

    public HalyardSingleBulkUpdate() {
        super(
            "singlebulkupdate",
            "Halyard Single Bulk Update is a command-line application designed to run SPARQL Update operations to transform data in an HBase Halyard dataset",
            "Example: halyard singlebulkupdate -s my_dataset -w hdfs:///my_tmp_workdir -q 'insert {?o owl:sameAs ?s} where {?s owl:sameAs ?o}'"
        );
        addOption("s", "source-dataset", "dataset_table", "Source HBase table with Halyard RDF store", true, true);
        addOption("q", "update-operation", "sparql_update_operation", "SPARQL update operation to be executed", true, true);
        addOption("w", "work-dir", "shared_folder", "Unique non-existent folder within shared filesystem to server as a working directory for the temporary HBase files,  the files are moved to their final HBase locations during the last stage of the load process", true, true);
        addOption("e", "target-timestamp", "timestamp", "Optionally specify timestamp of all updated records (default is actual time of the operation)", false, true);
        addOption("i", "elastic-index", "elastic_index_url", HBaseSail.ELASTIC_INDEX_URL, "Optional ElasticSearch index URL", false, true);
    }

    public int run(CommandLine cmd) throws Exception {
        String source = cmd.getOptionValue('s');
        String query = cmd.getOptionValue('q');
        String workdir = cmd.getOptionValue('w');
        configureString(cmd, 'i', null);
        TableMapReduceUtil.addDependencyJarsForClasses(getConf(),
               NTriplesUtil.class,
               Rio.class,
               AbstractRDFHandler.class,
               RDFFormat.class,
               RDFParser.class,
               Table.class,
               HBaseConfiguration.class,
               AuthenticationProtos.class);
        HBaseConfiguration.addHbaseResources(getConf());
        getConf().setStrings(TABLE_NAME_PROPERTY, source);
        getConf().setLong(HalyardBulkLoad.TIMESTAMP_PROPERTY, cmd.hasOption('e') ? Long.parseLong(cmd.getOptionValue('e')) : System.currentTimeMillis());
        int stages = QueryParserUtil.parseUpdate(QueryLanguage.SPARQL, query, null).getUpdateExprs().size();
        for (int stage = 0; stage < stages; stage++) {
	        Job job = Job.getInstance(getConf(), "HalyardSingleBulkUpdate -> " + workdir + " -> " + source + " stage #" + stage);
            job.getConfiguration().setInt(STAGE_PROPERTY, stage);
	        job.setJarByClass(HalyardSingleBulkUpdate.class);
	        job.setMapperClass(SPARQLUpdateMapper.class);
	        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
	        job.setMapOutputValueClass(KeyValue.class);
	        job.setInputFormatClass(QueryInputFormat.class);
	        job.setSpeculativeExecution(false);
			Connection conn = HalyardTableUtils.getConnection(getConf());
			try (Table hTable = HalyardTableUtils.getTable(conn, source, false, 0)) {
				TableName tableName = hTable.getName();
				RegionLocator regionLocator = conn.getRegionLocator(tableName);
				HFileOutputFormat2.configureIncrementalLoad(job, hTable.getDescriptor(), regionLocator);
				int repeatCount = Math.max(1, ParallelSplitFunction.getNumberOfForksFromFunctionArgument(query, true, stage));
	            QueryInputFormat.addQuery(job.getConfiguration(), "singlebulkupdate", query, repeatCount);
                Path outPath = new Path(workdir, "stage"+stage);
                FileOutputFormat.setOutputPath(job, outPath);
	            TableMapReduceUtil.addDependencyJars(job);
	            TableMapReduceUtil.initCredentials(job);
	            if (job.waitForCompletion(true)) {
					BulkLoadHFiles.create(getConf()).bulkLoad(tableName, outPath);
                    LOG.info("Stage #{} of {} completed.", stage+1, stages);
	            } else {
            		LOG.error("Stage #{} of {} failed to complete.", stage+1, stages);
	                return -1;
	            }
	        }
	    }
        LOG.info("Single Bulk Update completed.");
        return 0;
    }
}
