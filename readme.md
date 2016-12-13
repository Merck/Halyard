# Halyard

[![CI](https://api.travis-ci.org/Merck/Halyard.svg?branch=master)](https://travis-ci.org/Merck/Halyard)
[![Coverage](https://codecov.io/github/Merck/Halyard/coverage.svg?branch=master)](https://codecov.io/gh/Merck/Halyard/)

Halyard is an extremely horizontally scalable triple store with support for named graphs, designed for integration of extremely large semantic data models and for storage and SPARQL 1.1 querying of complete Linked Data universe snapshots. Halyard implementation is based on Eclipse RDF4J framework and Apache HBase database, and it is completely written in Java.

**Author: [Adam Sotona](mailto:adam.sotona@merck.com)**

**Discussion group: <https://groups.google.com/d/forum/halyard-users>**

**Documentation: <https://merck.github.io/Halyard>**

## Get started

[Download](https://github.com/Merck/Halyard/releases) and unzip the latest `halyard-sdk-<version>.zip` bundle to a Apache Hadoop cluster node with configured Apache HBase client.

Halyard is expected to run on an Apache Hadoop cluster node with configured Apache HBase client. Apache Hadoop and Apache HBase components are not bundled with Halyard. The runtime requirements are:

 * Apache Hadoop version 2.5.1 or higher
 * Apache HBase version 1.1.2 or higher
 * Java 8 Runtime

*Note: Recommended Apache Hadoop distribution is the latest version of Hortonworks Data Platform (HDP) or Amazon Elastic Map Reduce (EMR).*
   
[&#9650;](#)

### Hortonworks Data Platform Sample Cluster Setup

Hortonworks Data Platform is a Hadoop Distribution with all important parts of Hadoop included, however it does not directly provide hardware and core OS.

The whole HDP stack installation through Amabari is very well described at [Hortonworks Data Platform - Apache Ambari Installation page](http://docs.hortonworks.com/HDPDocuments/Ambari-2.4.2.0/bk_ambari-installation/content/index.html).

It is possible to strip down the set of Hadoop components to `HDFS`, `MapReduce2`, `YARN`, `HBase`, `ZooKeeper`, and optionally `Ambari Metrics` for cluster monitoring.

Detailed documentation about Hortonworks Data Platform is accessible from <http://docs.hortonworks.com>


[&#9650;](#)

### Amazon EMR Sample Cluster Setup

Amazon Elastic MapReduce is a service providing both - hardware and software stack to run Hadoop and Halyard on top of it.

Sample Amazon EMR setup is very well described in [Amazon EMR Management Guide - Getting Started](http://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs.html).

For Halyard purpose it is important to perform first two steps of the guide:

 - [Step 1: Set Up Prerequisites](http://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs-prerequisites.html)
 - [Step 2: Launch Your Sample Cluster](http://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs-launch-sample-cluster.html)

It is possible to strip down the set of provided components during `Create Cluster` by clicking on `Go to advanced options` and selecting just `Hadoop`, `ZooKeeper`, `HBase` and optionally `Ganglia` for cluster monitoring.

HBase for Halyard is possible to run in both Storage Modes - `HDFS` or `S3`.

Instance types with redundant storage space (like for example `d2.xlarge`) are highly recommended when you plan to Bulk Load large datasets using Halyard.

Instance types with enough memory and fast disks for local caching (for example `i2.xlarge`) are recommended when the cluster would mainly serve data through Halyard.

Additional EMR Task Nodes can be used to host additional Halyard SPARQL Endpoints.

Detailed documentation about Amazon EMR is available at <https://aws.amazon.com/documentation/emr/>


[&#9650;](#)

See [Documentation](https://merck.github.io/Halyard) for usage examples, architecture information, and more.

## Repository contents

 * `common` - a library for direct mapping between an RDF data model and Apache HBase
 * `strategy` - a generic parallel asynchronous implementation of RDF4J Evaluation Strategy
 * `sail` - an implementation of the RDF4J Storage and Inference Layer on top of Apache HBase
 * `tools` - a set of command line and Apache Hadoop MapReduce tools for loading, updating, querying, and exporting the data with maximum performance
 * `sdk` - a distributable bundle of Eclipse RDF4J and Halyard for command line use on an Apache Hadoop cluster with configured HBase
 * `webapps` - a re-distribution of Eclipse RDF4J Web Applications (RDF4J-Server and RDF4J-Workbench), patched and enhanced to include Halyard as another RDF repository option
