---
title: Overview
layout: doc
---
# Overview

Halyard is an extremely horizontally scalable [Triplestore][] with support for [Named Graphs][], designed for integration of extremely large [Semantic Data Models][], and for storage and [SPARQL 1.1][] querying of the whole [Linked Data][] universe snapshots. Halyard implementation is based on [Eclipse RDF4J][] framework and [Apache HBase][] database, and it is completely written in Java.

[Triplestore]: https://en.wikipedia.org/wiki/Triplestore
[Named Graphs]: https://en.wikipedia.org/wiki/Named_graph
[RDF]: https://en.wikipedia.org/wiki/Resource_Description_Framework
[Semantic Data Models]: https://en.wikipedia.org/wiki/Semantic_data_model
[SPARQL 1.1]: http://www.w3.org/TR/sparql11-query/
[Linked Data]: https://en.wikipedia.org/wiki/Linked_data
[Eclipse RDF4J]: http://rdf4j.org
[Apache HBase]: http://hbase.apache.org

## Build Instructions

Build environment prerequisites are:

 * Linux or Mac computer
 * Java SE Development Kit 8
 * Apache Maven software project management and comprehension tool

In the Halyard project root directory execute command: `mvn package`

Optionally you can build Halyard from NetBeans or other Java Development IDE.

## Runtime Requirements

Halyard is expected to run on an Apache Hadoop cluster node with configured Apache HBase client. Apache Hadoop and Apache HBase components are not bundled with Halyard. The runtime requirements are:

 * Apache Hadoop version 2.5.1 or higher
 * Apache HBase version 1.1.2 or higher
 * Java 8 Runtime

*Note: Recommended Apache Hadoop distribution is Hortonworks Data Platform (HDP) version 2.4.2 or Amazon Elastic Map Reduce (EMR).*

### Hortonworks Data Platform Sample Cluster Setup

Hortonworks Data Platform is a Hadoop Distribution with all important parts of Hadoop included, however it does not directly provide hardware and core OS.

The whole HDP stack installation through Amabari is very well described at [Hortonworks Data Platform - Apache Ambari Installation page](http://docs.hortonworks.com/HDPDocuments/Ambari-2.4.2.0/bk_ambari-installation/content/index.html).

It is possible to strip down the set of Hadoop components to `HDFS`, `MapReduce2`, `YARN`, `HBase`, `ZooKeeper`, and optionally `Ambari Metrics` for cluster monitoring.

Detailed documentation about Hortonworks Data Platform is accessible from <http://docs.hortonworks.com>

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

## Architecture Diagram

![Halyard Architecture Diagram](img/architecture.png){:width="100%"}
