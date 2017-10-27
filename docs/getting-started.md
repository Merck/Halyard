---
title: Getting Started
layout: doc
---

# Overview

Halyard is an extremely horizontally scalable [RDF store](https://en.wikipedia.org/wiki/Triplestore) with support for [named graphs](https://en.wikipedia.org/wiki/Named_graph), designed for storage and integration of extremely large [semantic data models](https://en.wikipedia.org/wiki/Semantic_data_model), and execution of [SPARQL 1.1](http://www.w3.org/TR/sparql11-query) queries of the whole [linked data](https://en.wikipedia.org/wiki/Linked_data) universe snapshots. The implementation of Halyard is based on [Eclipse RDF4J](http://rdf4j.org) framework and the [Apache HBase](http://hbase.apache.org) database, and it is completely written in Java.

## Build instructions

Build environment prerequisites are:

 * Linux or Mac computer
 * Java SE Development Kit 8
 * Apache Maven software project management and comprehension tool

In the Halyard project root directory execute the command: `mvn package`

Optionally, you can build Halyard from NetBeans or other Java development IDE.

## Runtime requirements

Halyard is expected to run on an Apache Hadoop cluster node with a configured Apache HBase client. Apache Hadoop and Apache HBase components are not bundled with Halyard. The runtime requirements are:

 * Apache Hadoop version 2.5.1 or higher
 * Apache HBase version 1.1.2 or higher
 * Java 8 Runtime

*Note: Recommended Apache Hadoop distribution is Hortonworks Data Platform (HDP) version 2.4.2 or Amazon Elastic Map Reduce (EMR).*

### Hortonworks Data Platform sample cluster setup

Hortonworks Data Platform (HDP) is a Hadoop distribution with all important parts of Hadoop included, however, it does not directly provide the hardware and core OS.

The whole HDP stack installation through Amabari is described in [Hortonworks Data Platform - Apache Ambari Installation page](http://docs.hortonworks.com/HDPDocuments/Ambari-2.4.2.0/bk_ambari-installation/content/index.html).

It is possible to strip down the set of Hadoop components to `HDFS`, `MapReduce2`, `YARN`, `HBase`, `ZooKeeper`, and optionally `Ambari Metrics` for cluster monitoring.

Detailed documentation about Hortonworks Data Platform is accessible at <http://docs.hortonworks.com>.

### Amazon EMR sample cluster setup

Amazon Elastic MapReduce is a service providing both hardware and software stack to run Hadoop and Halyard on top of it.

Sample Amazon EMR setup is described in [Amazon EMR Management Guide - Getting Started](http://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs.html).

For the purpose of Halyard it is important to perform the first two steps of the guide:

 - [Step 1: Set up prerequisites](http://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs-prerequisites.html)
 - [Step 2: Launch your sample cluster](http://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs-launch-sample-cluster.html)

It is possible to strip down the set of provided components during `Create Cluster` by clicking on `Go to advanced options` and selecting just `Hadoop`, `ZooKeeper`, `HBase` and optionally `Ganglia` for cluster monitoring.

HBase for Halyard can run in both storage modes: `HDFS` or `S3`.

Instance types with redundant storage space, such as `d2.xlarge`, are highly recommended when you plan to bulk load large datasets using Halyard.

Instance types with enough memory and fast disks for local caching, such as `i2.xlarge`, are recommended when the cluster would mainly serve data through Halyard.

Additional EMR Task Nodes can be used to host additional Halyard SPARQL endpoints.

A detailed documentation of the Amazon EMR is available at <https://aws.amazon.com/documentation/emr>.
