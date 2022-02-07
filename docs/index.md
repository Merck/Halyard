---
title: Home
layout: page
---

<div class="jumbotron">

# Halyard-star

![CI](https://github.com/pulquero/Halyard/actions/workflows/ci.yml/badge.svg)
[![Coverage](https://codecov.io/github/pulquero/Halyard/coverage.svg?branch=latest)](https://codecov.io/gh/pulquero/Halyard/)

**Fork of [Halyard](https://github.com/Merck/Halyard) with support for RDF-star, XPath 3 functions, SPIN functions (and many other improvements/fixes).**

**NB: this fork is not data compatible with the original.**

</div>

<div>

Halyard is an extremely horizontally scalable [RDF store](https://en.wikipedia.org/wiki/Triplestore) with support for [named graphs](https://en.wikipedia.org/wiki/Named_graph), designed for storage and integration of extremely large [semantic data models](https://en.wikipedia.org/wiki/Semantic_data_model), and execution of [SPARQL 1.1](http://www.w3.org/TR/sparql11-query) queries of the whole [linked data](https://en.wikipedia.org/wiki/Linked_data) universe snapshots. The implementation of Halyard is based on [Eclipse RDF4J](http://rdf4j.org) framework and the [Apache HBase](http://hbase.apache.org) database, and it is completely written in Java.

</div>

<div class="row">
  <div class="col-md-4">

## Run in Amazon EMR

To get started with Halyard, try deploying it on an Amazon Elastic MapReduce (EMR) cluster.

See the [Amazon EMR sample cluster setup](getting-started.html#amazon-emr-sample-cluster-setup).

## Run locally

Download and unzip the latest `halyard-sdk-<version>.zip` bundle from the [releases page](https://github.com/pulquero/Halyard/releases) to a Apache Hadoop cluster node with a configured Apache HBase client.

Halyard is expected to run on an Apache Hadoop cluster node with configured Apache HBase client. Apache Hadoop and Apache HBase components are not bundled with Halyard. The runtime requirements are:

* Apache Hadoop version 2.10 or higher
* Apache HBase version 2.4 or higher
* Java 8 Runtime

</div>

<div class="col-md-4">

## Get involved

* Read the [documentation](https://pulquero.github.io/Halyard/getting-started.html)
* Download the [binaries](https://github.com/pulquero/Halyard/releases)
* Clone the [GitHub repository](https://github.com/pulquero/Halyard)
* See the [open issues](https://github.com/pulquero/Halyard/issues)

</div>

<div class="col-md-4">

## Articles (original Halyard)

* [Inside Halyard: 1. Triples, Keys, Columns and Values - everything upside down](https://www.linkedin.com/pulse/inside-halyard-1-triples-keys-columns-values-upside-adam-sotona)
* [Inside Halyard: 2. When one working thread is not enough (PUSH versus PULL)](https://www.linkedin.com/pulse/inside-halyard-2-when-one-working-thread-enough-push-versus-sotona)
* [Inside Halyard: 3. Sipping a river through drinking straws](https://www.linkedin.com/pulse/inside-halyard-3-sipping-river-through-drinking-straws-adam-sotona)
* [Inside Halyard: 4. Bulk operations - shifting a mountain](https://www.linkedin.com/pulse/inside-halyard-4-bulk-operations-shifting-mountain-adam-sotona)
* [Inside Halyard: 5. SPARQL Federation without border controls](https://www.linkedin.com/pulse/inside-halyard-5-sparql-federation-without-border-controls-sotona)
* [Inside Halyard: 6. Statistics-based acceleration of SPARQL queries](https://www.linkedin.com/pulse/inside-halyard-6-statistics-based-acceleration-sparql-adam-sotona)
* [Sailing Halyard Over Three Major Clouds](https://www.linkedin.com/pulse/sailing-halyard-over-three-major-clouds-adam-sotona/)
* [Halyard Tips&Tricks - Dynamically Scaled Cloud Cluster Disk Space Issue](https://www.linkedin.com/pulse/halyard-tipstricks-dynamically-scaled-cloud-cluster-adam-sotona)
* [Halyard Tips&Tricks - Heavy Load-balanced SPARQL Endpoint](https://www.linkedin.com/pulse/halyard-tipstricks-heavy-load-balanced-sparql-endpoint-adam-sotona)
* [Halyard Tips&Tricks - Advanced Literal Search Techniques](https://www.linkedin.com/pulse/halyard-tipstricks-advanced-literal-search-adam-sotona)
* [Halyard Tips&Tricks - Jenkins as a Job Orchestrator](https://www.linkedin.com/pulse/halyard-tipstricks-jenkins-job-orchestrator-adam-sotona)
* [Halyard Tips&Tricks - Trillion Statements Challenge](https://www.linkedin.com/pulse/halyard-tipstricks-trillion-statements-challenge-adam-sotona)

</div>
