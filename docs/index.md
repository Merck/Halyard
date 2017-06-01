---
title: Home
layout: page
---
<div class="jumbotron">
# Halyard

[![CI](https://api.travis-ci.org/Merck/Halyard.svg?branch=master)](https://travis-ci.org/Merck/Halyard)
[![Coverage](https://codecov.io/github/Merck/Halyard/coverage.svg?branch=master)](https://codecov.io/gh/Merck/Halyard/)

Halyard is an extremely horizontally scalable Triplestore with support for Named Graphs, designed for integration of extremely large Semantic Data Models, and for storage and SPARQL 1.1 querying of the whole Linked Data universe snapshots. Halyard implementation is based on Eclipse RDF4J framework and Apache HBase database, and it is completely written in Java.
</div>

<div class="row">
  <div class="col-md-4">
## Run in Amazon EMR

To get started with Halyard, try deploying it on an Amazon Elastic MapReduce cluster.

See [Amazon EMR Sample Cluster Setup](getting-started.html#amazon-emr-sample-cluster-setup).

  </div>

  <div class="col-md-4">
## Run Locally

Download and unzip the latest `halyard-sdk-<version>.zip` bundle from [Releases page](https://github.com/Merck/Halyard/releases) to a Apache Hadoop cluster node with configured Apache HBase client.

Halyard is expected to run on an Apache Hadoop cluster node with configured Apache HBase client. Apache Hadoop and Apache HBase components are not bundled with Halyard. The runtime requirements are:

* Apache Hadoop version 2.5.1 or higher
* Apache HBase version 1.1.2 or higher
* Java 8 Runtime

*Note: Recommended Apache Hadoop distribution is Hortonworks Data Platform (HDP) version 2.4.2 or Amazon Elastic MapReduce (EMR).*

  </div>

  <div class="col-md-4">
## Get involved

* Clone [GitHub Repository](https://github.com/Merck/Halyard)
* See [Open Issues](https://github.com/Merck/Halyard/issues)
* Join [Discussion Group](https://groups.google.com/d/forum/halyard-users)
* Contact author: [Adam Sotona](mailto:adam.sotona@merck.com)

  </div>

</div>
