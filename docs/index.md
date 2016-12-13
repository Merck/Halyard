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
## Build

Build environment prerequisites are:

* Linux or Mac computer
* Java SE Development Kit 8
* Apache Maven software project management and comprehension tool

In the Halyard project root directory execute command: `mvn package` Optionally you can build Halyard from NetBeans or other Java Development IDE.
  </div>

  <div class="col-md-4">
## Install and run

Download and unzip the latest `halyard-sdk-<version>.zip` bundle from [Releases page](https://github.com/Merck/Halyard/releases) to a Apache Hadoop cluster node with configured Apache HBase client.

Halyard is expected to run on an Apache Hadoop cluster node with configured Apache HBase client. Apache Hadoop and Apache HBase components are not bundled with Halyard. The runtime requirements are:

* Apache Hadoop version 2.5.1 or higher
* Apache HBase version 1.1.2 or higher
* Java 8 Runtime

*Note: Recommended Apache Hadoop distribution is Hortonworks Data Platform (HDP) version 2.4.2*
  </div>

  <div class="col-md-4">
## Get involved

* Clone [GitHub Repository](https://github.com/Merck/Halyard)
* See [Open Issues](https://github.com/Merck/Halyard/issues)
* Join [Discussion Group](https://groups.google.com/d/forum/halyard-users)
* Contact author: [Adam Sotona](mailto:adam.sotona@merck.com)

</div>

</div>
