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

See [Documentation](https://merck.github.io/Halyard) for usage examples, architecture information, and more.

## Repository contents

 * `common` - a library for direct mapping between an RDF data model and Apache HBase
 * `strategy` - a generic parallel asynchronous implementation of RDF4J Evaluation Strategy
 * `sail` - an implementation of the RDF4J Storage and Inference Layer on top of Apache HBase
 * `tools` - a set of command line and Apache Hadoop MapReduce tools for loading, updating, querying, and exporting the data with maximum performance
 * `sdk` - a distributable bundle of Eclipse RDF4J and Halyard for command line use on an Apache Hadoop cluster with configured HBase
 * `webapps` - a re-distribution of Eclipse RDF4J Web Applications (RDF4J-Server and RDF4J-Workbench), patched and enhanced to include Halyard as another RDF repository option
