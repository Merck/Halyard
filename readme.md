# Halyard*

![CI](https://github.com/pulquero/Halyard/actions/workflows/ci.yml/badge.svg)
[![Coverage](https://codecov.io/github/pulquero/Halyard/coverage.svg?branch=latest)](https://codecov.io/gh/pulquero/Halyard/)

**Fork of [Halyard](https://github.com/Merck/Halyard) with support for RDF\*, XPath 3 functions, SPIN functions (and many other improvements/fixes).**

**NB: this fork is not data compatible with the original.**

Halyard is an extremely horizontally scalable triple store with support for named graphs, designed for integration of extremely large semantic data models and for storage and SPARQL 1.1 querying of complete Linked Data universe snapshots. Halyard implementation is based on Eclipse RDF4J framework and Apache HBase database, and it is completely written in Java.

**Documentation: <https://pulquero.github.io/Halyard>**

## Get started

[Download](https://github.com/pulquero/Halyard/releases) and unzip the latest `halyard-sdk-<version>.zip` bundle to a Apache Hadoop cluster node with configured Apache HBase client.

Halyard is expected to run on an Apache Hadoop cluster node with configured Apache HBase client. Apache Hadoop and Apache HBase components are not bundled with Halyard. The runtime requirements are:

 * Apache Hadoop version 2.10 or 3.1.1 or higher
 * Apache HBase version 2.3 or higher
 * Java 8 Runtime

See [Documentation](https://pulquero.github.io/Halyard) for usage examples, architecture information, and more.

## Repository contents

 * `common` - a library for direct mapping between an RDF data model and Apache HBase
 * `strategy` - a generic parallel asynchronous implementation of RDF4J Evaluation Strategy
 * `sail` - an implementation of the RDF4J Storage and Inference Layer on top of Apache HBase
 * `tools` - a set of command line and Apache Hadoop MapReduce tools for loading, updating, querying, and exporting the data with maximum performance
 * `sdk` - a distributable bundle of Eclipse RDF4J and Halyard for command line use on an Apache Hadoop cluster with configured HBase
 * `webapps` - a re-distribution of Eclipse RDF4J Web Applications (RDF4J-Server and RDF4J-Workbench), patched and enhanced to include Halyard as another RDF repository option
