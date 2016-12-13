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

## Architecture Diagram

![Halyard Architecture Diagram](img/architecture.png)

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

*Note: Recommended Apache Hadoop distribution is Hortonworks Data Platform (HDP) version 2.4.2*
