---
title: Namespace
layout: doc
---

This is the home of the Halyard namespace:

```
PREFIX halyard: <http://merck.github.io/Halyard/ns#>
```

## In-data used identifiers

### <a id="statsContext" href="http://merck.github.io/Halyard/ns#statsContext">`halyard:statsContext`</a>

A named graph context used by Halyard to store dataset statistics.

### <a id="statsRoot" href="http://merck.github.io/Halyard/ns#statsRoot">`halyard:statsRoot`</a>

Statistics dataset root node within the [`halyard:statsContext`](#statsContext) named graph context.

### <a id="namespacePrefix" href="http://merck.github.io/Halyard/ns#namespacePrefix">`halyard:namespacePrefix`</a>

Property used by Halyard to persist namespaces inside the datasets.

## Custom SPARQL filter functions

### <a id="forkAndFilterBy" href="http://merck.github.io/Halyard/ns#forkAndFilterBy">`halyard:forkAndFilterBy(<num_of_forks>, ?var1 [,?varN...])`</a>

Custom SPARQL function used in [Halyard Bulk Update](tools#Halyard_Bulk_Update) and [Halyard Bulk Export](tools#Halyard_Bulk_Export).

### ~~<a id="parallelSplitBy" href="http://merck.github.io/Halyard/ns#parallelSplitBy">`halyard:parallelSplitBy`</a>~~

~~Custom SPARQL function used in [Halyard Parallel Export](tools#Halyard_Parallel_Export).~~

### ~~<a id="decimateBy" href="http://merck.github.io/Halyard/ns#decimateBy">`halyard:decimateBy`</a>~~

## Custom data types

### <a id="search" href="http://merck.github.io/Halyard/ns#search">`halyard:search`</a>

Custom data type passing the value as a query string to Elastic Search index (when configured). The value is replaced with all matching values retrieved from Elastic Search index during SPARQL query, during direct API repository operations, or during RDF4J Workbench exploration of the datasets.  

## SAIL configuration predicates

### <a id="tableName" href="http://merck.github.io/Halyard/ns#tableName">`halyard:tableName`</a>

### <a id="splitBits" href="http://merck.github.io/Halyard/ns#splitBits">`halyard:splitBits`</a>

### <a id="createTable" href="http://merck.github.io/Halyard/ns#createTable">`halyard:createTable`</a>

### <a id="pushStrategy" href="http://merck.github.io/Halyard/ns#pushStrategy">`halyard:pushStrategy`</a>

### <a id="evaluationTimeout" href="http://merck.github.io/Halyard/ns#evaluationTimeout">`halyard:evaluationTimeout`</a>
