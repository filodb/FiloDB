<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [FiloDB Architecture Overview](#filodb-architecture-overview)
  - [Core](#core)
  - [Cassandra](#cassandra)
  - [Coordinator](#coordinator)
  - [Spark](#spark)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## FiloDB Architecture Overview

![FiloDB Architecture](filodb_architecture.png)

The code is laid out following the different parts and components.

### Coordinator

Provides an upper-level, client-facing interface for the core components, and manages the scheduling around the MemTable, Reprojector, flushes, and column store.  Handles different types of streaming data ingestion.

Clients such as Spark and the CLI implement source actors that extend the [RowSource](coordinator/src/main/scala/filodb.coordinator/RowSource.scala) trait. `RowSource` communicates with the [NodeCoordinatorActor](coordinator/src/main/scala/filodb.coordinator/NodeCoordinatorActor.scala) - one per node - to establish streaming ingestion and send rows of data.  `RowSource` retries rows for which it does not receive an `Ack` such that ingestion is at-least-once, and has built-in backpressure not to try to send too many unacked rows. The `NodeCoordinatorActor` in turn creates a [DatasetCoordinatorActor](coordinator/src/main/scala/filodb.coordinator/DatasetCoordinatorActor.scala) to handle the state of memtables for each (dataset, version) pair, backpressure, and flushing the memtables to the column store.

### Core

These components form the core part of FiloDB and are portable across data stores.

Ingested rows come from the `DatasetCoordinatorActor` into a [MemTable](core/src/main/scala/filodb.core/reprojector/MemTable.scala), of which there is only one implementation currently, the [FiloMemTable](core/src/main/scala/filodb.core/reprojector/FiloMemTable.scala).  MemTables hold enough rows so they can be chunked efficiently.  MemTables are flushed to the columnstore using the [Reprojector](core/src/main/scala/filodb.core/reprojector/Reprojector.scala) based on scheduling and policies set by the `DatasetCoordinatorActor`.  The rows in the `MemTable` form Segments (see [Segment.scala](core/src/main/scala/filodb.core/store/Segment.scala)) and are appended to the [ColumnStore](core/src/main/scala/filodb.core/store/ColumnStore).

The core module has an [InMemoryColumnStore](core/src/main/scala/filodb.core/store/InMemoryColumnStore.scala), a full `ColumnStore` implementation used for both testing and low-latency in-memory Spark queries.

On the read side, the [ColumnStoreScanner](core/src/main/scala/filodb.core/store/ColumnStoreScanner.scala) contains APIs for reading out segments and rows using various `ScanMethod`s - there are ones for single partition queries, queries that span multiple partitions using custom filtering functions, etc.  Helper functions in [KeyFilter](core/src/main/scala/filodb.core/query/KeyFilter.scala) help compose functions for filtered scanning.

FiloDB datasets consists of one or more projections, each of which contains columns.  The [MetaStore](core/src/main/scala/filodb.core/store/MetaStore.scala) defines an API for concurrent reads/writes/updates on dataset, projection, and column metadata.  Each [Column](core/src/main/scala/filodb.core/metadata/Column.scala) has a `ColumnType`, which has a [KeyType](core/src/main/scala/filodb.core/metadata/KeyType.scala).  `KeyType` is a fundamental type class defining serialization and extraction for each type of column/key.  Most of FiloDB depends heavily on [RichProjection](core/src/main/scala/filodb.core/metadata/Projection.scala), which contains the partition, row, and segment key columns and their `KeyType`s.

### Cassandra

An implementation of ColumnStore and MetaStore for Apache Cassandra.

### Spark

Contains the Spark input source for ingesting and querying data from FiloDB.
