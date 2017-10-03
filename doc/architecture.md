<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [FiloDB Architecture and Code Overview](#filodb-architecture-and-code-overview)
  - [Coordinator](#coordinator)
  - [Core](#core)
  - [Cassandra](#cassandra)
  - [Spark](#spark)
  - [Kafka](#kafka)
  - [HTTP](#http)
  - [Standalone](#standalone)
  - [CLI](#cli)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## FiloDB Architecture and Code Overview

TODO: redo, this is way outdated

![FiloDB Architecture](filodb_architecture.png)

The code is laid out following the different parts and components.

### Coordinator

Provides an upper-level, client-facing interface for the core components, and manages the scheduling around the MemTable, Reprojector, flushes, and column store.  Handles different types of streaming data ingestion.

Clients such as Spark and the CLI implement source actors that extend the [RowSource](../coordinator/src/main/scala/filodb.coordinator/RowSource.scala) trait. `RowSource` communicates with the [NodeCoordinatorActor](../coordinator/src/main/scala/filodb.coordinator/NodeCoordinatorActor.scala) - one per node - to establish streaming ingestion and send rows of data.  `RowSource` retries rows for which it does not receive an `Ack` such that ingestion is at-least-once, and has built-in backpressure not to try to send too many unacked rows. The `NodeCoordinatorActor` in turn creates a [DatasetCoordinatorActor](../coordinator/src/main/scala/filodb.coordinator/DatasetCoordinatorActor.scala) to handle the state of memtables for each (dataset, version) pair, backpressure, and flushing the memtables to the column store.

### Core

These components form the core part of FiloDB and are portable across data stores.  Subcomponents:

* `binaryrecord` - used for supporting efficient, no-serialization, multi-schema partition keys and for serializing rows for Spark ingestion
* `memstore` - a [MemStore](../core/src/main/scala/filodb.core/memstore/MemStore.scala) ingests records, encodes them into columnar chunks, and allows for real-time querying through the `ChunkSource` API.  The current implementation is a [TimeSeriesMemStore](../core/src/main/scala/filodb.core/memstore/TimeSeriesMemStore.scala) which is designed for very high cardinality time series data.  For each dataset it stores one or more shards, each of which may contain many many thousands of [TimeSeriesPartition](../core/src/main/scala/filodb.core/memstore/TimeSeriesPartition.scala) instances.  MemStores also manage persistence of encoded data via `ChunkSink`s.
* `store` - contains the main APIs for persistence and chunk reading, including [ChunkSource](../core/src/main/scala/filodb.core/store/ChunkSourceSink.scala) and `ChunkSink`, as well as the [MetaStore](../core/src/main/scala/filodb.core/store/MetaStore.scala) for metadata persistence.  Most of the APIs are based on reactive streams for backpressure handling.
* `query` - contains aggregation and querying logic built on top of `ChunkSource`s.
* `metadata` - Dataset and Column definitions
* 
FiloDB datasets consists of one or more projections, each of which contains columns.  The [MetaStore](../core/src/main/scala/filodb.core/store/MetaStore.scala) defines an API for concurrent reads/writes/updates on dataset, projection, and column metadata.  Each [Column](../core/src/main/scala/filodb.core/metadata/Column.scala) has a `ColumnType`, which has a [KeyType](../core/src/main/scala/filodb.core/metadata/KeyType.scala).  `KeyType` is a fundamental type class defining serialization and extraction for each type of column/key.  Most of FiloDB depends heavily on [RichProjection](../core/src/main/scala/filodb.core/metadata/Projection.scala), which contains the partition, row, and segment key columns and their `KeyType`s.

### Cassandra

An implementation of ColumnStore and MetaStore for Apache Cassandra.

### Spark

Contains the Spark input source for ingesting and querying data from FiloDB.

### Kafka

Contains the Kafka ingestion source for FiloDB standalone

### HTTP

### Standalone

The standalone module is used for FiloDB real-time direct ingestion (from Kafka) and querying without Spark, for metrics and event use cases.

### CLI

Contains the client CLI for setting up datasets and connecting with a FiloDB standalone cluster for direct querying using PromQL.