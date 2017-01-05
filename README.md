# FiloDB

[![Join the chat at https://gitter.im/velvia/FiloDB](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/velvia/FiloDB?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/filodb/FiloDB.svg?branch=master)](https://travis-ci.org/filodb/FiloDB)

High-performance distributed analytical database + Spark SQL queries + built for streaming.

[filodb-announce](https://groups.google.com/forum/#!forum/filodb-announce) google group
and [filodb-discuss](https://groups.google.com/forum/#!forum/filodb-discuss) google group

```
    _______ __      ____  ____ 
   / ____(_) /___  / __ \/ __ )
  / /_  / / / __ \/ / / / __  |
 / __/ / / / /_/ / /_/ / /_/ / 
/_/   /_/_/\____/_____/_____/  
```

![](Dantat.jpg)

Columnar, versioned layers of data wrapped in a yummy high-performance analytical database engine.

See [architecture](doc/architecture.md) and [datasets and reading](doc/datasets_reading.md) for more information.  Also see the Spark Notebooks under `doc`... there is one for time-series/geo analysis of the NYC Taxi dataset, and one for interactive charting of the GDELT dataset!

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Overview](#overview)
  - [Use Cases](#use-cases)
  - [Anti-use-cases](#anti-use-cases)
  - [Roadmap](#roadmap)
- [Pre-requisites](#pre-requisites)
- [Getting Started](#getting-started)
- [Introduction to FiloDB Data Modelling](#introduction-to-filodb-data-modelling)
  - [Computed Columns](#computed-columns)
  - [FiloDB vs Cassandra Data Modelling](#filodb-vs-cassandra-data-modelling)
  - [Data Modelling and Performance Considerations](#data-modelling-and-performance-considerations)
  - [Predicate Pushdowns](#predicate-pushdowns)
  - [Example FiloDB Schema for machine metrics](#example-filodb-schema-for-machine-metrics)
  - [Distributed Partitioning](#distributed-partitioning)
- [Using FiloDB Data Source with Spark](#using-filodb-data-source-with-spark)
  - [Configuring FiloDB](#configuring-filodb)
    - [Passing Cassandra Authentication Settings](#passing-cassandra-authentication-settings)
  - [Spark Data Source API Example (spark-shell)](#spark-data-source-api-example-spark-shell)
  - [Spark/Scala/Java API](#sparkscalajava-api)
  - [Spark Streaming Example](#spark-streaming-example)
  - [SQL/Hive Example](#sqlhive-example)
  - [Querying Datasets](#querying-datasets)
- [Using the CLI](#using-the-cli)
  - [Running the CLI](#running-the-cli)
  - [CLI Example](#cli-example)
- [Current Status](#current-status)
  - [Upcoming version 0.5 changes:](#upcoming-version-05-changes)
  - [Version 0.4 change list:](#version-04-change-list)
- [Deploying](#deploying)
- [Monitoring and Metrics](#monitoring-and-metrics)
  - [Metrics Sinks](#metrics-sinks)
  - [Metrics Configuration](#metrics-configuration)
- [Code Walkthrough](#code-walkthrough)
- [Building and Testing](#building-and-testing)
- [You can help!](#you-can-help)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Overview

FiloDB is a new open-source distributed, versioned, and columnar analytical database designed for modern streaming workloads.

* **High performance** - competitive with Parquet scan speeds, plus filtering along two or more dimensions
  - Very flexible filtering: filter on only part of a partition key, much more flexible than allowed in Cassandra
  - Much faster bulk ingestion than raw Cassandra tables
* **Compact storage** - within 35% of Parquet for CassandraColumnStore
  - Up to 27x more data stored per GB, compared to Cassandra 2.x, in real world fact table storage
  - See the blog post on [Apache Cassandra for analytics: a performance and storage analysis](https://www.oreilly.com/ideas/apache-cassandra-for-analytics-a-performance-and-storage-analysis)
* **Idempotent writes** - primary-key based appends and updates; easy exactly-once ingestion from streaming sources
* **Distributed** - pluggable storage engine includes Apache Cassandra and in-memory
* **Low-latency** - minimal SQL query latency of 15ms on one node; sub-second easily achievable with filtering and easy to use concurrency control
  - See post on [700 SQL Queries per Second in Apache Spark with FiloDB](http://velvia.github.io/Spark-Concurrent-Fast-Queries/)
* **SQL queries** - plug in Tableau or any tool using JDBC/ODBC drivers
* Ingest from Spark/Spark Streaming from any supported Spark data source

[Overview presentation](http://velvia.github.io/presentations/2015-filodb-spark-streaming/#/) -- see the docs folder for design docs.

To compile the .mermaid source files to .png's, install the [Mermaid CLI](http://knsv.github.io/mermaid/mermaidCLI.html).

### Use Cases

* Storage and analysis of streaming event / time series data
* Data warehousing
* In-memory database for Spark Streaming analytics
* Low-latency in-memory SQL database engine

### Anti-use-cases

* Heavily transactional, update-oriented workflows

### Roadmap

Your input is appreciated!

NOTE: Please beware that significant storage-layer changes are taking place.  For a stable version, please use the `v0.4` release/tag. At the next release, the storage layer should be stable for production use. 

* Spark 2.0 and Scala 2.11 - coming soon
* Kafka input API / connector (without needing Spark)
* In-memory caching for significant query speedup
* True columnar querying and execution, using late materialization and vectorization techniques.  GPU/SIMD.
* Projections.  Often-repeated queries can be sped up significantly with projections.

## Pre-requisites

1. [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
2. [SBT](http://www.scala-sbt.org/) to build
3. [Apache Cassandra](http://cassandra.apache.org/) 2.x or 3.x (We prefer using [CCM](https://github.com/pcmanus/ccm) for local testing) (Optional if you are using the in-memory column store)
4. [Apache Spark (1.6.x)](http://spark.apache.org/) - 2.0 coming soon

## Getting Started

1. Clone the project and cd into the project directory,

    ```
    $ git clone https://github.com/tuplejump/FiloDB.git
    $ cd FiloDB
    ```

    - It is recommended you use the last stable released version.
    - To build, run `filo-cli` (see below) and also `sbt spark/assembly`.

2. Choose either the Cassandra column store (default) or the in-memory column store.
    - Start a Cassandra Cluster.
    - Copy `core/src/main/resources/filodb-defaults.conf` and modify the Cassandra settings for your cluster.  This step and passing in a custom config may be skipped for a localhost Cassandra cluster with no auth.
    - Or, use FiloDB's in-memory column store with Spark (does not work with CLI). Pass the `--conf spark.filodb.store=in-memory` to `spark-submit` / `spark-shell`.  This is a great option to test things out, and is really really fast!

3. For Cassandra, update the `keyspace-replication-options` config, then run `filo-cli -Dconfig.file=/path/to/my/filo.conf --command init` to initialize the default `filodb_admin` keyspace.   In addition, you should use CQLSH to create any additional keyspaces you desire to store FiloDB datasets in.
4. Dataset creation can be done using `filo-cli` or using Spark Shell / Scala/Java API.
5. Inserting data can be done using `filo-cli` (CSV only), using Spark SQL/JDBC (INSERT INTO), or the Spark Shell / Scala / Java API.
6. Querying is done using Spark SQL/JDBC or Scala/Java API.
7. Listing/deleting/maintenance can be done using `filo-cli`.  If using Cassandra, `cqlsh` can also be used to inspect metadata.

Note: There is at least one release out now, tagged via Git and also located in the "Releases" tab on Github.

## Introduction to FiloDB Data Modelling

Perhaps it's easiest by starting with a diagram of how FiloDB stores data.

<table>
  <tr>
    <td></td>
    <td colspan="2">Column A</td>
    <td colspan="2">Column B</td>
  </tr>
  <tr>
    <td>Partition key 1</td>
    <td>Segment 1</td>
    <td>Segment 2</td>
    <td>Segment 1</td>
    <td>Segment 2</td>
  </tr>
  <tr>
    <td>Partition key 2</td>
    <td>Segment 1</td>
    <td>Segment 2</td>
    <td>Segment 1</td>
    <td>Segment 2</td>
  </tr>
</table>

Three types of key define the data model of a FiloDB table.

1. **partition key** - decides how data is going to be distributed across the cluster. All data within one partition key is guaranteed to fit on one node. May consist of multiple columns.
2. **segment key** - groups row values into efficient chunks.  Segments within a partition are sorted by segment key and range scans can be done over segment keys.   
1. **row key**     - acts as a primary key within each partition and decides how data will be sorted within each segment.  May consist of multiple columns.

The PRIMARY KEY for FiloDB consists of (partition key, segment_key, row key).  When choosing the above values you must make sure the combination of the three are unique.  If any component of a primary key contains a null value, then a default value will be substituted.

Specifying the partitioning column is optional.  If a partitioning column is not specified, FiloDB will create a default one with a fixed value, which means everything will be thrown into one node, and is only suitable for small amounts of data.  If you don't specify a partitioning column, then you have to make sure combination of segment key and row key values are all unique.

For examples of data modeling and choosing keys, see the examples below as well as [datasets](doc/datasets_reading.md).

For additional information refer to Data Modeling and Performance Considerations.

### Computed Columns

You may specify a function, or computed column, for use with any key column, except for row keys.  This is especially useful for computing a good segment key, or hashing values to generate a good partition key.

| Name      | Description     | Example     |
| :-------- | :-------------- | :---------- |
| string    | returns a constant string value | `:string /0` |
| getOrElse | returns default value if column value is null.  This is not needed most of the time as FiloDB will use a default value in case of null, though `:getOrElse` may help with key uniqueness.   NOTE: do not use the default value null for strings. | `:getOrElse columnA ---` |
| round     | rounds down a numeric column.  Useful for bucketing by time or bucketing numeric IDs.  | `:round timestamp 10000` |
| stringPrefix | takes the first N chars of a string; good for partitioning | `:stringPrefix token 4` |
| hash      | hashes keys of any type to an int between 0 and N | `:hash customerID 400` | 
| timeslice | bucketizes a Long (millisecond) or Timestamp column using duration strings - 500ms, 5s, 10m, 3h, etc. | `:timeslice arrivalTime 30s` |
| monthOfYear | return 1 to 12 (IntColumn) for the month number of a Long (millisecond) or Timestamp column | `:monthOfYear pickup_datetime` |

### FiloDB vs Cassandra Data Modelling

* Like Cassandra, partitions (physical rows) distribute data and clustering keys act as a primary key within a partition
* Like Cassandra, a single partition is the smallest unit of parallelism when querying from Spark
* Wider rows work better for FiloDB (bigger chunk/segment size)
* FiloDB does not have Cassandra's restrictions for partition key filtering. You can filter by any partition keys with most operators.  This means less tables in FiloDB can match more query patterns.
* Cassandra range scans over clustering keys is available over the segment key

### Data Modelling and Performance Considerations

**Choosing Partition Keys**.

- Partition keys are the most efficient way to filter data.  Remember that, unlike Cassandra, FiloDB is able to efficiently filter any column in a partition key -- even string contains, IN on only one column.  It can do this because FiloDB pre-scans a much smaller table ahead of scanning the main columnar chunk table.  This flexibility means that there is no need to populate different tables with different orders of partition keys just to optimize for different queries.
- If there are too few partitions, then FiloDB will not be able to distribute and parallelize reads.
- If the numer of rows in each partition is too few, then the storage will not be efficient.
- If the partition key is time based, there may be a hotspot in the cluster as recent data is all written into the same set of replicas, and likewise for read patterns as well.
- Consider picking a column or group of columns with low cardinality, and has good distribution so that data is distributed across the cluster.
- Consider only those columns that do not get updated. Since partition key is part of primary key, partition key columns cannot get updated.

**Segment Key and Chunk Size**.

Within each partition, data is delineated by the segment key into *segments*.  Within each segment, successive flushes of the MemTable writes data in *chunks*.  The segmentation is key to sorting and filtering data within partitions, and the chunk size (which depends on segmentation) also affects performance.  The smaller the chunk size, the higher the overhead of scanning data becomes.

Segmentation and chunk size distribution may be checked by the CLI `analyze` command.  In addition, the following configuration affects segmentation and chunk size:
* `memtable.max-rows-per-table`, `memtable.flush-trigger-rows` affects how many rows are kept in the MemTable at a time, and this along with how many partitions are in the MemTable directly leads to the chunk size upon flushing.
* The segment size is directly controlled by the segment key.  Choosing a segment key that groups data into big enough chunks (at least 1000 is a good guide) is highly recommended.  Experimentation along with running `filo-cli analyze` is recommended to come up with a good segment key.  See the Spark ingestion of GDELT below on an example... choosing an inappropriate segment key leads to MUCH slower ingest and read performance.
* `chunk_size` option when creating a dataset caps the size of a single chunk.
* Avoid picking any column that has the possibility of getting updated as a segment key column.
* Consider a low cardinal column within partition for segment key. Ideal segment key will hold at least 1000 values in a chunk as explained above. Use a computed column like `:string /0` as the segment key if there are no good candidates available or your chosen segment key has potential to hold very few values in chunks under each segment key.
* Consider moving one of the partition keys as segment keys if your partition is not too wide.  Use the `analyze` filo-cli command to discover partition size.
* Cosider creating computed columns to make good segment key. Ex: Rounding date to month etc.
* Ideal segment key would have chunks filled with thousands of values and frequently gets used as filter in queries.

### Predicate Pushdowns

To help with planning, here is an exact list of the predicate pushdowns (in Spark) that help with reducing I/O and query times:

* Partition key column(s): =, IN on any partition key column
  * = on every partition key results in a **single-partition** query
  * = or IN on every partition key results in a **multi-partition** query, still faster than full table scan
  * Otherwise, a filtered full table scan results - partitions not matching predicates will not be scanned
* Segment key:  must be of the form `segmentKey >/>= value AND segmentKey </<= value` or `segmentKey = value`
  Segment Key predicates will pushdown to cassandra if your storage engine is in Cassandra. FiloDB segment keys map to cluster key of the underlying cassandra storage.

Note: You can see predicate pushdown filters in application logs by setting logging level to INFO.

### Example FiloDB Schema for machine metrics

This is one way I would recommend setting things up to take advantage of FiloDB.

The metric names are the column names.  This lets you select on just one metric and effectively take advantage of columnar layout.

* Partition key = hostname
* Row key = timestamp (say millis)
* Segment key = `:round timestamp 10000` (Timestamp rounded to nearest 10000 millis or 10 seconds)
* Columns: hostname, timestamp, CPU, load_avg, disk_usage, etc.

You can add more metrics/columns over time, but storing each metric in its own column is FAR FAR more efficient, at least in FiloDB.   For example, disk usage metrics are likely to have very different numbers than load_avg, and so Filo can optimize the storage of each one independently.  Right now I would store them as ints and longs if possible.

With the above layout, as long as there aren’t too many hostnames, set the memtable max size and flush trigger to both high numbers, you should get good read performance.  Queries that would work well for the above layout:

- SELECT avg(load_avg), min(load_avg), max(load_avg) FROM metrics WHERE timestamp > t1 AND timestamp < t2
etc.

Queries that would work well once we expose a local Cassandra query interface:
- Select metrics from one individual host

Another possible layout is something like this:

* Partition key = hostname % 1024 (or pick your # of shards)
* Row key = hostname, timestamp

### Distributed Partitioning

FiloDB will automatically form a cluster (via Akka Cluster), divide the range of partition keys amongst the nodes using a consistent-hashing algorithm, and re-route incoming records to the right ingestion node.  Thus, users no longer need to sort their incoming data in Spark.

## Using FiloDB Data Source with Spark

FiloDB has a Spark data-source module - `filodb.spark`. So, you can use the Spark Dataframes `read` and `write` APIs with FiloDB. To use it, follow the steps below

1. Start Cassandra and update project configuration if required.
2. From the FiloDB project directory, execute,
   ```
   $ sbt clean
   $ ./filo-cli --command init
   $ sbt spark/assembly
   ```
3. Use the jar `FiloDB/spark/target/scala-2.10/filodb-spark-assembly-0.4.jar` with Spark 1.6.x.

The options to use with the data-source api are:

| option           | value                                                            | command    | optional |
|------------------|------------------------------------------------------------------|------------|----------|
| dataset          | name of the dataset                                              | read/write | No       |
| database         | name of the database to use for the dataset.  For Cassandra, defaults to `filodb.cassandra.keyspace` config.  | read/write | Yes |
| row_keys         | comma-separated list of column name(s) to use for the row primary key within each partition.  Computed columns are not allowed.  | write      | No if mode is OverWrite or creating dataset for first time  |
| segment_key      | name of the column (could be computed) to use to group rows into segments in a partition.   | write      | yes - defaults to `:string /0` |
| partition_keys   | comma-separated list of column name(s) or computed column functions to use for the partition key.  If not specified, defaults to `:string /0` (a single partition).  | write      | Yes      |
| splits_per_node  | number of read threads per node, defaults to 4 | read | Yes |
| reset_schema     | If true, allows dataset schema (eg partition keys) to be redefined for an existing dataset when SaveMode.Overwrite is used.  Defaults to false.  | write | Yes |
| chunk_size       | Max number of rows to put into one chunk.  Note that this only has an effect if the dataset is created for the first time.| write | Yes |
| flush_after_write | initiates a memtable flush after Spark INSERT / DataFrame.write;  this ensures all the rows are flushed to ColumnStore.  Might want to be turned off for streaming  | write | yes - default true |
| version          | numeric version of data to write, defaults to 0  | read/write | Yes |

Partitioning columns could be created using an expression on the original column in Spark:

    val newDF = df.withColumn("partition", df("someCol") % 100)

or even UDFs:

    val idHash = sqlContext.udf.register("hashCode", (s: String) => s.hashCode())
    val newDF = df.withColumn("partition", idHash(df("id")) % 100) 

However, note that the above methods will lead to a physical column being created, so use of computed columns is probably preferable.

### Configuring FiloDB

Some options must be configured before starting the Spark Shell or Spark application. FiloDB executables are invoked by spark application. These configuration settings can be tuned as per the needs of individual application invoking filoDB executables. There are two methods:

1. Modify the `application.conf` and rebuild, or repackage a new configuration.
2. Override the built in defaults by setting SparkConf configuration values, preceding the filo settings with `spark.filodb`.  For example, to change the default keyspace, pass `--conf spark.filodb.cassandra.keyspace=mykeyspace` to Spark Shell/Submit.  To use the fast in-memory column store instead of Cassandra, pass `--conf spark.filodb.store=in-memory`.
3. It might be easier to pass in an entire configuration file to FiloDB.  Pass the java option `-Dfilodb.config.file=/path/to/my-filodb.conf`, for example using `--java-driver-options`.

Note that if Cassandra is kept as the default column store, the keyspace can be changed on each transaction by specifying the `database` option in the data source API, or the database parameter in the Scala API.

For a list of all configuration params as well as a template for a config file, see the `filodb_defaults.conf` file included in the source and packaged with the jar as defaults.

For metrics system configuration, see the metrics section below.

#### Passing Cassandra Authentication Settings

Typically, you have per-environment configuration files, and you do not want to check in username and password information.  Here are ways to pass in authentication settings:

* Pass in the credentials on the command line.
  - For Spark, `--conf spark.filodb.cassandra.username=XYZ` etc.
  - For CLI, other apps, pass in JVM args: `-Dfilodb.cassandra.username=XYZ`
* Put the credentials in a local file on the host, and refer to it from your config file.   In your config file, do `include "/usr/local/filodb-cass-auth.properties"`.  The properties file would look like:

        filodb.cassandra.username=XYZ
        filodb.cassandra.password=AABBCC

### Spark Data Source API Example (spark-shell)

You can follow along using the [Spark Notebook](http://github.com/andypetrella/spark-notebook)... launch the notebook using `EXTRA_CLASSPATH=$FILO_JAR ADD_JARS=$FILO_JAR ./bin/spark-notebook &` where `FILO_JAR` is the path to `filodb-spark-assembly` jar.  See the [FiloDB_GDELT](doc/FiloDB_GDELT.snb) notebook to follow the GDELT examples below, or the [NYC Taxi](doc/FiloDB_Taxi_Geo_demo.snb) notebook for some really neat time series/geo analysis!

Or you can start a spark-shell locally,

```bash
bin/spark-shell --jars ../FiloDB/spark/target/scala-2.10/filodb-spark-assembly-0.4.jar --packages com.databricks:spark-csv_2.10:1.4.0 --driver-memory 3G --executor-memory 3G
```

Loading CSV file from Spark:

```scala
scala> val csvDF = sqlContext.read.format("com.databricks.spark.csv").
           option("header", "true").option("inferSchema", "true").
           load("../FiloDB/GDELT-1979-1984-100000.csv")
```

Creating a dataset from a Spark DataFrame,
```scala
scala> import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SaveMode

scala> csvDF.write.format("filodb.spark").
             option("dataset", "gdelt").
             option("row_keys", "GLOBALEVENTID").
             option("segment_key", ":round GLOBALEVENTID 10000").
             option("partition_keys", "MonthYear").
             mode(SaveMode.Overwrite).save()
```

Above, we partition the GDELT dataset by MonthYear, creating roughly 72 partitions for 1979-1984, with the unique GLOBALEVENTID used as a row key.  We group every 10000 eventIDs into a segment using the convenient `:round` computed column (GLOBALEVENTID is correlated with time, so in this case we could pack segments with consecutive EVENTIDs). You could use multiple columns for the partition or row keys, of course.  For example, to partition by country code and year instead:

```scala
scala> csvDF.write.format("filodb.spark").
             option("dataset", "gdelt_by_country_year").
             option("row_keys", "GLOBALEVENTID").
             option("segment_key", ":string 0").
             option("partition_keys", "Actor2CountryCode,Year").
             mode(SaveMode.Overwrite).save()
```

Note that in the above case, since events are spread over a much larger number of partitions, it no longer makes sense to use GLOBALEVENTID as a segment key - at least with the original 10000 as a rounding factor.  There are very few events for a given country and year within the space of 10000 event IDs, leading to inefficient storage.  Instead, we use a single segment for each partition.  We probably could have used `:round GLOBALEVENTID 500000` or some other bigger factor as well.  Using `:round GLOBALEVENT 10000` lead to 3x slower ingest and at least 5x slower reads.

The key definitions can be left out for appends:

```scala
sourceDataFrame.write.format("filodb.spark").
                option("dataset", "gdelt").
                mode(SaveMode.Append).save()
```

Note that for efficient columnar encoding, wide rows with fewer partition keys are better for performance.

By default, data is written to replace existing records with the same primary key.  To turn this primary key replacement off for faster ingestion, set `filodb.reprojector.bulk-write-mode` to `true`.
 
Reading the dataset,
```
val df = sqlContext.read.format("filodb.spark").option("dataset", "gdelt").load()
```

The dataset can be queried using the DataFrame DSL. See the section [Querying Datasets](#querying-datasets) for examples.

Note: For your production data loads sort the data frame before saving to FiloDB when the data source is not Cassandra. This will ensure to efficiently load segment chunks. Refer to [Distributed Partitioning](#distributed-partitioning) for additional info.

### Spark/Scala/Java API

There is a more typesafe API than the Spark Data Source API.

```scala
import filodb.spark._
sqlContext.saveAsFilo(df, "gdelt",
                      rowKeys = Seq("GLOBALEVENTID"),
                      segmentKey = ":round GLOBALEVENTID 10000",
                      partitionKeys = Seq("MonthYear"))
```

The above creates the gdelt table based on the keys above, and also inserts data from the dataframe df.

NOTE: If you are running Spark Shell in DSE, you might need to do `import _root_.filodb.spark._`.

Please see the ScalaDoc for the method for more details -- there is a `database` option for specifying the Cassandra keyspace, and a `mode` option for specifying the Spark SQL SaveMode.

There is also an API purely for inserting data... after all, specifying the keys is not needed when inserting into an existing table.

```scala
import filodb.spark._
sqlContext.insertIntoFilo(df, "gdelt")
```

The API for creating a DataFrame is also much more concise:

```scala
val df = sqlContext.filoDataset("gdelt")
val df2 = sqlContext.filoDataset("gdelt", database = Some("keyspace2"))
```

The above method calls rely on an implicit conversion. From Java, you would need to create a new `FiloContext` first:

```java
FiloContext fc = new filodb.spark.FiloContext(sqlContext);
fc.insertIntoFilo(df, "gdelt");
```

### Spark Streaming Example

It's not difficult to ingest data into FiloDB using Spark Streaming.  Simple use `foreachRDD` on your `DStream` and then [transform each RDD into a DataFrame](https://spark.apache.org/docs/latest/streaming-programming-guide.html#dataframe-and-sql-operations).

For an example, see the [StreamingTest](spark/src/test/scala/filodb.spark/StreamingTest.scala).

### SQL/Hive Example

Start Spark-SQL:

```bash
  bin/spark-sql --jars path/to/FiloDB/spark/target/scala-2.10/filodb-spark-assembly-0.4.jar
```

(NOTE: if you want to connect with a real Hive Metastore, you should probably instead start the thrift server, also adding the `--jars` above, and then start the `spark-beeline` client)

Create a temporary table using an existing dataset,

```sql
  create temporary table gdelt
  using filodb.spark
  options (
   dataset "gdelt"
 );
```

Then, start running SQL queries!

You probably want to create a permanent Hive Metastore entry so you don't have to run `create temporary table` every single time at startup:

```sql
  CREATE TABLE gdelt using filodb.spark options (dataset "gdelt");
```

Once this is done, you could insert data using SQL syntax:

```sql
  INSERT INTO TABLE gdelt SELECT * FROM othertable;
```

Of course, this assumes `othertable` has a similar schema.

### Querying Datasets

Now do some queries, using the DataFrame DSL:

```scala
scala> df.select(count(df("MonthYear"))).show()
...<skipping lots of logging>...
COUNT(MonthYear)
4037998
```

or SQL, to find the top 15 events with the highest tone:

```scala
scala> df.registerTempTable("gdelt")

scala> sqlContext.sql("SELECT Actor1Name, Actor2Name, AvgTone FROM gdelt ORDER BY AvgTone DESC LIMIT 15").collect()
res13: Array[org.apache.spark.sql.Row] = Array([208077.29634561483])
```

Now, how about something uniquely Spark .. feed SQL query results to MLLib to compute a correlation:

```scala
scala> import org.apache.spark.mllib.stat.Statistics

scala> val numMentions = df.select("NumMentions").map(row => row.getInt(0).toDouble)
numMentions: org.apache.spark.rdd.RDD[Double] = MapPartitionsRDD[100] at map at DataFrame.scala:848

scala> val numArticles = df.select("NumArticles").map(row => row.getInt(0).toDouble)
numArticles: org.apache.spark.rdd.RDD[Double] = MapPartitionsRDD[104] at map at DataFrame.scala:848

scala> val correlation = Statistics.corr(numMentions, numArticles, "pearson")
```

Notes: You can also query filoDB tables using Spark thrift server. Refer to [SQL/Hive Example](#sqlhive-example) for additional information regarding thrift server. 

FiloDB logs can be viewed in corresponding spark application logs by setting appropriate settings in `log4j.properties`, or `logback.xml` for DSE.

## Using the CLI

The `filo-cli` accepts arguments as key-value pairs. The following keys are supported:

| key          | purpose                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
|--------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| dataset    | It is required for all the operations. Its value should be the name of the dataset                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| database   | Specifies the "database" the dataset should operate in.  For Cassandra, this is the keyspace.  If not specified, uses config value.  |
| limit      | This is optional key to be used with `select`. Its value should be the number of rows required.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| columns    | This is required for defining the schema of a dataset. Its value should be a comma-separated string of the format, `column1:typeOfColumn1,column2:typeOfColumn2` where column1 and column2 are the names of the columns and typeOfColumn1 and typeOfColumn2 are one of `int`,`long`,`double`,`string`,`bool`                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| rowKeys | This is required for defining the row keys. Its value should be comma-separated list of column names or computed column functions to make up the row key                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| segmentKey | The column name or computed column for the segment key |
| partitionKeys | Comma-separated list of column names or computed columns to make up the partition key |
| command    | Its value can be either of `init`, `create`, `importcsv`, `analyze`, `delete`, `truncate` or `list`.<br/><br/>The `init` command is used to create the FiloDB schema.<br/><br/>The `create` command is used to define new a dataset. For example,<br/>```./filo-cli --command create --dataset playlist --columns id:int,album:string,artist:string,title:string --rowKeys id --segmentKey ':string /0' ``` <br/>Note: The sort column is not optional.<br/><br/>The `list` command can be used to view the schema of a dataset. For example, <br/>```./filo-cli --command list --dataset playlist```<br/><br/>The `importcsv` command can be used to load data from a CSV file into a dataset. For example,<br/>```./filo-cli --command importcsv --dataset playlist --filename playlist.csv```<br/>Note: The CSV file should be delimited with comma and have a header row. The column names must match those specified when creating the schema for that dataset.<br>
The `delete` command is used to delete datasets, like a drop.<br>
`truncate` truncates data for an existing dataset to 0. |
| select     | Its value should be a comma-separated string of the columns to be selected,<br/>```./filo-cli --dataset playlist --select album,title```<br/>The result from `select` is printed in the console by default. An output file can be specified with the key `--outfile`. For example,<br/>```./filo-cli --dataset playlist --select album,title --outfile playlist.csv```                                                                                                                                                                                                                                                                                                                                                                                                                   |
| delimiter  | This is optional key to be used with `importcsv` command. Its value should be the field delimiter character. Default value is comma.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| numSegments | The maximum number of segments to analyze for the analyze command.  Prevents analyze of large tables from taking too long. Defaults to 10000. |
| timeoutSeconds | The number of seconds for timeout for initialization, table creation, other quick things  |

### Running the CLI

You may want to customize a configuration to point at your Cassandra cluster, or change other configuration parameters.  The easiest is to pass in a customized config file:

    ./filo-cli -Dfilodb.config.file=/path/to/myfilo.conf --command init

You may also set the `FILO_CONFIG_FILE` environment var instead, but any `-Dfilodb.config.file` args passed in takes precedence.

Individual configuration params may also be changed by passing them on the command line.  They must be the first arguments passed in.  For example:

    ./filo-cli -Dfilodb.columnstore.segment-cache-size=10000 --command ingestcsv ....

All `-D` config options must be passed before any other arguments.

You may also configure CLI logging by copying `cli/src/main/resources/logback.xml` to your deploy folder, customizing it, and passing on the command line `-Dlogback.configurationFile=/path/to/filo-cli-logback.xml`.

You can also change the logging directory by setting the FILO_LOG_DIR environment variable before calling the CLI.

NOTE: The CLI currently only operates on the Cassandra column store.  The `--database` option may be used to specify which keyspace to operate on.  If the keyspace is not initialized, then FiloDB code will automatically create one for you, but you may want to create it yourself to control the options that you want.

### CLI Example
The following examples use the [GDELT public dataset](http://data.gdeltproject.org/documentation/GDELT-Data_Format_Codebook.pdf) and can be run from the project directory.

Create a dataset with all the columns :

```
./filo-cli --command create --dataset gdelt --columns GLOBALEVENTID:int,SQLDATE:string,MonthYear:int,Year:int,FractionDate:double,Actor1Code:string,Actor1Name:string,Actor1CountryCode:string,Actor1KnownGroupCode:string,Actor1EthnicCode:string,Actor1Religion1Code:string,Actor1Religion2Code:string,Actor1Type1Code:string,Actor1Type2Code:string,Actor1Type3Code:string,Actor2Code:string,Actor2Name:string,Actor2CountryCode:string,Actor2KnownGroupCode:string,Actor2EthnicCode:string,Actor2Religion1Code:string,Actor2Religion2Code:string,Actor2Type1Code:string,Actor2Type2Code:string,Actor2Type3Code:string,IsRootEvent:int,EventCode:string,EventBaseCode:string,EventRootCode:string,QuadClass:int,GoldsteinScale:double,NumMentions:int,NumSources:int,NumArticles:int,AvgTone:double,Actor1Geo_Type:int,Actor1Geo_FullName:string,Actor1Geo_CountryCode:string,Actor1Geo_ADM1Code:string,Actor1Geo_Lat:double,Actor1Geo_Long:double,Actor1Geo_FeatureID:int,Actor2Geo_Type:int,Actor2Geo_FullName:string,Actor2Geo_CountryCode:string,Actor2Geo_ADM1Code:string,Actor2Geo_Lat:double,Actor2Geo_Long:double,Actor2Geo_FeatureID:int,ActionGeo_Type:int,ActionGeo_FullName:string,ActionGeo_CountryCode:string,ActionGeo_ADM1Code:string,ActionGeo_Lat:double,ActionGeo_Long:double,ActionGeo_FeatureID:int,DATEADDED:string,Actor1Geo_FullLocation:string,Actor2Geo_FullLocation:string,ActionGeo_FullLocation:string --rowKeys GLOBALEVENTID --segmentKey ':string 0'
```

Verify the dataset metadata:

```
./filo-cli --command list --dataset gdelt
```

Import data from a CSV file:

```
./filo-cli --command importcsv --dataset gdelt --filename GDELT-1979-1984-100000.csv
```

Query/export some columns:

```
./filo-cli --dataset gdelt --select MonthYear,Actor2Code --limit 5 --outfile out.csv
```


## Current Status

Version 0.4 is the stable, latest released version.  It has been tested on a cluster for a different variety of schemas, has a stable data model and ingestion, and features a huge number of improvements over the previous version.

### Upcoming version 0.5 changes:

* NEW storage layout with incremental indices, provides much better ingestion for large partitions and skewed data 
* Automatic routing of ingestion records across the network - no need to `sort` your DataFrame in Spark
* creating a function for checking java and another to check sbt (@jenaiz)

### Version 0.4 change list:

* Defaults to Spark 1.6.1
* New metrics and monitoring framework based on Kamon.io, with built in stats logging and statsd output, and tracing of write path
* Replaced Phantom with direct usage of Java C* driver.  Bonus: use prepared statements, should result in better performance all around especially on ingest; plus supports C* 3.0+
* WHERE clauses specifying multiple partition keys now get pushed down.  Should result in much better read performance in those cases.
* New :hash function makes it easier to hash partition key components into smaller cardinality (but specify the full key in WHERE clauses)
* New config `filodb.cassandra.keyspace-replication-options` allows any CQL replication option to be set when FiloDB keyspaces are created with CLI --command init
* A few new configs for Cassandra CQL / chunk / sstable compression; can help improve remote read performance
* CLI log directory can be easily changed with FILO_LOG_DIR env var
* CLI analyze command can now analyze segments from multiple partitions up to a configurable maximum # of segments
* Allow comma-separated list of hosts for `filodb.cassandra.hosts`
* Fix missing data on read issue with wrapping token ranges in C*
* Fix actor path uniqueness issue on ingestion

## Deploying

- sbt spark/assembly
- sbt cli/assembly
- Copy `core/src/main/resources/application.conf` and modify as needed for your own config file
- Set FILO_CONFIG_FILE to the path to your custom config
- Run the cli jar as the filo CLI command line tool and initialize keyspaces if using Cassandra: `filo-cli-*.jar --command init`

There is a branch for Datastax Enterprise 4.8 / Spark 1.4.  Note that if you are using DSE or have vnodes enabled, a lower number of vnodes (16 or less) is STRONGLY recommended as higher numbers of vnodes slows down queries substantially and basically prevents subsecond queries from happening.

By default, FiloDB nodes (basically all the Spark executors) talk to each other using a random port and locally assigned hostname.  You may wish to set `filodb.spark.driver.port`, `filodb.spark.executor.port` to assign specific ports (for AWS, for example) or possibly use a different config file on each host and set `akka.remote.netty.tcp.hostname` on each host's config file.

## Monitoring and Metrics

FiloDB uses [Kamon](http://kamon.io) for metrics and Akka/Futures/async tracing.  Not only does this give us summary statistics, but this also gives us Zipkin-style tracing of the ingestion write path in production, which can give a much richer picture than just stats.

### Metrics Sinks

* statsd sink - this is packaged into FiloDB's spark module (but not the CLI module) by default.  All you need to do is stand up [statsd](https://github.com/etsy/statsd), [Statsite](http://armon.github.io/statsite/), or equivalent daemon.  See the Kamon [Statsd module](http://kamon.io/backends/statsd/) guide for configuration.
* Kamon metrics logger - this is part of the coordinator module and will log all metrics (including segment trace metrics) at every Kamon tick interval, which defaults to 10 seconds.  It is disabled by default but could be enabled with `--conf spark.filodb.metrics-logger.enabled=true` or changing `filodb.metrics-logger.enabled` in your conf file.  Also, which metrics to log including pattern matching on names can be configured.
* Kamon trace logger - this logs detailed trace information on segment appends and is always on if detailed tracing is on.

### Metrics Configuration

Kamon has many configurable options.  To get more detailed traces on the write / segment append path, for example, here is how you might pass to `spark-submit` or `spark-shell` options to set detailed tracing on and to trace 3% of all segment appends:

    --driver-java-options '-XX:+UseG1GC -XX:MaxGCPauseMillis=500 -Dkamon.trace.level-of-detail=simple-trace -Dkamon.trace.random-sampler.chance=3'

To change the metrics flush interval, you can set `kamon.metric.tick-interval` and `kamon.statsd.flush-interval`.  The statsd flush-interval must be equal to or greater than the tick-interval.

Methods of configuring Kamon (except for the metrics logger):

- The best way to configure Kamon is to pass this Java property: `-Dkamon.config-provider=filodb.coordinator.KamonConfigProvider`.  This lets you configure Kamon through the same mechanisms as the rest of FiloDB: `-Dfilo.config.file` for example, and the configuration is automatically passed to each executor/worker.  Otherwise:
- Passing Java options on the command line with `-D`, or for Spark, `--driver-java-options` and `--executor-java-options`
- Passing options in a config file and using `-Dconfig.file`.  NOTE: `-Dfilo.config.file` will not work because Kamon uses a different initialization stack. Need to be done for both drivers and executors.

## Code Walkthrough

Please go to the [architecture](doc/architecture.md) doc.

## Building and Testing

Run the tests with `sbt test`, or for continuous development, `sbt ~test`.  Noisy cassandra logs can be seen in `filodb-test.log`.

To run benchmarks, from within SBT:

    cd jmh
    jmh:run -i 5 -wi 5 -f3

You can get the huge variety of JMH options by running `jmh:run -help`.

There are also stress tests in the stress module.  See the [Stress README](stress/README.md).

## You can help!

Contributions are welcome!
