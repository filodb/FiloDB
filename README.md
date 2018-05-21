# FiloDB

[![Join the chat at https://gitter.im/velvia/FiloDB](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/velvia/FiloDB?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/filodb/FiloDB.svg?branch=master)](https://travis-ci.org/filodb/FiloDB)

Distributed, real-time, in-memory, massively scalable, multi-schema time series / event / operational database.  Prometheus support.

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

See [architecture](doc/architecture.md) and [datasets and reading](doc/datasets_reading.md) for more information.  Also see the Spark Notebooks under `doc`... there is one for time-series/geo analysis of the NYC Taxi dataset, and one for interactive charting of the GDELT dataset!

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Overview](#overview)
  - [Use Cases](#use-cases)
  - [Anti-use-cases](#anti-use-cases)
- [Pre-requisites](#pre-requisites)
- [Getting Started](#getting-started)
- [End to End Kafka Developer Setup](#end-to-end-kafka-developer-setup)
    - [Multiple Servers](#multiple-servers)
    - [Local Scale Testing](#local-scale-testing)
- [Introduction to FiloDB Data Modelling](#introduction-to-filodb-data-modelling)
  - [Example FiloDB Schema for machine metrics](#example-filodb-schema-for-machine-metrics)
  - [Data Modelling and Performance Considerations](#data-modelling-and-performance-considerations)
  - [Predicate Pushdowns](#predicate-pushdowns)
  - [Sharding](#sharding)
- [Using the FiloDB HTTP API](#using-the-filodb-http-api)
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
- [Deploying](#deploying)
- [Monitoring and Metrics](#monitoring-and-metrics)
  - [Metrics Sinks](#metrics-sinks)
  - [Metrics Configuration](#metrics-configuration)
- [Code Walkthrough](#code-walkthrough)
- [Building and Testing](#building-and-testing)
  - [Debugging serialization and queries](#debugging-serialization-and-queries)
  - [Benchmarking](#benchmarking)
- [You can help!](#you-can-help)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Overview

FiloDB is an open-source distributed, real-time, in-memory, massively scalable, multi-schema time series / event / operational database with Prometheus query support and some Spark support as well.

The normal configuration for real-time ingestion is deployment as stand-alone processes in a cluster, ingesting directly from Apache Kafka.  The processes form a cluster using peer-to-peer Akka Cluster technology.

* **Massively Scalable** - designed to ingest many millions of entities, sharded across multiple processes, with distributed querying built in
* **Prometheus PromQL Support**
* **Tag-based Indexing** - Support for indexing and fast querying over flexible tags for each time series/partition, just like Prometheus
* **Efficient** - holds a huge amount of data in-memory thanks to columnar compression techniques
* **Low-latency** - designed for highly concurrent, low-latency workloads such as dashboards and alerting
* **Real Time** - data immediately available for querying once ingested
* **Fault Tolerant** - designed for dual-datacenter operation with strong recoverability and no single point of failure. explain explain
* **Multi-Schema and Multi-Stream** - easily segregate and prioritize different classes of metrics and data.  Easily support different types of events.
* **Off Heap** - intelligent memory management minimizes garbage collection

[Overview presentation](http://velvia.github.io/presentations/2015-filodb-spark-streaming/#/) -- see the docs folder for design docs.

To compile the .mermaid source files to .png's, install the [Mermaid CLI](http://knsv.github.io/mermaid/mermaidCLI.html).

### Use Cases

* Real-time operational metrics storage, querying, dashboards, visibility
* Distributed tracing (ie Zipkin like) storage
* Low-latency real-time ad-hoc application metric debugging
* Real-time event storage and querying

### Anti-use-cases

* Heavily transactional, update-oriented workflows

## Pre-requisites

1. [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
2. [SBT](http://www.scala-sbt.org/) to build
3. [Apache Cassandra](http://cassandra.apache.org/) 2.x or 3.x (We prefer using [CCM](https://github.com/pcmanus/ccm) for local testing) (Optional if you are using the in-memory column store)
4. [Apache Kafka](http://kafka.apache.org/) 0.10.x or above
 
Optional:

4. [Apache Spark (2.0)](http://spark.apache.org/)

## Getting Started

1. Clone the project and cd into the project directory,

    ```
    $ git clone https://github.com/filodb/FiloDB.git
    $ cd FiloDB
    ```

    - It is recommended you use the last stable released version.
    - To build, run `filo-cli` (see below) and also `sbt spark/assembly`.

Follow the instructions below to set up an end to end local environment.

## End to End Kafka Developer Setup

This section describes how you can run an end-to-end test locally on a Macbook by ingesting time series data into FiloDB In Memory Store, and querying from it using PromQL.

Use your favorite package manager to install and set up pre-requisite infrastructure. Kafka 0.10.2+ or 0.11 can be used. 
```
brew install kafka
brew services start zookeeper
brew services start kafka
```

Create a new Kafka topic with 4 partitions. This is where time series data will be ingested for FiloDB to consume
```
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 4 --topic timeseries-dev
```

Download and start Cassandra 2.1.

```
bin/cassandra
```

Build the required projects
```
sbt standalone/assembly cli/assembly tsgenerator/assembly
```

First set up the dataset. This should create the keyspaces and tables in Cassandra. 
```
./filo-cli -Dconfig.file=conf/timeseries-filodb-server.conf  --command init
./filo-cli -Dconfig.file=conf/timeseries-filodb-server.conf  --command create --dataset timeseries --dataColumns timestamp:long,value:double --partitionColumns tags:map --shardKeyColumns __name__,job
```
Verify that tables were created in `filodb` and `filodb-admin` keyspaces.

NOTE: if you have already gone through the procedure above, you may need to clear out the existing metadata: `./filo-cli -Dconfig.file=conf/timeseries-filodb-server.conf --command clearMetadata`, then repeat the steps above.  Otherwise you will not be in a clean state and may have stale schemas especially if the code has changed.

The script below brings up the FiloDB Dev Standalone server, and then sets up the timeseries dataset (NOTE: if you previously started FiloDB and have not cleared the metadata, then the -s is not needed as FiloDB will recover previous ingestion configs from Cassandra)

```
./filodb-dev-start.sh -s
```
You can now tail the server logs. If you'd like to direct the logs to a specific folder, you can set the LOG_DIR environment variable. 
```
tail -f logs/filodb-server-1.log
```
Verify this message `NodeClusterActor Actor[akka://filo-standalone/user/node/singleton/nodecluster#-528195195] said hello!` in the logs. The logs should also indicate kafka connections being setup for consumption for the dataset. Wait for this to happen.

Now run the time series generator. This will ingest 20 time series (the default) with 100 samples each into the Kafka topic with current timestamps.  The required argument is the path to the source config.  Use `--help` for all the options.

```
java -cp tsgenerator/target/scala-2.11/tsgenerator-*.telemetry-SNAPSHOT filodb.timeseries.TestTimeseriesProducer -c conf/timeseries-dev-source.conf
```

At this point, you should be able to confirm such a message in the server logs: `KAMON counter name=memstore-rows-ingested count=4999`

Now you are ready to query FiloDB for the ingested data. The following command should return matching subset of the data that was ingested by the producer.

```
./filo-cli '-Dakka.remote.netty.tcp.hostname=127.0.0.1' --host 127.0.0.1 --dataset timeseries --promql 'heap_usage{host="H1"}'
```

You can also look at Cassandra to check for persisted data. Look at the tables in `filodb` and `filodb-admin` keyspaces.

To stop the dev server. Note that this will stop all the FiloDB servers if multiple are running.
```
./filodb-dev-stop.sh
```

#### Multiple Servers

To run multiple servers simultaneously, first set up Consul dev agent:

```
> brew install consul 
> cat /usr/local/etc/consul/config/basic_config.json 
{
"data_dir": "/usr/local/var/consul",
"ui" : true,
"dns_config" : {
    "enable_truncate" : true
}
 	
```

Then run consul consul agent in dev mode:
```
consul agent -dev -config-dir=/usr/local/etc/consul/config/
```
Perform the filo-cli `init` and `create` steps as before.

Start first FiloDB server 
```
./filodb-dev-start.sh -c conf/timeseries-filodb-server-consul.conf -l 1
```
And subsequent FiloDB servers. Change log file suffix with the `-l` option for each server. Add the `-s` option to 
the last server, so data setup is initiated after all servers come up.
```
./filodb-dev-start.sh -c conf/timeseries-filodb-server-consul.conf -l 2 -p -s
```

#### Local Scale Testing

Follow the same steps as in original setup, but do this first to clear out existing metadata:

```bash
./filo-cli -Dconfig.file=conf/timeseries-filodb-server.conf --command clearMetadata
```

Then follow the steps to create the dataset etc.  Create a different Kafka topic with 128 partitions:

```bash
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 128 --topic timeseries-perf
```

Start two servers as follows. This will not start ingestion yet:

```bash
./filodb-dev-start.sh -l 1
./filodb-dev-start.sh -l 2 -p
```

Set up ingestion:

```bash
./filo-cli --host 127.0.0.1 --dataset timeseries --command setup --filename conf/timeseries-128shards-source.conf
```

Now if you curl the cluster status you should see 128 shards which are slowly turning active: `curl http://127.0.0.1:8080/api/v1/cluster/timeseries/status | jq '.'`

Generate records:

```
java -cp tsgenerator/target/scala-2.11/tsgenerator-*.telemetry-SNAPSHOT filodb.timeseries.TestTimeseriesProducer -c conf/timeseries-128shards-source.conf -p 5000
```

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
    <td>Chunk 1</td>
    <td>Chunk 2</td>
    <td>Chunk 1</td>
    <td>Chunk 2</td>
  </tr>
  <tr>
    <td>Partition key 2</td>
    <td>Chunk 1</td>
    <td>Chunk 2</td>
    <td>Chunk 1</td>
    <td>Chunk 2</td>
  </tr>
</table>

Data is modeled and ingested using the two different parts of a record's primary key: the **partition key** and the **row key**.

1. **partition key** - decides how data is going to be distributed across the cluster. All data within one partition key should be routed to the same shard, or Kafka partition, and one shard must fit completely into the memory of a single node.
1. **row key**     - NOTE: for time series data the row key is only used when doing a range lookup during querying.  Each chunk is delimited by a start and end row key.   OUTDATED: acts as a primary key within each partition. Records with the same row key will replace previous ones. Within each chunkset, records are sorted by row key.  Row keys also facilitate range scans within a partition.

The PRIMARY KEY for FiloDB consists of (partition key, row key).  When choosing the above values you must make sure the combination of the two are unique.  If any component of a primary key contains a null value, then a default value will be substituted.

For examples of data modeling and choosing keys, see the examples below as well as [datasets](doc/datasets_reading.md).

For additional information refer to Data Modeling and Performance Considerations.

### Example FiloDB Schema for machine metrics

* Partition key = `metricName:string,tags:map`
* Row key = `timestamp`
* Columns: `timestamp:long,value:double`

The above is the classic Prometheus-compatible schema.  It supports indexing on any component of the partition key.  Thus standard Prometheus queries that filter by a tag such as `hostname` or `datacenter` for example would work fine.

If this was for events, you might have multiple columns, and some of them can be strings too.  This totally works fine.  See the example dataset schemas.

### Data Modelling and Performance Considerations

NOTE: this section is really old and needs to be rewritten.

**Choosing Partition Keys**.

- A good start for a partition key is a hash of an ID or columns that distribute well, plus a time bucket.  This spreads data around but also prevents too much data from piling up in a single partition.
- Partition keys are the most efficient way to filter data.
- FiloDB indexes components of a partition key.  A map can be used (when using a Prometheus-like data model for example) and all the keys and values within the map will be indexed, in addition to regular partition key columns.  This allows for very fast in-memory queries across many partitions.
- If the numer of rows in each partition is too few, then the storage will not be efficient.
- If the partition key changes during ingestion, then a new partition (or time series) is automatically created, just like in Prometheus.
- For Cassandra, each partition cannot be too big, target 500MB-1GB as a maximum size.  Usually older data is TTL'ed.

**Row Keys and Chunk Size**.

Within a partition, data is divided into chunks.  The relationship between partitions, chunks, and row keys are as follows:

* During ingestion, all the rows for a partition are sorted by row key in a memtable, then chunked (with the max chunk size configurable). Thus all chunks are internally in row key sorted order.
* Filtering within a partition by row key ranges is available.  This results in only chunks that intersect with the row key range being read.
* The choice of row key does not affect chunk size, but does affect row key range scanning.  Choose a row key according to how you want to filter partition data -- put the most important filtering key first in the row key.

Bigger chunks are better.  The chunk size can be predicted as follows:

    Math.min(`chunk_size`, MemtableSize / (AverageActiveNumberOfPartitions))

For example, with the default `max-rows-per-table` of 200000, and 100 active partitions (let's say the time bucket is a day, and on average the other partition keys are hashed into 100 buckets), then the average chunk size will be 200000/100 = 2000 rows per chunk.

Chunk size distribution may be checked by the CLI `analyze` command.  In addition, the following configuration affects chunk size:
* `memtable.max-rows-per-table`, `memtable.flush-trigger-rows` affects how many rows are kept in the MemTable at a time, and this along with how many partitions are in the MemTable directly leads to the chunk size upon flushing.
* `chunk_size` option when creating a dataset caps the size of a single chunk.  Smaller chunk sizes lead to slower reads, but more even distribution of chunks (ie better range scanning)
* Row keys are immutable.  Don't pick columns that might change.
* Consider moving one of the partition keys as a part of a row key if your chunk sizes/partitions are too small.  Use the `analyze` filo-cli command to discover partition size.
* Less partitions and smaller chunk sizes leads to more chunks being written with each flush, which leads to better range scanning

### Predicate Pushdowns

To help with planning, here is an exact list of the predicate pushdowns (in Spark) that help with reducing I/O and query times:

* Partition key column(s): =, IN on any partition key column
  * = on every partition key results in a **single-partition** query
  * = or IN on every partition key results in a **multi-partition** query, still faster than full table scan
  * Otherwise, a filtered full table scan results - partitions not matching predicates will not be scanned
* Rowkey Range Scan:
  * Range scans are available for matching either the entire row key or the first N components of the row key
  * The last component compared must be either = or a range, and all other components must be = comparison
  * For example, if a row key consists of colA, colB, colC, then the following predicates are valid for push down:
    - `colA = value`
    - `colA >/>= value AND colA </<= value`
    - `colA = value AND colB = val2`
    - `colA = value AND (colB >/>= val1 AND colB </<= val2)`
    - `colA = valA AND colB = valB AND colC = valC`
    - `colA = valA AND colB = valB AND (colC >/>= val1 AND colC </<= val2)`

Note: You can see predicate pushdown filters in application logs by setting logging level to INFO.

### Sharding

TODO: add details about FiloDB sharding, the shard-key vs partition key mechanism, etc.

## Using the FiloDB HTTP API

Please see the [HTTP API](doc/http_api.md) doc.

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
| row_keys         | comma-separated list of column name(s) to use for the row primary key within each partition.  Computed columns are not allowed.  May be used for range queries within partitions and chunks are sorted by row keys. | write      | No if mode is OverWrite or creating dataset for first time  |
| partition_keys   | comma-separated list of column name(s) or computed column functions to use for the partition key.  If not specified, defaults to `:string /0` (a single partition).  | write      | Yes      |
| splits_per_node  | number of read threads per node, defaults to 4 | read | Yes |
| reset_schema     | If true, allows dataset schema (eg partition keys) to be redefined for an existing dataset when SaveMode.Overwrite is used.  Defaults to false.  | write | Yes |
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
bin/spark-shell --jars ../FiloDB/spark/target/scala-2.11/filodb-spark-assembly-0.7.0.spark20-SNAPSHOT.jar --packages com.databricks:spark-csv_2.11:1.4.0 --driver-memory 3G --executor-memory 3G
```

Loading CSV file from Spark:

```scala
scala> val csvDF = spark.read.format("com.databricks.spark.csv").
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
             option("partition_columns", "MonthYear:int").
             mode(SaveMode.Overwrite).save()
```

Above, we partition the GDELT dataset by MonthYear, creating roughly 72 partitions for 1979-1984, with the unique GLOBALEVENTID used as a row key.  You could use multiple columns for the partition or row keys, of course.  For example, to partition by country code and year instead:

```scala
scala> csvDF.write.format("filodb.spark").
             option("dataset", "gdelt_by_country_year").
             option("row_keys", "GLOBALEVENTID").
             option("partition_columns", "Actor2CountryCode:string,Year:int").
             mode(SaveMode.Overwrite).save()
```

In both cases, the use of `GLOBALEVENTID` as a row key would allow for range queries within partitions by the `GLOBALEVENTID`.

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
val df = spark.read.format("filodb.spark").option("dataset", "gdelt").load()
```

The dataset can be queried using the DataFrame DSL. See the section [Querying Datasets](#querying-datasets) for examples.

### Spark/Scala/Java API

There is a more typesafe API than the Spark Data Source API.

```scala
import filodb.spark._
spark.saveAsFilo(df, "gdelt",
                      rowKeys = Seq("GLOBALEVENTID"),
                      partitionColumns = Seq("MonthYear:int"))
```

The above creates the gdelt table based on the keys above, and also inserts data from the dataframe df.

NOTE: If you are running Spark Shell in DSE, you might need to do `import _root_.filodb.spark._`.

Please see the ScalaDoc for the method for more details -- there is a `database` option for specifying the Cassandra keyspace, and a `mode` option for specifying the Spark SQL SaveMode.

There is also an API purely for inserting data... after all, specifying the keys is not needed when inserting into an existing table.

```scala
import filodb.spark._
spark.insertIntoFilo(df, "gdelt")
```

The API for creating a DataFrame is also much more concise:

```scala
val df = spark.filoDataset("gdelt")
val df2 = spark.filoDataset("gdelt", database = Some("keyspace2"))
```

The above method calls rely on an implicit conversion. From Java, you would need to create a new `FiloContext` first:

```java
FiloContext fc = new filodb.spark.FiloContext(sparkSession.sqlContext);
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
...
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

The `filo-cli` accepts arguments as key-value pairs.  For quick help run it with no arguments.  The following keys are supported:

| key          | purpose                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
|--------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| dataset    | It is required for all the operations. Its value should be the name of the dataset                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| database   | Specifies the "database" the dataset should operate in.  For Cassandra, this is the keyspace.  If not specified, uses config value.  |
| limit      | This is optional key to be used with `select`. Its value should be the number of rows required.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| dataColumns    | This is required for defining the data columns of a dataset. Its value should be a comma-separated string of the format, `column1:typeOfColumn1,column2:typeOfColumn2` where column1 and column2 are the names of the columns and typeOfColumn1 and typeOfColumn2 are one of `int`,`long`,`double`,`string`,`bitmap`,`ts`                                                                                                                                                                                                                                                                                                                                                                                                              |
| rowKeys | This defines the row keys. Its value should be a comma-separated list of column names to make up the row key. These must be from `dataColumns`. It defaults to a single column "timestamp"                                                                                                                    |
| partitionColumns | Required.  Comma-separated string of the format, `column1:typeOfColumn1,column2:typeOfColumn2` where column1 and column2 are the names of the columns and typeOfColumn1 and typeOfColumn2 are one of `int`,`long`,`double`,`string`,`bitmap`,`ts`,`map` to make up the partition columns.  Only one partition column is required.  Map columns must be the last partition column.                   |
| command    | Its value can be either of `init`, `create`, `importcsv`, `truncate`, `status`, `setup`, `indexnames`, `indexvalues`, `clearMetadata` or `list`.<br/><br/>The `init` command is used to create the FiloDB schema.<br/><br/>The `create` command is used to define new a dataset. The `list` command can be used to view the schema of a dataset. For example, <br/>```./filo-cli --command list --dataset playlist```<br/><br/>The `importcsv` command can be used to load data from a CSV file into a dataset. For example,<br/>```./filo-cli --command importcsv --dataset playlist --filename playlist.csv```<br/>Note: The CSV file should be delimited with a comma and have a header row. The column names must match those specified when creating the schema for that dataset.<br>
`truncate` truncates data for an existing dataset to 0.<br>
The other commands are used to connect with, setup ingestion, check status, and query standalone FiloDB nodes. |
| select     | Its value should be a comma-separated string of the columns to be selected,<br/>```./filo-cli --dataset playlist --select album,title```<br/>The result from `select` is printed in the console by default. An output file can be specified with the key `--outfile`. For example,<br/>```./filo-cli --dataset playlist --select album,title --outfile playlist.csv```                                                                                                                                                                                                                                                                                                                                                                                                                   |
| delimiter  | This is optional key to be used with `importcsv` command. Its value should be the field delimiter character. Default value is comma.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
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
./filo-cli --command create --dataset gdelt --dataColumns GLOBALEVENTID:int,SQLDATE:string,Year:int,FractionDate:double,Actor1Code:string,Actor1Name:string,Actor1CountryCode:string,Actor1KnownGroupCode:string,Actor1EthnicCode:string,Actor1Religion1Code:string,Actor1Religion2Code:string,Actor1Type1Code:string,Actor1Type2Code:string,Actor1Type3Code:string,Actor2Code:string,Actor2Name:string,Actor2CountryCode:string,Actor2KnownGroupCode:string,Actor2EthnicCode:string,Actor2Religion1Code:string,Actor2Religion2Code:string,Actor2Type1Code:string,Actor2Type2Code:string,Actor2Type3Code:string,IsRootEvent:int,EventCode:string,EventBaseCode:string,EventRootCode:string,QuadClass:int,GoldsteinScale:double,NumMentions:int,NumSources:int,NumArticles:int,AvgTone:double,Actor1Geo_Type:int,Actor1Geo_FullName:string,Actor1Geo_CountryCode:string,Actor1Geo_ADM1Code:string,Actor1Geo_Lat:double,Actor1Geo_Long:double,Actor1Geo_FeatureID:int,Actor2Geo_Type:int,Actor2Geo_FullName:string,Actor2Geo_CountryCode:string,Actor2Geo_ADM1Code:string,Actor2Geo_Lat:double,Actor2Geo_Long:double,Actor2Geo_FeatureID:int,ActionGeo_Type:int,ActionGeo_FullName:string,ActionGeo_CountryCode:string,ActionGeo_ADM1Code:string,ActionGeo_Lat:double,ActionGeo_Long:double,ActionGeo_FeatureID:int,DATEADDED:string,Actor1Geo_FullLocation:string,Actor2Geo_FullLocation:string,ActionGeo_FullLocation:string --partitionColumns MonthYear:int --rowKeys GLOBALEVENTID
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

TODO

## Deploying

- sbt spark/assembly
- sbt cli/assembly
- Copy `core/src/main/resources/application.conf` and modify as needed for your own config file
- Set FILO_CONFIG_FILE to the path to your custom config
- Run the cli jar as the filo CLI command line tool and initialize keyspaces if using Cassandra: `filo-cli-*.jar --command init`

Note that if you are using DSE or have vnodes enabled, a lower number of vnodes (16 or less) is STRONGLY recommended as higher numbers of vnodes slows down queries substantially and basically prevents subsecond queries from happening.

If you are using DSE 5.0, you need to shade the hdrhistogram jar when building the FiloDB assembly due to a version conflict.

By default, FiloDB nodes (basically all the Spark executors) talk to each other using a random port and locally assigned hostname.  You may wish to set `filodb.spark.driver.port`, `filodb.spark.executor.port` to assign specific ports (for AWS, for example) or possibly use a different config file on each host and set `akka.remote.netty.tcp.hostname` on each host's config file.

Recommended flags:

- `-XX:MaxInlineLevel=20`

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

The docs use [mermaid](https://github.com/knsv/mermaid) and [doctoc](https://github.com/thlorenz/doctoc).  On a Mac, to install:

    brew install yarn
    yarn global add mermaid.cli
    yarn global add doctoc

Multi-JVM tests output a separate log file per process, in the `logs` dir under `multijvm-nodeN-test.log`.

Some useful environment vars:
* `LOG_AKKA_TO_CONSOLE` - define this to have noisy Akka Cluster logs output to the console
* `MAYBE_MULTI_JVM` - enable multi-JVM tests for the Kafka and Standalone modules.  These require both Cassandra and Kafka to be up and running.

### Debugging serialization and queries

Right now both Java and Kryo serialization are used for Akka messaging.  Kryo is used for query result serialization.  If there are mysterious hangs, or other potentially serialization-related bugs, here is where to investigate:

1. Run the `SerializationSpec` in coordinator.client module, especially if changes have been done to the Akka configuration.  This test uses the Akka serialization module to ensure settings and serializers work correctly.
2. Set `MAYBE_MULTI_JVM` to true and run `cassandra/test` and `standalone/test`. They test multi-node communication for both ingestion and querying.
3. Set the following log levels to trace queries and serialization in detail:
    - `filodb.coordinator.queryengine` to DEBUG - especially useful to see how `ExecPlans` get distributed
    - `com.esotericsoftware.minlog` to DEBUG or TRACE -- detailed overall serialization
    - `com.esotericsoftware.kryo.io` to TRACE - detailed `BinaryRecord` / `BinaryVector` serde debugging 
4. In particular, enable the above when running the CLI with the standalone FiloDB process to do PromQL queries.

To dynamically change the log level, you can use the `/admin/loglevel` HTTP API (per host).  Example:

    curl -d 'trace' http://localhost:8080/admin/loglevel/com.esotericsoftware.minlog

### Benchmarking

To run benchmarks, from within SBT:

    cd jmh
    jmh:run -i 5 -wi 5 -f3

You can get the huge variety of JMH options by running `jmh:run -help`.  For good profiling, get JVM Flight Recorder output:

    jmh:run -i 5 -wi 10 -f3 -prof jmh.extras.JFR -jvmArgsAppend -XX:MaxInlineLevel=20 -jvmArgsAppend -Xmx2g

The JFR output file will be printed at the bottom of the console output.  Load that from within `jmc` and you can get great analysis.  Flame graphs and perfasm/dtraceasm profiling is also available.

There are also stress tests in the stress module.  See the [Stress README](stress/README.md).

## You can help!

Contributions are welcome!
