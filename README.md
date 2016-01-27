# FiloDB

[![Join the chat at https://gitter.im/velvia/FiloDB](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/velvia/FiloDB?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
High-performance distributed analytical database + Spark SQL queries + built for streaming.

[filodb-announce](https://groups.google.com/forum/#!forum/filodb-announce) google group

```
    _______ __      ____  ____ 
   / ____(_) /___  / __ \/ __ )
  / /_  / / / __ \/ / / / __  |
 / __/ / / / /_/ / /_/ / /_/ / 
/_/   /_/_/\____/_____/_____/  
```

![](Dantat.jpg)

Columnar, versioned layers of data wrapped in a yummy high-performance analytical database engine.

See [architecture](doc/architecture.md) and [datasets and reading](doc/datasets_reading.md) for more information.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Overview](#overview)
  - [Use Cases](#use-cases)
  - [Anti-use-cases](#anti-use-cases)
  - [Roadmap](#roadmap)
- [Pre-requisites](#pre-requisites)
- [Getting Started](#getting-started)
  - [FiloDB Data Modelling and Performance Considerations](#filodb-data-modelling-and-performance-considerations)
  - [Example FiloDB Schema for machine metrics](#example-filodb-schema-for-machine-metrics)
  - [Distributed Partitioning](#distributed-partitioning)
- [Using FiloDB data-source with Spark](#using-filodb-data-source-with-spark)
  - [Configuring FiloDB](#configuring-filodb)
  - [Spark data-source Example (spark-shell)](#spark-data-source-example-spark-shell)
  - [Spark Streaming Example](#spark-streaming-example)
  - [Spark SQL Example (spark-sql)](#spark-sql-example-spark-sql)
  - [Querying Datasets](#querying-datasets)
- [Using the CLI](#using-the-cli)
    - [CLI Example](#cli-example)
- [Current Status](#current-status)
- [Building and Testing](#building-and-testing)
- [You can help!](#you-can-help)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Overview

FiloDB is a new open-source distributed, versioned, and columnar analytical database designed for modern streaming workloads.

* **High performance** - faster than Parquet scan speeds, plus filtering along two or more dimensions
* **Compact storage** - within 35% of Parquet
* **Idempotent writes** - primary-key based appends and updates; easy exactly-once ingestion from streaming sources
* **Distributed** - pluggable storage engine includes Apache Cassandra and in-memory
* **Low-latency** - minimal SQL query latency of 25ms on one node; sub-second easily achievable with filtering and easy to use concurrency control
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

* True columnar querying and execution, using late materialization and vectorization techniques
* Use of GPU and SIMD instructions to speed up queries
* Non-Spark ingestion API.  Your input is again needed.
* In-memory caching for significant query speedup
* Projections.  Often-repeated queries can be sped up significantly with projections.

## Pre-requisites

1. [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
2. [SBT](http://www.scala-sbt.org/) to build
3. [Apache Cassandra](http://cassandra.apache.org/) (We prefer using [CCM](https://github.com/pcmanus/ccm) for local testing) (Optional if you are using the in-memory column store)
4. [Apache Spark (1.5.x)](http://spark.apache.org/)

## Getting Started

1. Clone the project and cd into the project directory,

    ```
    $ git clone https://github.com/tuplejump/FiloDB.git
    $ cd FiloDB
    ```

2. Choose either the Cassandra column store (default) or the in-memory column store.
    - Start a Cassandra Cluster. If its not accessible at `localhost:9042` update it in `core/src/main/resources/application.conf`.
    - Or, use FiloDB's in-memory column store with Spark (does not work with CLI). Pass the `--conf spark.filodb.store=in-memory` to `spark-submit` / `spark-shell`.  This is a great option to test things out, and is really really fast!

3. For Cassandra, run `filo-cli --command init` to initialize the default `filodb` keyspace.
4. Now, you can use Spark to ingest/query, or the CLI to ingest/examine metadata.

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
1. **row key**       - acts as a primary key within each partition and decides how data will be sorted within each segment.  May consist of multiple columns.

The PRIMARY KEY for FiloDB consists of (partition key, row key).  When choosing the above values you must make sure the combination of the two are unique.  No component of a primary key may be null - see the `:getOrElse` function for a way of dealing with null inputs.

Specifying the partitioning column is optional.  If a partitioning column is not specified, FiloDB will create a default one with a fixed value, which means everything will be thrown into one node, and is only suitable for small amounts of data.  If you don't specify a partitioning column, then you have to make sure your row keys are all unique.

### Computed Columns

You may specify a function, or computed column, for use with any key column.  This is especially useful for working around the non-null requirement for keys, or for computing a good segment key.

| Name      | Description     | Example     |
| :-------- | :-------------- | :---------- |
| string    | returns a constant string value | `:string /0` |
| getOrElse | returns default value if column value is null | `:getOrElse columnA ---` |

### FiloDB vs Cassandra Data Modelling

* Like Cassandra, partitions (physical rows) distribute data and clustering keys act as a primary key within a partition
* Like Cassandra, a single partition is the smallest unit of parallelism when querying from Spark
* Wider rows work better for FiloDB (bigger chunk/segment size)
* FiloDB does not have Cassandra's restrictions for partition key filtering. You can filter by any partition keys with most operators.  This means less tables in FiloDB can match more query patterns.

### Data Modelling and Performance Considerations

**Choosing Partition Keys**.

- Partition keys are the most efficient way to filter data
- If there are too few partitions, then FiloDB will not be able to distribute and parallelize reads.
- If the numer of rows in each partition is too few, then the storage will not be efficient.
- If the partition key is time based, there may be a hotspot in the cluster as recent data is all written into the same set of replicas, and likewise for read patterns as well.

**Segment Key and Chunk Size**.

Within each partition, data is delineated by the segment key into *segments*.  Within each segment, successive flushes of the MemTable writes data in *chunks*.  The segmentation is key to sorting and filtering data within partitions, and the chunk size (which depends on segmentation) also affects performance.  The smaller the chunk size, the higher the overhead of scanning data becomes.

Segmentation and chunk size distribution may be checked by the CLI `analyze` command.  In addition, the following configuration affects segmentation and chunk size:
* `memtable.max-rows-per-table`, `memtable.flush-trigger-rows` affects how many rows are kept in the MemTable at a time, and this along with how many partitions are in the MemTable directly leads to the chunk size upon flushing.
* The segment size is directly controlled by the segment key.  Choosing a segment key that groups data into big enough chunks (at least 1000 is a good guide) is highly recommended.  Experimentation along with running `filo-cli analyze` is recommended to come up with a good segment key.
* `chunk_size` option when creating a dataset caps the size of a single chunk.

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

Currently, FiloDB is a library in Spark and requires the user to distribute data such that no two nodes have rows with the same partition key.

* The easiest strategy to accomplish this is to have data partitioned via a queue such as Kafka.  That way, when the data comes into Spark Streaming, it is already partitioned correctly.
* Another way of accomplishing this is to use a DataFrame's `sort` method before using the DataFrame write API.

## Using FiloDB data-source with Spark

FiloDB has a Spark data-source module - `filodb.spark`. So, you can use the Spark Dataframes `read` and `write` APIs with FiloDB. To use it, follow the steps below

1. Start Cassandra and update project configuration if required.
2. From the FiloDB project directory, execute,
   ```
   $ sbt clean
   $ ./filo-cli --command init
   $ sbt spark/assembly
   ```
3. Use the jar `FiloDB/spark/target/scala-2.10/filodb-spark-assembly-0.2-SNAPSHOT.jar` with Spark 1.5.x.

The options to use with the data-source api are:

| option           | value                                                            | command    | optional |
|------------------|------------------------------------------------------------------|------------|----------|
| dataset          | name of the dataset                                              | read/write | No       |
| sort_column      | name of the column according to which the data should be sorted  | write      | No       |
| partition_column | name of the column according to which data should be partitioned | write      | Yes      |
| splits_per_node  | number of read threads per node, defaults to 4 | read | Yes |
| default_partition_key | default value to use for the partition key if the partition_column has a null value.  If not specified, an error is thrown. Note that this only has an effect if the dataset is created for the first time.| write | Yes |
| version          | numeric version of data to write, defaults to 0  | write | Yes |

### Configuring FiloDB

Some options must be configured before starting the Spark Shell or Spark application.  There are two methods:

1. Modify the `application.conf` and rebuild, or repackage a new configuration.
2. Override the built in defaults by setting SparkConf configuration values, preceding the filo settings with `spark.filodb`.  For example, to change the keyspace, pass `--conf spark.filodb.cassandra.keyspace=mykeyspace` to Spark Shell/Submit.  To use the fast in-memory column store instead of Cassandra, pass `--conf spark.filodb.store=in-memory`.

### Spark data-source Example (spark-shell)

You can follow along using the [Spark Notebook](http://github.com/andypetrella/spark-notebook) in doc/FiloDB.snb....  launch the notebook using `EXTRA_CLASSPATH=$FILO_JAR ADD_JARS=$FILO_JAR ./bin/spark-notebook &` where `FILO_JAR` is the path to `filodb-spark-assembly` jar.

Or you can start a spark-shell locally,

```bash
bin/spark-shell --jars ../FiloDB/spark/target/scala-2.10/filodb-spark-assembly-0.1-SNAPSHOT.jar --packages com.databricks:spark-csv_2.10:1.2.0 --driver-memory 3G --executor-memory 3G
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
             option("sort_column", "GLOBALEVENTID").
             mode(SaveMode.Overwrite).save()
```

Or, specifying the `partition_column`,

```scala
scala> csvDF.write.format("filodb.spark").
    option("dataset", "gdelt").
    option("sort_column", "GLOBALEVENTID").
    option("partition_column", "MonthYear").
    mode(SaveMode.Overwrite).save()
```

Note that for efficient columnar encoding, wide rows with fewer partition keys are better for performance.

Reading the dataset,
```
val df = sqlContext.read.format("filodb.spark").option("dataset", "gdelt").load()
```

The dataset can be queried using the DataFrame DSL. See the section [Querying Datasets](#querying-datasets) for examples.

### Spark Streaming Example

It's not difficult to ingest data into FiloDB using Spark Streaming.  Simple use `foreachRDD` on your `DStream` and then [transform each RDD into a DataFrame](https://spark.apache.org/docs/latest/streaming-programming-guide.html#dataframe-and-sql-operations).

For an example, see the [StreamingTest](spark/src/test/scala/filodb.spark/StreamingTest.scala).

### Spark SQL Example (spark-sql)

Start Spark-SQL:

```bash
  bin/spark-sql --jars path/to/FiloDB/spark/target/scala-2.10/filodb-spark-assembly-0.1-SNAPSHOT.jar
```

Create a temporary table using an existing dataset,

```sql
  create temporary table gdelt
  using filodb.spark
  options (
   dataset "gdelt"
 );
```

Then, start running SQL queries!

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

## Using the CLI

The `filo-cli` accepts arguments as key-value pairs. The following keys are supported:

| key          | purpose                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
|--------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| dataset    | It is required for all the operations. Its value should be the name of the dataset                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| limit      | This is optional key to be used with `select`. Its value should be the number of rows required.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| columns    | This is required for defining the schema of a dataset. Its value should be a comma-separated string of the format, `column1:typeOfColumn1,column2:typeOfColumn2` where column1 and column2 are the names of the columns and typeOfColumn1 and typeOfColumn2 are one of `int`,`long`,`double`,`string`,`bool`                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| rowKeys | This is required for defining the row keys. Its value should be comma-separated list of column names or computed column functions to make up the row key                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| segmentKey | The column name or computed column for the segment key |
| partitionKeys | Comma-separated list of column names or computed columns to make up the partition key |
| command    | Its value can be either of `init`,`create`,`importcsv`,`analyze` or `list`.<br/><br/>The `init` command is used to create the FiloDB schema.<br/><br/>The `create` command is used to define new a dataset. For example,<br/>```./filo-cli --command create --dataset playlist --columns id:int,album:string,artist:string,title:string --sortColumn id``` <br/>Note: The sort column is not optional.<br/><br/>The `list` command can be used to view the schema of a dataset. For example, <br/>```./filo-cli --command list --dataset playlist```<br/><br/>The `importcsv` command can be used to load data from a CSV file into a dataset. For example,<br/>```./filo-cli --command importcsv --dataset playlist --filename playlist.csv```<br/>Note: The CSV file should be delimited with comma and have a header row. The column names must match those specified when creating the schema for that dataset. |
| select     | Its value should be a comma-separated string of the columns to be selected,<br/>```./filo-cli --dataset playlist --select album,title```<br/>The result from `select` is printed in the console by default. An output file can be specified with the key `--outfile`. For example,<br/>```./filo-cli --dataset playlist --select album,title --outfile playlist.csv```                                                                                                                                                                                                                                                                                                                                                                                                                   |
| delimiter  | This is optional key to be used with `importcsv` command. Its value should be the field delimiter character. Default value is comma.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| timeoutMinutes | The number of minutes to time out for CSV ingestion.  Needs to be greater than the max amount of time for ingesting the whole file.  Defaults to 99.  |

#### CLI Example
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

Version 0.1 is released!  It offers a stable point from which to try FiloDB.
* Ingestion is relatively efficient, stable, and tested up to a 2.4 GB / 15 million row data source (doesn't mean more doesn't work, just what is regularly tested)
* Query columnar projection and partition key filtering pushdown.  Sort key filtering pushdown not in yet.
* Only ingestion through Spark / Spark Streaming, and CLI ingestion via CSV files.
* CSV export from CLI will only read data from one node of a cluster.

Version 0.2 will include many more features:
- Composite partition, sort, and segment keys
- More efficient storage engine

## Building and Testing

Run the tests with `sbt test`, or for continuous development, `sbt ~test`.  Noisy cassandra logs can be seen in `filodb-test.log`.

## You can help!

- Send your use cases for OLAP on Cassandra and Spark
    + Especially IoT and Geospatial
- Email if you want to contribute

Your feedback will help decide the next batch of features, such as:
    - which data types to add support for
    - what architecture is best supported
