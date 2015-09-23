# FiloDB

[![Join the chat at https://gitter.im/velvia/FiloDB](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/velvia/FiloDB?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
Distributed.  Columnar.  Versioned.  Streaming.  SQL.

```
    _______ __      ____  ____ 
   / ____(_) /___  / __ \/ __ )
  / /_  / / / __ \/ / / / __  |
 / __/ / / / /_/ / /_/ / /_/ / 
/_/   /_/_/\____/_____/_____/  
```

![](Dantat.jpg)

Columnar, versioned layers of data wrapped in a yummy high-performance analytical database engine.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Run analytical queries up to 100x faster on Spark SQL and Cassandra.](#run-analytical-queries-up-to-100x-faster-on-spark-sql-and-cassandra)
- [Pre-requisites](#pre-requisites)
- [Getting Started](#getting-started)
  - [Using the CLI](#using-the-cli)
    - [CLI Example](#cli-example)
  - [Using FiloDB data-source with Spark](#using-filodb-data-source-with-spark)
    - [Spark data-source Example (spark-shell)](#spark-data-source-example-spark-shell)
    - [Spark SQL Example (spark-sql)](#spark-sql-example-spark-sql)
    - [Ingesting and Querying with DataFrames (Old API)](#ingesting-and-querying-with-dataframes-old-api)
    - [Querying Datasets](#querying-datasets)
- [Current Status](#current-status)
- [Roadmap](#roadmap)
- [Building and Testing](#building-and-testing)
- [You can help!](#you-can-help)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Run analytical queries up to 100x faster on Spark SQL and Cassandra.

FiloDB is a new open-source distributed, versioned, and columnar analytical database.

* **Distributed** - FiloDB is designed from the beginning to run on best-of-breed distributed, scale-out storage platforms such as Apache Cassandra.  Queries run in parallel in Apache Spark for scale-out ad-hoc analysis.
* **Columnar** - FiloDB brings breakthrough performance levels for analytical queries by using a columnar storage layout with different space-saving techniques like dictionary compression.  The performance is comparable to Parquet, and one to two orders of magnitude faster than Spark on Cassandra 2.x for analytical queries.  For the POC performance comparison, please see [cassandra-gdelt](http://github.com/velvia/cassandra-gdelt) repo.
* **Versioned** - At the same time, row-level, column-level operations and built in versioning gives FiloDB far more flexibility than can be achieved using file-based technologies like Parquet alone.
* Designed for **streaming** - Enable easy exactly-once ingestion from Kafka for streaming events, time series, and IoT applications - yet enable extremely fast ad-hoc analysis using the ease of use of SQL.  Each row is keyed by a partition and sort key, and writes using the same key are idempotent.  FiloDB does the hard work of keeping data stored in an efficient and sorted format.

FiloDB is easy to use!  You can use Spark SQL for both ingestion (including from Streaming!) and querying.

Connect Tableau or any other JDBC analysis tool to Spark SQL, and easily ingest data from any source with Spark support(JSON, CSV, traditional database, Kafka, etc.)

FiloDB is a great fit for bulk analytical workloads, or streaming /  event data.  It is not optimized for heavily transactional, update-oriented workflows.

[Overview presentation](http://velvia.github.io/presentations/2014-filodb/#/) -- see the docs folder for design docs.

To compile the .mermaid source files to .png's, install the [Mermaid CLI](http://knsv.github.io/mermaid/mermaidCLI.html).

## Pre-requisites

1. [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
2. [SBT](http://www.scala-sbt.org/)
3. [Apache Cassandra](http://cassandra.apache.org/)(We prefer using [CCM](https://github.com/pcmanus/ccm))
4. [Apache Spark (1.4.x)](http://spark.apache.org/) (optional - not required if you are only using the CLI)

## Getting Started

1. Clone the project and cd into the project directory,

    ```
    $ git clone https://github.com/velvia/FiloDB.git
    $ cd FiloDB
    ```

2. Start a Cassandra Cluster. If its not accessible at `localhost:9042` update it in `core/src/main/resources/application.conf`.

3. FiloDB can be used through `filo-cli` or as a Spark data source. The CLI supports data ingestion from CSV files only.

There are two crucial parts to a dataset in FiloDB,

1. partitioning column - decides how data is going to be distributed across the cluster
2. sort column         - acts as a primary key within each partition and decides how data will be sorted within each partition

Specifying partitioning column is optional.  If a partitioning column is not specified, FiloDB will create a default one with a fixed value, which means everything will be thrown into one node, and is only suitable for small amounts of data.

### Using the CLI

The `filo-cli` accepts arguments as key-value pairs. The following keys are supported:

| key          | purpose                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
|--------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| dataset    | It is required for all the operations. Its value should be the name of the dataset                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| limit      | This is optional key to be used with `select`. Its value should be the number of rows required.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| columns    | This is required for defining the schema of a dataset. Its value should be a comma-separated string of the format, `column1:typeOfColumn1,column2:typeOfColumn2` where column1 and column2 are the names of the columns and typeOfColumn1 and typeOfColumn2 are one of `int`,`long`,`double`, `string`                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| sortColumn | This is required for defining the schema. Its value should be the name of the column on which the data is to be sorted.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| command    | Its value can be either of `init`,`create`,`importcsv` or `list`.<br/><br/>The `init` command is used to create the FiloDB schema.<br/><br/>The `create` command is used to define new a dataset. For example,<br/>```./filo-cli --command create --dataset playlist --columns id:int,album:string,artist:string,title:string --sortColumn id``` <br/>Note: The sort column is not optional.<br/><br/>The `list` command can be used to view the schema of a dataset. For example, <br/>```./filo-cli --command list --dataset playlist```<br/><br/>The `importcsv` command can be used to load data from a CSV file into a dataset. For example,<br/>```./filo-cli --command importcsv --dataset playlist --filename playlist.csv```<br/>Note: The CSV file should be delimited with comma and have a header row. The column names must match those specified when creating the schema for that dataset. |
| select     | Its value should be a comma-separated string of the columns to be selected,<br/>```./filo-cli --dataset playlist --select album,title```<br/>The result from `select` is printed in the console by default. An output file can be specified with the key `--outfile`. For example,<br/>```./filo-cli --dataset playlist --select album,title --outfile playlist.csv```                                                                                                                                                                                                                                                                                                                                                                                                                   |
| delimiter  | This is optional key to be used with `importcsv` command. Its value should be the field delimiter character. Default value is comma.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |

#### CLI Example
The following examples use the [GDELT public dataset](http://data.gdeltproject.org/documentation/GDELT-Data_Format_Codebook.pdf) and can be run from the project directory.

Create a dataset with all the columns :

```
./filo-cli --command create --dataset gdelt --columns GLOBALEVENTID:int,SQLDATE:string,MonthYear:int,Year:int,FractionDate:double,Actor1Code:string,Actor1Name:string,Actor1CountryCode:string,Actor1KnownGroupCode:string,Actor1EthnicCode:string,Actor1Religion1Code:string,Actor1Religion2Code:string,Actor1Type1Code:string,Actor1Type2Code:string,Actor1Type3Code:string,Actor2Code:string,Actor2Name:string,Actor2CountryCode:string,Actor2KnownGroupCode:string,Actor2EthnicCode:string,Actor2Religion1Code:string,Actor2Religion2Code:string,Actor2Type1Code:string,Actor2Type2Code:string,Actor2Type3Code:string,IsRootEvent:int,EventCode:string,EventBaseCode:string,EventRootCode:string,QuadClass:int,GoldsteinScale:double,NumMentions:int,NumSources:int,NumArticles:int,AvgTone:double,Actor1Geo_Type:int,Actor1Geo_FullName:string,Actor1Geo_CountryCode:string,Actor1Geo_ADM1Code:string,Actor1Geo_Lat:double,Actor1Geo_Long:double,Actor1Geo_FeatureID:int,Actor2Geo_Type:int,Actor2Geo_FullName:string,Actor2Geo_CountryCode:string,Actor2Geo_ADM1Code:string,Actor2Geo_Lat:double,Actor2Geo_Long:double,Actor2Geo_FeatureID:int,ActionGeo_Type:int,ActionGeo_FullName:string,ActionGeo_CountryCode:string,ActionGeo_ADM1Code:string,ActionGeo_Lat:double,ActionGeo_Long:double,ActionGeo_FeatureID:int,DATEADDED:string,Actor1Geo_FullLocation:string,Actor2Geo_FullLocation:string,ActionGeo_FullLocation:string --sortColumn GLOBALEVENTID
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

### Using FiloDB data-source with Spark

FiloDB has a Spark data-source module - `filodb.spark`. So, you can use the Spark Dataframes `read` and `write` APIs with FiloDB. To use it, follow the steps below

1. Start Cassandra and update project configuration if required.
2. From the FiloDB project directory, execute,
   ```
   $ sbt clean
   $ ./filo-cli --command init
   $ sbt spark/assembly
   ```
3. Use the jar `FiloDB/spark/target/scala-2.10/filodb-spark-assembly-0.1-SNAPSHOT.jar` with Spark 1.4.x.

The options to use with the data-source api are:

| option           | value                                                            | command    | optional |
|------------------|------------------------------------------------------------------|------------|----------|
| dataset          | name of the dataset                                              | read/write | No       |
| sort_column      | name of the column according to which the data should be sorted  | write      | No       |
| partition_column | name of the column according to which data should be partitioned | write      | Yes      |

#### Spark data-source Example (spark-shell)

You can follow along using the [Spark Notebook](http://github.com/andypetrella/spark-notebook) in doc/FiloDB.snb....  launch the notebook using `EXTRA_CLASSPATH=$FILO_JAR ADD_JARS=$FILO_JAR ./bin/spark-notebook &` where `FILO_JAR` is the path to `filodb-spark-assembly` jar.

Or you can start a spark-shell locally,

```bash
bin/spark-shell --jars ../FiloDB/spark/target/scala-2.10/filodb-spark-assembly-0.1-SNAPSHOT.jar --packages com.databricks:spark-csv_2.10:1.2.0 --driver-memory 3G --executor-memory 3G
```
Note that FiloDB uses an off-heap direct memory memtable, and you probably need to bump Spark's default direct memory size with a `-XX:MaxDirectMemorySize=1G` or some larger setting.


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
    option("partition_column","Year").
    mode(SaveMode.Overwrite).save()
```

Reading the dataset,
```
val df = sqlContext.read.format("filodb.spark").option("dataset", "gdelt").load()
```

The dataset can be queried using the DataFrame DSL. See the section [Querying Datasets](#querying-datasets) for examples.

#### Spark SQL Example (spark-sql)

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

#### Ingesting and Querying with DataFrames (Old API)

Create a table using the method `saveAsFiloDataset`:

```scala
scala> import filodb.spark._
import filodb.spark._
scala> sqlContext.saveAsFiloDataset(myDF, "table1", sortCol, partCol, createDataset=true)
```

Read using `filoDataset`:

```scala
scala> val df = sqlContext.filoDataset("gdelt")
15/06/04 15:21:41 INFO DCAwareRoundRobinPolicy: Using data-center name 'datacenter1' for DCAwareRoundRobinPolicy (if this is incorrect, please provide the correct datacenter name with DCAwareRoundRobinPolicy constructor)
15/06/04 15:21:41 INFO Cluster: New Cassandra host localhost/127.0.0.1:9042 added
15/06/04 15:21:41 INFO FiloRelation: Read schema for dataset gdelt = Map(ActionGeo_CountryCode -> Column(ActionGeo_CountryCode,gdelt,0,StringColumn,FiloSerializer,false,false), Actor1Geo_FullName -> Column(Actor1Geo_FullName,gdelt,0,StringColumn,FiloSerializer,false,false), Actor2Name -> Column(Actor2Name,gdelt,0,StringColumn,FiloSerializer,false,false), ActionGeo_ADM1Code -> Column(ActionGeo_ADM1Code,gdelt,0,StringColumn,FiloSerializer,false,false), Actor2CountryCode -> Column(Actor2CountryCode,gdelt,0,StringColumn,FiloSerializer,fals...
```

The dataset can be queried using the DataFrame DSL. See the section [Querying Datasets](#querying-datasets) for examples.

#### Querying Datasets

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


## Current Status

* The storage format is subject to change at this time.
* Only ingestion through Spark / Spark Streaming, and CLI ingestion via CSV files.
* Only string, Int, Long partition keys and Long/Timestamp/Int/Double sort keys are supported, but many more to come
* You are currently responsible for ensuring that partition keys are not spread across multiple nodes of a DataFrame when ingesting.  It might be as simple as a `df.sort`.

## Roadmap

Your input is appreciated!

* Support for many more data types and sort and partition keys - please give us your input!
* Non-Spark ingestion API.  Your input is again needed.
* In-memory caching for significant query speedup
* Projections.  Often-repeated queries can be sped up significantly with projections.
* Use of GPU and SIMD instructions to speed up queries

## Building and Testing

Run the tests with `sbt test`, or for continuous development, `sbt ~test`.  Noisy cassandra logs can be seen in `filodb-test.log`.

## You can help!

- Send your use cases for OLAP on Cassandra and Spark
    + Especially IoT and Geospatial
- Email if you want to contribute

Your feedback will help decide the next batch of features, such as:
    - which data types to add support for
    - what architecture is best supported
