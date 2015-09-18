# FiloDB

[![Join the chat at https://gitter.im/velvia/FiloDB](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/velvia/FiloDB?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
Distributed.  Columnar.  Versioned.  Streaming.

```
    _______ __      ____  ____ 
   / ____(_) /___  / __ \/ __ )
  / /_  / / / __ \/ / / / __  |
 / __/ / / / /_/ / /_/ / /_/ / 
/_/   /_/_/\____/_____/_____/  
```

![](Dantat.jpg)

Columnar, versioned layers of data wrapped in a yummy high-performance analytical database engine.

## Run analytical queries up to 100x faster on Spark SQL and Cassandra.

FiloDB is a new open-source distributed, versioned, and columnar analytical database.

* **Distributed** - FiloDB is designed from the beginning to run on best-of-breed distributed, scale-out storage platforms such as Apache Cassandra.  Queries run in parallel in Apache Spark for scale-out ad-hoc analysis.
* **Columnar** - FiloDB brings breakthrough performance levels for analytical queries by using a columnar storage layout with different space-saving techniques like dictionary compression.  The performance is comparable to Parquet, and one to two orders of magnitude faster than Spark on Cassandra 2.x for analytical queries.  For the POC performance comparison, please see [cassandra-gdelt](http://github.com/velvia/cassandra-gdelt) repo.
* **Versioned** - At the same time, row-level, column-level operations and built in versioning gives FiloDB far more flexibility than can be achieved using file-based technologies like Parquet alone.
* Designed for **streaming** - Enable easy exactly-once ingestion from Kafka for streaming events, time series, and IoT applications - yet enable extremely fast ad-hoc analysis using the ease of use of SQL.  Each row is keyed by a partition and sort key, and writes using the same key are idempotent.  FiloDB does the hard work of keeping data stored in an efficient and sorted format.

FiloDB is easy to use!  You can use Spark SQL for both ingestion (including from Streaming!) and querying.  You can connect Tableau or any other JDBC analysis tool to Spark SQL, and easily ingest data from JSON, CSV, any traditional database, Kafka, or any of the many sources supported by Spark.

FiloDB is a great fit for bulk analytical workloads, or streaming /  event data.  It is not optimized for heavily transactional, update-oriented workflows.

[Overview presentation](http://velvia.github.io/presentations/2014-filodb/#/) -- see the docs folder for design docs.

To compile the .mermaid source files to .png's, install the [Mermaid CLI](http://knsv.github.io/mermaid/mermaidCLI.html).

## Current Status

* The storage format is subject to change at this time.
* You can ingest data using Spark, partitioned by a partition key, and sorted using a sort key.  CLI ingestion only by CSV files.
* Only string partition keys and Long/Timestamp/Int/Double sort keys are supported, but many more to come

## Roadmap

Your input is appreciated!

* Support for many more data types and sort and partition keys - please give us your input!
* Non-Spark ingestion API.  Your input is again needed.
* In-memory caching for significant query speedup
* Projections.  Often-repeated queries can be sped up significantly with projections.
* Use of GPU and SIMD instructions to speed up queries

## Getting Started

**YOU WILL NEED JDK 1.8**

First, build the CLI using `sbt cli/assembly`.  This will create an executable in `cli/target/scala-2.10/filo-cli-*`.  In the examples below, the "filo-cli" command is an alias to this executable.

To initialize the C* tables:

     filo-cli --command init

NOTE: the default Cassandra configuration is in `core/src/main/resources/application.conf`.

## Using Spark to ingest and query data

Build the spark data source module with `sbt spark/assembly`.  Then, CD into a Spark 1.4.x distribution (1.4.0 and onwards should work), and start spark-shell with something like:

```
bin/spark-shell --jars ../FiloDB/spark/target/scala-2.10/filodb-spark-assembly-0.1-SNAPSHOT.jar
```

### Ingesting and Querying with DataFrames (New API)

You can use the Spark Dataframes `read` and `write` APIs with FiloDB.  This should also make it possible to create and ingest data using only JDBC, or the SQL Shell.  To create a dataset:

```scala
scala> import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SaveMode
scala> dataDF.write.format("filodb.spark").
              option("dataset", <<dataset-name>>).
              option("sort_column", <<sortcolumn-name>>).
              option("partition_column", <<partitioncolumn-name>>).
              mode(SaveMode.Overwrite).
              save()
```

Note the `OverWrite` save mode, which is not the default and causes the dataset and missing columns to be created.

To read it back:

```scala
val df = sqlContext.read.format("filodb.spark").option("dataset", "gdelt").load()
```

### Detailed Ingestion Example

This example uses a checked in CSV file to walk through ingesting data.  To ingest the CSV, start spark-shell with the spark-csv package:

```bash
bin/spark-shell --jars $REPOS/FiloDB/spark/target/scala-2.10/filodb-spark-assembly-0.1-SNAPSHOT.jar --packages com.databricks:spark-csv_2.10:1.2.0 --driver-memory 3G --executor-memory 3G
```

Loading CSV file from Spark:

```scala
scala> val csvDF = sqlContext.read.format("com.databricks.spark.csv").
           option("header", "true").option("inferSchema", "true").
           load("GDELT-1979-1984-100000.csv")
```

Now, we have to decide on a sort and partitioning column.  The partitioning column decides how data is going to be distributed across the cluster, while the sort column acts as a primary key within each partition and decides how data will be sorted within each partition.

If a partitioning column is not specified, FiloDB's Spark API will create a default one with the name "_partition", with a fixed value, which means everything will be thrown into one node, and is only suitable for small amounts of data.  The following command will add the default partition column, and create the dataset too:

```scala
scala> import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SaveMode
scala> csvDF.write.format("filodb.spark").
             option("dataset", "gdelt").option("sort_column", "GLOBALEVENTID").
             mode(SaveMode.Overwrite).save()
```

- Write data without partition column

- DIscuss how to create a partition column
- Write data with partition column

### Ingesting and Querying with DataFrames (Old API)

The easiest way to create a table, its columns, and ingest data is to use the implicit method `saveAsFiloDataset`:

```scala
scala> import filodb.spark._
import filodb.spark._
scala> sqlContext.saveAsFiloDataset(myDF, "table1", sortCol, partCol, createDataset=true)
```

Currently it does not append but rather overwrites, but this will be fixed.
Reading is just as easy:

```scala
scala> val df = sqlContext.filoDataset("gdelt")
15/06/04 15:21:41 INFO DCAwareRoundRobinPolicy: Using data-center name 'datacenter1' for DCAwareRoundRobinPolicy (if this is incorrect, please provide the correct datacenter name with DCAwareRoundRobinPolicy constructor)
15/06/04 15:21:41 INFO Cluster: New Cassandra host localhost/127.0.0.1:9042 added
15/06/04 15:21:41 INFO FiloRelation: Read schema for dataset gdelt = Map(ActionGeo_CountryCode -> Column(ActionGeo_CountryCode,gdelt,0,StringColumn,FiloSerializer,false,false), Actor1Geo_FullName -> Column(Actor1Geo_FullName,gdelt,0,StringColumn,FiloSerializer,false,false), Actor2Name -> Column(Actor2Name,gdelt,0,StringColumn,FiloSerializer,false,false), ActionGeo_ADM1Code -> Column(ActionGeo_ADM1Code,gdelt,0,StringColumn,FiloSerializer,false,false), Actor2CountryCode -> Column(Actor2CountryCode,gdelt,0,StringColumn,FiloSerializer,fals...
```

You could also verify the schema via `df.printSchema`.

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

First, build the CLI using `sbt cli/assembly`.  This will create an executable in `cli/target/scala-2.10/filo-cli-*`.  In the examples below, the "filo-cli" command is an alias to this executable.

To initialize the C* tables:

     filo-cli --command init

Create a dataset with all the columns from the [GDELT public dataset](http://data.gdeltproject.org/documentation/GDELT-Data_Format_Codebook.pdf):

```
filo-cli --command create --dataset gdelt --columns GLOBALEVENTID:int,SQLDATE:string,MonthYear:int,Year:int,FractionDate:double,Actor1Code:string,Actor1Name:string,Actor1CountryCode:string,Actor1KnownGroupCode:string,Actor1EthnicCode:string,Actor1Religion1Code:string,Actor1Religion2Code:string,Actor1Type1Code:string,Actor1Type2Code:string,Actor1Type3Code:string,Actor2Code:string,Actor2Name:string,Actor2CountryCode:string,Actor2KnownGroupCode:string,Actor2EthnicCode:string,Actor2Religion1Code:string,Actor2Religion2Code:string,Actor2Type1Code:string,Actor2Type2Code:string,Actor2Type3Code:string,IsRootEvent:int,EventCode:string,EventBaseCode:string,EventRootCode:string,QuadClass:int,GoldsteinScale:double,NumMentions:int,NumSources:int,NumArticles:int,AvgTone:double,Actor1Geo_Type:int,Actor1Geo_FullName:string,Actor1Geo_CountryCode:string,Actor1Geo_ADM1Code:string,Actor1Geo_Lat:double,Actor1Geo_Long:double,Actor1Geo_FeatureID:int,Actor2Geo_Type:int,Actor2Geo_FullName:string,Actor2Geo_CountryCode:string,Actor2Geo_ADM1Code:string,Actor2Geo_Lat:double,Actor2Geo_Long:double,Actor2Geo_FeatureID:int,ActionGeo_Type:int,ActionGeo_FullName:string,ActionGeo_CountryCode:string,ActionGeo_ADM1Code:string,ActionGeo_Lat:double,ActionGeo_Long:double,ActionGeo_FeatureID:int,DATEADDED:string,Actor1Geo_FullLocation:string,Actor2Geo_FullLocation:string,ActionGeo_FullLocation:string --sortColumn GLOBALEVENTID
```

Note that the sort column must be specified.

Verify the dataset metadata:

```
filo-cli --command list --dataset gdelt
```

Import a CSV file (note: it must have a header row, the column names must match what was created before, and must be comma-delimited):

```
filo-cli --command importcsv --dataset gdelt --partition first --filename GDELT_1979-1984.csv
```

Query/export some columns:

```
filo-cli --dataset gdelt --partition first --select MonthYear,Actor2Code
```

## Building and Testing

Run the tests with `sbt test`, or for continuous development, `sbt ~test`.  Noisy cassandra logs can be seen in `filodb-test.log`.

## You can help!

- Send your use cases for OLAP on Cassandra and Spark
    + Especially IoT and Geospatial
- Email if you want to contribute

Your feedback will help decide the next batch of features, such as:
    - which data types to add support for
    - what architecture is best supported

