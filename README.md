# FiloDB

[![Join the chat at https://gitter.im/velvia/FiloDB](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/velvia/FiloDB?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
Distributed.  Columnar.  Versioned.

```
    _______ __      ____  ____ 
   / ____(_) /___  / __ \/ __ )
  / /_  / / / __ \/ / / / __  |
 / __/ / / / /_/ / /_/ / /_/ / 
/_/   /_/_/\____/_____/_____/  
```

FiloDB is a new open-source database based on Apache Cassandra and Spark SQL.  FiloDB brings breakthrough performance levels for analytical queries by using a columnar storage layout with different space-saving techniques like dictionary compression.  At the same time, row-level, column-level operations and built in versioning gives FiloDB far more flexibility than can be achieved using file-based technologies like Parquet alone.  

* FiloDB aim's to bring one to two orders of magnitude speedups over OLAP performance of Cassandra 2.x CQL tables + Spark.  For the POC performance comparison, please see [cassandra-gdelt](http://github.com/velvia/cassandra-gdelt) repo.
* Enable easy exactly-once ingestion from Kafka for streaming geospatial applications. 
* Incrementally computed columns and geospatial annotations
* MPP-like automatic caching of projections from popular queries for fast results

FiloDB is a great fit for bulk analytical workloads, or streaming / append-only event data.  It is not optimized for heavily transactional, update-oriented workflows.

[Overview presentation](http://velvia.github.io/presentations/2014-filodb/#/) -- see the docs folder for design docs.

To compile the .mermaid source files to .png's, install the [Mermaid CLI](http://knsv.github.io/mermaid/mermaidCLI.html).

## Current Status

Definitely alpha or pre-alpha.  What is here is more intended to show what is possible with columnar storage on Cassandra combined with Spark, and gather feedback.
- Append-only
- CSV ingest only, although adding additional ingestion types (like Kafa) is not hard - see `CsvSourceActor`.
- Keyed by partition and row number only
- Only int, double, long, and string types
- Localhost only
- No locality in Spark input source

Also, the design and architecture are heavily in flux.

## Building and Testing

Run the tests with `sbt test`, or for continuous development, `sbt ~test`.  Noisy cassandra logs can be seen in `filodb-test.log`.

## Using the CLI

Create a dataset with all the columns from the [GDELT public dataset](http://data.gdeltproject.org/documentation/GDELT-Data_Format_Codebook.pdf):

```
sbt "cli/run --command create --dataset gdelt --columns GLOBALEVENTID:int,SQLDATE:string,MonthYear:int,Year:int,FractionDate:double,Actor1Code:string,Actor1Name:string,Actor1CountryCode:string,Actor1KnownGroupCode:string,Actor1EthnicCode:string,Actor1Religion1Code:string,Actor1Religion2Code:string,Actor1Type1Code:string,Actor1Type2Code:string,Actor1Type3Code:string,Actor2Code:string,Actor2Name:string,Actor2CountryCode:string,Actor2KnownGroupCode:string,Actor2EthnicCode:string,Actor2Religion1Code:string,Actor2Religion2Code:string,Actor2Type1Code:string,Actor2Type2Code:string,Actor2Type3Code:string,IsRootEvent:int,EventCode:string,EventBaseCode:string,EventRootCode:string,QuadClass:int,GoldsteinScale:double,NumMentions:int,NumSources:int,NumArticles:int,AvgTone:double,Actor1Geo_Type:int,Actor1Geo_FullName:string,Actor1Geo_CountryCode:string,Actor1Geo_ADM1Code:string,Actor1Geo_Lat:double,Actor1Geo_Long:double,Actor1Geo_FeatureID:int,Actor2Geo_Type:int,Actor2Geo_FullName:string,Actor2Geo_CountryCode:string,Actor2Geo_ADM1Code:string,Actor2Geo_Lat:double,Actor2Geo_Long:double,Actor2Geo_FeatureID:int,ActionGeo_Type:int,ActionGeo_FullName:string,ActionGeo_CountryCode:string,ActionGeo_ADM1Code:string,ActionGeo_Lat:double,ActionGeo_Long:double,ActionGeo_FeatureID:int,DATEADDED:string,Actor1Geo_FullLocation:string,Actor2Geo_FullLocation:string,ActionGeo_FullLocation:string"
```

You could also add columns later with the same syntax.

Create a partition:

```
sbt "cli/run --command create --dataset gdelt --partition first"
```

Verify the dataset metadata:

```
sbt "cli/run --command list --dataset gdelt"
```

Import a CSV file:

```
sbt "cli/run --command importcsv --dataset gdelt --partition first --filename GDELT_1979-1984.csv"
```

Query/export some columns:

```
sbt "cli/run --dataset gdelt --partition first --select MonthYear,Actor2Code"
```

## Using the Spark SQL query engine

Build the spark input source module with `sbt spark/assembly`.  Then, CD into a Spark 1.3.1 distribution (1.3.0 and onwards should work), and start spark-shell with something like:

```
bin/spark-shell --jars ../FiloDB/spark/target/scala-2.10/filodb-spark-assembly-0.1-SNAPSHOT.jar
```

Create a config, then create a dataframe on the above dataset:

```scala
scala> val config = com.typesafe.config.ConfigFactory.parseString("max-outstanding-futures = 16")
config: com.typesafe.config.Config = Config(SimpleConfigObject({"max-outstanding-futures":16}))

scala> import filodb.spark._
import filodb.spark._

scala> val df = sqlContext.filoDataset(config, "gdelt")
15/06/04 15:21:41 INFO DCAwareRoundRobinPolicy: Using data-center name 'datacenter1' for DCAwareRoundRobinPolicy (if this is incorrect, please provide the correct datacenter name with DCAwareRoundRobinPolicy constructor)
15/06/04 15:21:41 INFO Cluster: New Cassandra host localhost/127.0.0.1:9042 added
15/06/04 15:21:41 INFO FiloRelation: Read schema for dataset gdelt = Map(ActionGeo_CountryCode -> Column(ActionGeo_CountryCode,gdelt,0,StringColumn,FiloSerializer,false,false), Actor1Geo_FullName -> Column(Actor1Geo_FullName,gdelt,0,StringColumn,FiloSerializer,false,false), Actor2Name -> Column(Actor2Name,gdelt,0,StringColumn,FiloSerializer,false,false), ActionGeo_ADM1Code -> Column(ActionGeo_ADM1Code,gdelt,0,StringColumn,FiloSerializer,false,false), Actor2CountryCode -> Column(Actor2CountryCode,gdelt,0,StringColumn,FiloSerializer,fals...
```

You could also verify the schema via `df.printSchema`.

Now do some queries, using the DataFrame DSL:

```scala
scala> df.select(count(df("MonthYear"))).show()
...<skipping lots of logging>...
COUNT(MonthYear)
4037998
```

or SQL:

```scala
scala> df.registerTempTable("gdelt")

scala> sqlContext.sql("SELECT avg(MonthYear) FROM gdelt").collect()
res13: Array[org.apache.spark.sql.Row] = Array([208077.29634561483])
```