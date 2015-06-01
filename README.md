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

Apache Parquet has proven that columnar storage layouts can minimize I/O and
speed up scanning for large analytical queries.  However, the parquet format
requires developers to address issues such as file-format abstraction, de-
duplication, granular updates, indexing, and versioning.  Meanwhile, Cassandra
is rock-solid and has very promising Spark integration, but its storage layout
prevents Spark queries from reaching Parquet levels of performance.  Can we use
Cassandra with columnar storage to power fast queries using Spark?

FiloDB is a new open-source database based on Apache Cassandra and Spark SQL.  FiloDB brings breakthrough performance levels for analytical queries by using a columnar storage layout with different space-saving techniques like dictionary compression.  At the same time, row-level, column-level operations and built in versioning gives FiloDB far more flexibility than can be achieved using Parquet alone.  

* FiloDB aim's to bring one to two orders of magnitude speedups over OLAP performance of Cassandra 2.x CQL tables + Spark.  For the POC performance comparison, please see [cassandra-gdelt](http://github.com/velvia/cassandra-gdelt) repo.
* Enable easy exactly-once ingestion from Kafka for streaming geospatial applications. 
* Incrementally computed columns and geospatial annotations

FiloDB is a great fit for bulk analytical workloads, or streaming / append-only event data.  It is not optimized for heavily transactional, update-oriented workflows.

[Overview presentation](http://velvia.github.io/presentations/2014-filodb/#/) -- see the docs folder for design docs.

To compile the .mermaid source files to .png's, install the [Mermaid CLI](http://knsv.github.io/mermaid/mermaidCLI.html).

## Current Status

Definitely alpha or pre-alpha.  What is here is more intended to show what is possible with columnar storage on Cassandra combined with Spark.
- Append-only
- CSV ingest only, although adding additional ingestion types (like Kafa) is not hard - see `CsvSourceActor`.
- Keyed by partition and row number
- Only int, double, long, and string types

Also, the design and architecture are heavily in flux.

## Future work

- Lots!  Flexible ingestion patterns, replaces and deletes; flexible primary keys; better integration with Cassandra;

## Building and Testing

Run the tests with `sbt test`, or for continuous development, `sbt ~test`.  Noisy cassandra logs can be seen in `filodb-test.log`.

## Using the CLI

Create a dataset with all the columns from the [GDELT public dataset](http://data.gdeltproject.org/documentation/GDELT-Data_Format_Codebook.pdf):

```
sbt "cli/run --command create --dataset gdelt --columns GLOBALEVENTID:int,SQLDATE:string,MonthYear:int,Year:int,FractionDate:double,Actor1Code:string,Actor1Name:string,Actor1CountryCode:string,Actor1KnownGroupCode:string,Actor1EthnicCode:string,Actor1Religion1Code:string,Actor1Religion2Code:string,Actor1Type1Code:string,Actor1Type2Code:string,Actor1Type3Code:string,Actor2Code:string,Actor2Name:string,Actor2CountryCode:string,Actor2KnownGroupCode:string,Actor2EthnicCode:string,Actor2Religion1Code:string,Actor2Religion2Code:string,Actor2Type1Code:string,Actor2Type2Code:string,Actor2Type3Code:string,IsRootEvent:int,EventCode:string,EventBaseCode:string,EventRootCode:string,QuadClass:int,GoldsteinScale:double,NumMentions:int,NumSources:int,NumArticles:int,AvgTone:double,Actor1Geo_Type:int,Actor1Geo_FullName:string,Actor1Geo_CountryCode:string,Actor1Geo_ADM1Code:string,Actor1Geo_Lat:double,Actor1Geo_Long:double,Actor1Geo_FeatureID:int,Actor2Geo_Type:int,Actor2Geo_FullName:string,Actor2Geo_CountryCode:string,Actor2Geo_ADM1Code:string,Actor2Geo_Lat:double,Actor2Geo_Long:double,Actor2Geo_FeatureID:int,ActionGeo_Type:int,ActionGeo_FullName:string,ActionGeo_CountryCode:string,ActionGeo_ADM1Code:string,ActionGeo_Lat:double,ActionGeo_Long:double,ActionGeo_FeatureID:int,DATEADDED:string,Actor1Geo_FullLocation:string,Actor2Geo_FullLocation:string,ActionGeo_FullLocation:string"
```

You could also add columns later with the same syntax.

Verify the dataset metadata:

```
sbt "cli/run --command list --dataset gdelt"
```

Create a partition:

```
sbt "cli/run --command create --dataset gdelt --partition first"
```

Import a CSV file:

```
sbt "cli/run --command importcsv --dataset gdelt --partition first --filename GDELT_1979-1984.csv"
```