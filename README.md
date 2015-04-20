# FiloDB
Distributed.  Columnar.  Versioned.

```
    _______ __      ____  ____ 
   / ____(_) /___  / __ \/ __ )
  / /_  / / / __ \/ / / / __  |
 / __/ / / / /_/ / /_/ / /_/ / 
/_/   /_/_/\____/_____/_____/  
```

Folks implementing OLAP solutions for big data has had to choose between
Parquet-based solutions and proprietary, third party solutions such as HP’s
Vertica, Redshift, etc.  Parquet, being an HDFS file format, has drawbacks when
it comes to easy data deduplication, concurrent workloads such as IoT, memory
usage, and flexibility.  Cassandra and Spark is an interesting combination, but
the performance isn’t quite there yet.

FiloDB is a new open-source OLAP database combining high performance Parquet-
like columnar storage, the flexibility of a database, the familiarity of SQL
queries, the operational ease and robustness of Apache Cassandra, and the power
and integration of Apache Spark.   Highlights:

* FiloDB aim to bring one to two orders of magnitude speedups over OLAP performance of Cassandra 2.x CQL tables + Spark.  For the POC performance comparison, please see [cassandra-gdelt](http://github.com/velvia/cassandra-gdelt) repo.
* FiloDB has built-in versioning capabilities, bringing unprecedented flexibility to the world of OLAP analysis.
* Enable easy exactly-once ingestion from Kafka for streaming geospatial applications. 
* Incremental computed columns and geospatial annotations

[Overview presentation](http://velvia.github.io/presentations/2014-filodb/#/) -- see the docs folder for design docs.

To compile the .mermaid source files to .png's, install the [Mermaid CLI](http://knsv.github.io/mermaid/mermaidCLI.html).

## Building and Testing

Run the tests with `sbt test`, or for continuous development, `sbt ~test`.  Noisy cassandra logs can be seen in `filodb-test.log`.
