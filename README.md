# FiloDB
Distributed.  Columnar.  Versioned.

```
    _______ __      ____  ____ 
   / ____(_) /___  / __ \/ __ )
  / /_  / / / __ \/ / / / __  |
 / __/ / / / /_/ / /_/ / /_/ / 
/_/   /_/_/\____/_____/_____/  
```

Apache Parquet has proven that columnar storage layouts can minimize I/O and
speed up scanning for large analytical queries.  Parquet forces developers to
deal with its file-format abstraction, implementing deduplication, granular
updates, indexing, and versioning themselves.  Meanwhile, Cassandra is rock-
solid and has very promising Spark integration, but its storage layout prevents
Spark queries from reaching Parquet levels of performance.  Can we use Cassandra
with columnar storage to power fast queries using Spark?

FiloDB is a new open-source database based on Apache Cassandra and Spark SQL.  FiloDB brings breakthrough performance levels for analytical queries by using a columnar storage layout with compression techniques like dictionary compression.  At the same time, row-level, column-level operations and built in versioning gives FiloDB far more flexibility than can be achieved using Parquet.  

* FiloDB aim's to bring one to two orders of magnitude speedups over OLAP performance of Cassandra 2.x CQL tables + Spark.  For the POC performance comparison, please see [cassandra-gdelt](http://github.com/velvia/cassandra-gdelt) repo.
* Enable easy exactly-once ingestion from Kafka for streaming geospatial applications. 
* Incrementally computed columns and geospatial annotations

FiloDB is a great fit for bulk analytical workloads, or streaming / append-only event data.  It is not optimized for heavily transactional, update-oriented workflows.

[Overview presentation](http://velvia.github.io/presentations/2014-filodb/#/) -- see the docs folder for design docs.

To compile the .mermaid source files to .png's, install the [Mermaid CLI](http://knsv.github.io/mermaid/mermaidCLI.html).

## Building and Testing

Run the tests with `sbt test`, or for continuous development, `sbt ~test`.  Noisy cassandra logs can be seen in `filodb-test.log`.
