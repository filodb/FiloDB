# FiloDB and Spark

NOTE: These docs are DEPRECATED.  Currently the Spark module is not working.  If you are interested in helping to revive this module and these docs, please raise an issue and/or use the mailing lists.  Thank you.

## Using FiloDB Data Source with Spark

FiloDB has a Spark data-source module - `filodb.spark`. So, you can use the Spark Dataframes `read` and `write` APIs with FiloDB. To use it, follow the steps below

1. Start Cassandra and update project configuration if required.
2. From the FiloDB project directory, execute,
   ```
   $ sbt clean
   $ ./filo-cli --command init
   $ sbt spark/assembly
   ```
3. Start the Spark Shell with the FiloDB Spark assembly jar, like this:
    ```
    bin/spark-shell --jars ~/src/github/FiloDB/spark/target/scala-2.11/filodb-spark-assembly-0.8-SNAPSHOT.jar   --packages com.databricks:spark-csv_2.11:1.4.0 \
      --conf spark.sql.shuffle.partitions=4 --driver-memory 10G --executor-memory 10G \
      --master 'local[4]' --driver-java-options '-XX:+UseG1GC -XX:MaxGCPauseMillis=500 \
      -Dconfig.file=src/github/FiloDB/conf/timeseries-filodb-server.conf'
    ```

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

NOTE: Most of this is deprecated.

Reading the dataset,
```
val df = spark.read.format("filodb.spark").option("dataset", "gdelt").load()
```

The dataset can be queried using the DataFrame DSL. See the section [Querying Datasets](#querying-datasets) for examples.

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

