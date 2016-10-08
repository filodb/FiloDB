## FiloDB Stress Tests

To build:

* In SBT, `stress/assembly`

To run:

In your Spark distribution:

    spark-submit --driver-memory 5g --executor-memory 5g --master 'local-cluster[2,1,2048]' \
      --class filodb.stress.<stress test main class> <path-to-assembly-jar>  <args> 

The above example runs in `local-cluster` mode with 2 worker processes each with 2GB of memory.

To get very useful logging of Kamon stats, plus enable Java Mission Control for rich profiling insights, add this option: `--driver-java-options='-Dkamon.metric.tick-interval=30s -Dkamon.statsd.flush-interval=30s -XX:+UnlockCommercialFeatures -XX:+FlightRecorder' --conf spark.filodb.metrics-logger.enabled=true`

### InMemoryQueryStress

Read/concurrency stress test, takes NYC Taxi dataset as an input.  It's recommended to use a subset, say 1 million rows, to test initially, then ramp up to more data.

### StreamingStress

Spark Streaming job to continuously ingest data, and query at the same time

### IngestionStress

Batch based stress ingestion of NYC Taxi dataset with two different schemas, one much easier and one with small segments that will take longer.  Verifies correct row count.

### BatchIngestion

Not a stress test per se but "normal" ingestion using multi column row key and partition key, very realistic

### RowReplaceStress

Similar to BatchIngestion, but designed to inject repeated rows of a variable "replacement factor" to see what its effect would be.  Some test results:

* MBP laptop, C* 2.1.6, Spark 1.6.2, `local[4]` with 5GB driver/executor memory, first million lines of NYC Taxi dataset:
* Writing times includes doing a sort (remove sort when routing work is done)
* Read time is one SQL query, see the source for the query

| Replacement Factor | Injected rows  | Write speed(ms) | Read speed(ms) |
|--------------------|----------------|-----------------|----------------|
|  0.00              | 1000000        | 19825           |  687           |
|  0.25              | 1251497        | 22199           |  632           |
|  0.50              | 1501591        | 22189           |  673           |
|  0.80              | 1799237        | 26053           |  738           |

To prove that the RowReplaceStress actually appends rows to disk as opposed to just extra rows in the memtable, we run the following command:

    ./filo-cli --command analyze --dataset nyc_taxi_replace --database filostress

which shows that indeed there are skipped rows on disk.  Overall, the write speed corresponds to the number of ingested rows, but the read speed corresponds to the number of extra rows actually written - which may be very different since replaced rows in the MemTable are not written to disk.
