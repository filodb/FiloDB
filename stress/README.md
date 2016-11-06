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