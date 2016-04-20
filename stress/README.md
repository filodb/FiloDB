## FiloDB Stress Tests

To build:

* In SBT, `stress/assembly`

To run:

In your Spark distribution:

    spark-submit --driver-memory 5g --executor-memory 5g --master 'local-cluster[2,1,2048]' \
      --class filodb.stress.<stress test main class> <path-to-assembly-jar>  <args> 

The above example runs in `local-cluster` mode with 2 worker processes each with 2GB of memory.

### InMemoryQueryStress

Read/concurrency stress test, takes NYC Taxi dataset as an input.  It's recommended to use a subset, say 1 million rows, for the test.

TODO: create a main class that runs all the tests