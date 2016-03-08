## FiloDB Stress Tests

To build:

* In SBT, `stress/assembly`

To run:

In your Spark distribution:

    spark-submit <path-to-assembly-jar> --class filodb.stress.<stress test main class>  --driver-memory 5g --executor-memory 5g <args> 

TODO: create a main class that runs all the tests