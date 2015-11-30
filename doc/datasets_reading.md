## Datasets

* [GDELT public dataset](http://data.gdeltproject.org/documentation/GDELT-Data_Format_Codebook.pdf) - used frequently for bulk ingestion testing.  Ingest the first 4 million lines (1979-1984), which comes out to about 1.1GB CSV file.  Entire dataset is more than 250 million rows / 250GB
    - To ingest:

```scala
val csvDF = sqlContext.read.format("com.databricks.spark.csv").
              option("header", "true").option("inferSchema", "true").
              load(pathToGdeltCsv)
import org.apache.spark.sql.SaveMode
csvDF.write.format("filodb.spark").
  option("dataset", "gdelt").
  option("sort_column", "GLOBALEVENTID").
  option("partition_column", "MonthYear").
  option("default_partition_key", "<none>").
  mode(SaveMode.Overwrite).save()
```

* [NYC Taxi Trip and Fare Info](http://www.andresmh.com/nyctaxitrips/) - really interesting geospatial-temporal public dataset.  Trip data is 2.4GB for one part, ~ 15 million rows, and there are 12 parts.
    - `select(count("medallion")).show` should result in 14776615 records for the `trip_data_1.csv` (Part 1).   CSV takes around 25 secs on my machine.
    - id column is a workaround to make a unique ID out of two columns

```scala
val strPrefix = sqlContext.udf.register("strPrefix", { (s: String, numChars: Int) => s.take(numChars) })
val aInt = new java.util.concurrent.atomic.AtomicInteger(0)
val autoId = sqlContext.udf.register("autoId", { () => aInt.getAndIncrement() })
val tripsWithCols = tripsCsv.
   withColumn("medalPrefix", strPrefix(tripsCsv("medallion"), lit(2))).withColumn("id", autoId())
import org.apache.spark.sql.SaveMode
tripsWithCols.write.format("filodb.spark").option("dataset", "nyctaxitrips").
  option("sort_column", "id").option("partition_column", "medalPrefix").
  option("default_partition_key", "<none>").
  option("segment_size", "100000").mode(SaveMode.Overwrite).save()
```

* [Weather Datasets and APIs](https://github.com/killrweather/killrweather/wiki/9.-Weather-Data-Sources-and-APIs)
    - Also see the [KillrWeather](https://github.com/killrweather/killrweather/tree/master/data/load) data sets
* [Airline on-time performance data](http://stat-computing.org/dataexpo/2009/) - nearly 120 million records, 16GB uncompressed

## Reading Material (Mostly for FiloDB research)

* [Design and Implementation of Modern Column-Oriented DBs](http://db.csail.mit.edu/pubs/abadi-column-stores.pdf)
* [Positional Update Handling in Column Stores](http://www.cs.cornell.edu/~guoz/Guozhang%20Wang%20slides/Positional%20Update%20Handling%20in%20Column%20Stores.pdf)