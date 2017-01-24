<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Datasets](#datasets)
- [Reading Material (Mostly for FiloDB research)](#reading-material-mostly-for-filodb-research)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Datasets

* [GDELT public dataset](http://data.gdeltproject.org/documentation/GDELT-Data_Format_Codebook.pdf) - A geo-spatial-political event dataset.  Ingest the first 4 million lines (1979-1984), which comes out to about 1.1GB CSV file.  Entire dataset is more than 250 million rows / 250GB.  See the discussion on README about different modelling options.
    - To ingest:

```scala
val csvDF = sqlContext.read.format("com.databricks.spark.csv").
              option("header", "true").option("inferSchema", "true").
              load(pathToGdeltCsv)
import org.apache.spark.sql.SaveMode
csvDF.write.format("filodb.spark").
             option("dataset", "gdelt").
             option("row_keys", "GLOBALEVENTID").
             option("partition_keys", "MonthYear").
             mode(SaveMode.Overwrite).save()
```

* [NYC Taxi Trip and Fare Info](http://www.andresmh.com/nyctaxitrips/) - really interesting geospatial-temporal public time series data.  Contains New York City taxi transactions, and an example of how to handle time series / IoT with many entities. Trip data is 2.4GB for one part, ~ 15 million rows, and there are 12 parts.
    - `select(count("medallion")).show` should result in 14776615 records for the `trip_data_1.csv` (Part 1). 
    - Partition by string prefix of medallion gives a pretty even distribution, into 676 shards, of all taxi transactions.  Note that even with this level of sharding, reading data for one taxi/medallion for a given time range is still pretty fast.
    - Putting `pickup_datetime` first in row key allows range queries by time.

```scala
val taxiDF = sqlContext.read.format("com.databricks.spark.csv").
               option("header", "true").option("inferSchema", "true").
               load(pathToNycTaxiCsv)
import org.apache.spark.sql.SaveMode
taxiDF.write.format("filodb.spark").
  option("dataset", "nyc_taxi").
  option("row_keys", "pickup_datetime,medallion,hack_license").
  option("partition_keys", ":stringPrefix medallion 2").
  mode(SaveMode.Overwrite).save()
```

There is a [Spark Notebook](FiloDB_Taxi_Geo_demo.snb) to analyze the NYC Taxi dataset.

* [Weather Datasets and APIs](https://github.com/killrweather/killrweather/wiki/9.-Weather-Data-Sources-and-APIs)
    - Also see the [KillrWeather](https://github.com/killrweather/killrweather/tree/master/data/load) data sets
* [Airline on-time performance data](http://stat-computing.org/dataexpo/2009/) - nearly 120 million records, 16GB uncompressed
* [PCoE NASA Datasets](http://ti.arc.nasa.gov/tech/dash/pcoe/prognostic-data-repository/) - a series of time series datasets used for modeling and prediction of failure scenarios 

## Reading Material (Mostly for FiloDB research)

* [Design and Implementation of Modern Column-Oriented DBs](http://db.csail.mit.edu/pubs/abadi-column-stores.pdf)
* [Positional Update Handling in Column Stores](http://www.cs.cornell.edu/~guoz/Guozhang%20Wang%20slides/Positional%20Update%20Handling%20in%20Column%20Stores.pdf)
* [SnappyData Architecture](http://www.snappydata.io/snappy-industrial) - a hybrid in-memory, Spark-integrated row/column store
