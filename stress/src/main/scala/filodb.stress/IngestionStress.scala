package filodb.stress

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import scala.util.Random
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

import filodb.core.{DatasetRef, Perftools}
import filodb.spark._

/**
 * Ingests into two different tables simultaneously, one with small segment size. Tests ingestion pipeline
 * handling of tons of concurrent write / I/O, backpressure, accuracy, etc.
 * Reads back both datasets and compares every cell and row to be sure the data is readable and accurate.
 *
 * To prepare, download the first month's worth of data from http://www.andresmh.com/nyctaxitrips/
 * Also, run this to initialize the filo-stress keyspace:
 *   `filo-cli --database filostress --command init`
 *
 * Recommended to run this with the first million rows only as a first run to make sure everything works.
 * Test at different memory settings - but recommend minimum 4G.
 *
 * Also, if you run this locally, run it using local-cluster to test clustering effects.
 *
 * TODO: randomize number of lines to ingest.  Maybe $numOfLines - util.Random.nextInt(10000)....
 */
object IngestionStress extends App {
  val taxiCsvFile = args(0)

  def puts(s: String): Unit = {
    //scalastyle:off
    println(s)
    //scalastyle:on
  }

  // Setup SparkContext, etc.
  val conf = (new SparkConf).setAppName("test")
                            .set("spark.filodb.cassandra.keyspace", "filostress")
                            .set("spark.sql.shuffle.partitions", "4")
                            .set("spark.scheduler.mode", "FAIR")
  val sc = new SparkContext(conf)
  val sql = new SQLContext(sc)
  import sql.implicits._

  // Ingest the taxi file two different ways using two Futures
  // One way is by hour of day - very relaxed and fast
  // Another is the "stress" schema - very tiny segments, huge amounts of memory churn and I/O bandwidth
  val csvDF = sql.read.format("com.databricks.spark.csv").
                 option("header", "true").option("inferSchema", "true").
                 load(taxiCsvFile)
  // Define a hour of day function
  import org.joda.time.DateTime
  import java.sql.Timestamp
  val hourOfDay = sql.udf.register("hourOfDay", { (t: Timestamp) => new DateTime(t).getHourOfDay })
  val dfWithHoD = csvDF.withColumn("hourOfDay", hourOfDay(csvDF("pickup_datetime")))

  val stressLines = csvDF.count()
  val hrLines = dfWithHoD.groupBy($"hourOfDay", $"hack_license", $"pickup_datetime").count().count()
  puts(s"$taxiCsvFile has $stressLines unique lines of data for stress schema, and " +
       s"$hrLines unique lines of data for hour of day schema")

  import scala.concurrent.ExecutionContext.Implicits.global

  val stressIngestor = Future {
    val ingestMillis = Perftools.timeMillis {
      puts("Starting stressful ingestion...")
      csvDF.sort($"medallion").write.format("filodb.spark").
        option("dataset", "taxi_medallion_seg").
        option("row_keys", "hack_license,pickup_datetime").
        option("segment_key", ":stringPrefix medallion 3").
        option("partition_keys", ":stringPrefix medallion 2").
        mode(SaveMode.Overwrite).save()
      puts("Stressful ingestion done.")
    }

    puts(s"\n ==> Stressful ingestion took $ingestMillis ms\n")

    val df = sql.filoDataset("taxi_medallion_seg")
    df.registerTempTable("taxi_medallion_seg")
    df
  }

  val hrOfDayIngestor = Future {
    val ingestMillis = Perftools.timeMillis {
      puts("Starting hour-of-day (easy) ingestion...")

      dfWithHoD.sort($"hourOfDay").write.format("filodb.spark").
        option("dataset", "taxi_hour_of_day").
        option("row_keys", "hack_license,pickup_datetime").
        option("segment_key", ":timeslice pickup_datetime 4d").
        option("partition_keys", "hourOfDay").
        option("reset_schema", "true").
        mode(SaveMode.Overwrite).save()

      puts("hour-of-day (easy) ingestion done.")
    }

    puts(s"\n ==> hour-of-day (easy) ingestion took $ingestMillis ms\n")

    val df = sql.filoDataset("taxi_hour_of_day")
    df.registerTempTable("taxi_hour_of_day")
    df
  }

  def checkDatasetCount(df: DataFrame, expected: Long): Future[Long] = Future {
    val count = df.count()
    if (count == expected) { puts(s"Count matched $count for dataframe $df") }
    else                   { puts(s"Expected $expected rows, but actually got $count for dataframe $df") }
    count
  }

  def printIngestionStats(dataset: String): Unit = {
    val stats = FiloDriver.client.ingestionStats(DatasetRef(dataset), 0)
    puts(s"  Stats for dataset $dataset =>")
    stats.foreach(s => puts(s"   $s"))
  }

  val fut = for { stressDf  <- stressIngestor
        hrOfDayDf <- hrOfDayIngestor
        stressCount <- checkDatasetCount(stressDf, stressLines)
        hrCount   <- checkDatasetCount(hrOfDayDf, hrLines) } yield {
    puts("Now doing data comparison checking")

    // Do something just so we have to depend on both things being done
    puts(s"Counts: $stressCount $hrCount")

    puts("\nStats for each dataset:")
    printIngestionStats("taxi_medallion_seg")
    printIngestionStats("taxi_hour_of_day")

    // clean up!
    FiloDriver.shutdown()
    sc.stop()
  }

  Await.result(fut, 99.minutes)
}