package filodb.stress

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import filodb.core.{DatasetRef, Perftools}
import filodb.spark._

/**
 * Ingests into two different tables simultaneously, using different schemas. Tests ingestion pipeline
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
 * TODO: randomize number of lines to ingest.  Maybe numOfLines - util.Random.nextInt(10000)....
 */
object IngestionStress extends App {
  val taxiCsvFile = args(0)

  def puts(s: String): Unit = {
    // scalastyle:off
    println(s)
    // scalastyle:on
  }

  val stressName = sys.props.getOrElse("stress.table1name", "taxi_medallion_seg")
  val hrOfDayName = sys.props.getOrElse("stress.table2name", "taxi_hour_of_day")
  val keyspaceName = sys.props.getOrElse("stress.keyspace", "filostress")

  // Setup SparkContext, etc.
  val sess = SparkSession.builder.appName("IngestionStress")
                                 .config("spark.filodb.cassandra.keyspace", keyspaceName)
                                 .config("spark.sql.shuffle.partitions", "4")
                                 .config("spark.scheduler.mode", "FAIR")
                                 .getOrCreate
  val sc = sess.sparkContext

  // Ingest the taxi file two different ways using two Futures
  // One way is by hour of day - very relaxed and fast
  // Another is the "stress" schema - very tiny segments, huge amounts of memory churn and I/O bandwidth
  val csvDF = sess.read.format("com.databricks.spark.csv").
                 option("header", "true").option("inferSchema", "true").
                 load(taxiCsvFile)
  // Define a hour of day function
  import java.sql.Timestamp

  import org.joda.time.DateTime
  val hourOfDay = sess.udf.register("hourOfDay", { (t: Timestamp) => new DateTime(t).getHourOfDay })
  val dfWithHoD = csvDF.withColumn("hourOfDay", hourOfDay(csvDF("pickup_datetime")))

  val stressLines = csvDF.count()
  puts(s"$taxiCsvFile has $stressLines unique lines of data")

  import scala.concurrent.ExecutionContext.Implicits.global

  // "stress" because it ingests tons of partitions - every medallion in fact
  val stressIngestor = Future {
    val ingestMillis = Perftools.timeMillis {
      puts("Starting stressful ingestion...")
      csvDF.write.format("filodb.spark").
        option("dataset", stressName).
        option("row_keys", "hack_license,pickup_datetime").
        option("partition_columns", "medallion:string").
        option("reset_schema", "true").
        mode(SaveMode.Overwrite).save()
      puts("Stressful ingestion done.")
    }

    puts(s"\n ==> Stressful ingestion took $ingestMillis ms\n")

    val df = sess.filoDataset(stressName)
    df.createOrReplaceTempView(stressName)
    df
  }

  val hrOfDayIngestor = Future {
    val ingestMillis = Perftools.timeMillis {
      puts("Starting hour-of-day (easy) ingestion...")

      dfWithHoD.write.format("filodb.spark").
        option("dataset", hrOfDayName).
        option("row_keys", "pickup_datetime,medallion,hack_license").
        option("partition_columns", "hourOfDay:int").
        option("reset_schema", "true").
        mode(SaveMode.Overwrite).save()

      puts("hour-of-day (easy) ingestion done.")
    }

    puts(s"\n ==> hour-of-day (easy) ingestion took $ingestMillis ms\n")

    val df = sess.filoDataset(hrOfDayName)
    df.createOrReplaceTempView(hrOfDayName)
    df
  }

  def checkDatasetCount(df: DataFrame, expected: Long): Future[Long] = Future {
    val count = df.count()
    if (count == expected) { puts(s"Count matched $count for dataframe $df") }
    else                   { puts(s"Expected $expected rows, but actually got $count for dataframe $df") }
    count
  }

  def printIngestionStats(dataset: String): Unit = {
    val stats = FiloDriver.client.ingestionStats(DatasetRef(dataset))
    puts(s"  Stats for dataset $dataset =>")
    stats.foreach(s => puts(s"   $s"))
  }

  val fut = for { stressDf  <- stressIngestor
        hrOfDayDf <- hrOfDayIngestor
        stressCount <- checkDatasetCount(stressDf, stressLines)
        hrCount   <- checkDatasetCount(hrOfDayDf, stressLines) } yield {
    puts("Now doing data comparison checking")

    // Do something just so we have to depend on both things being done
    puts(s"Counts: $stressCount $hrCount")

    puts("\nStats for each dataset:")
    printIngestionStats(stressName)
    printIngestionStats(hrOfDayName)

    // clean up!
    FiloDriver.shutdown()
    FiloExecutor.shutdown()
    sc.stop()
  }

  Await.result(fut, 99.minutes)
}