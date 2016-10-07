package filodb.stress

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import scala.util.Random
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

import filodb.core.{DatasetRef, Perftools}
import filodb.spark._

/**
 * Batch ingestion of a single dataset (NYC Taxi) intended to represent typical ingestion schemas
 * with multi-column partition and row keys.  Not a stress test per se, but intended for performance
 * profiling of a realistic schema.  The GDELT dataset is not as realistic due to a single ID column
 * and the more ordered nature of its sources, whereas the NYC Taxi data is more randomly ordered.
 * The keys have been picked for accuracy, and longer row keys put more stress on ingest performance.
 *
 * To prepare, download the first month's worth of data from http://www.andresmh.com/nyctaxitrips/
 * Also, run this to initialize the filo-stress keyspace:
 *   `filo-cli --database filostress --command init`
 *
 * Recommended to run this with the first million rows only as a first run to make sure everything works.
 * Test at different memory settings - but recommend minimum 4G.
 *
 * Also, if you run this locally, run it using local-cluster to test clustering effects.
 */
object BatchIngestion extends App {
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

  val csvDF = sql.read.format("com.databricks.spark.csv").
                 option("header", "true").option("inferSchema", "true").
                 load(taxiCsvFile)

  val csvLines = csvDF.count()

  val ingestMillis = Perftools.timeMillis {
    puts("Starting batch ingestion...")
    csvDF.sort($"medallion").write.format("filodb.spark").
      option("dataset", "nyc_taxi").
      option("row_keys", "hack_license,medallion,pickup_datetime,pickup_longitude").
      option("segment_key", ":timeslice pickup_datetime 6d").
      option("partition_keys", ":monthOfYear pickup_datetime,:stringPrefix medallion 2").
      mode(SaveMode.Overwrite).save()
    puts("Batch ingestion done.")
  }

  puts(s"\n ==> Batch ingestion took $ingestMillis ms\n")

  val df = sql.filoDataset("nyc_taxi")
  df.registerTempTable("nyc_taxi")

  val count = df.count()
  if (count == csvLines) { puts(s"Count matched $count for dataframe $df") }
  else                   { puts(s"Expected $csvLines rows, but actually got $count for dataframe $df") }

  // clean up!
  FiloDriver.shutdown()
  FiloExecutor.shutdown()
  sc.stop()
}