package filodb.stress

import com.opencsv.CSVParser
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime
import scala.concurrent.Future

import filodb.core.DatasetRef
import filodb.spark._

/**
 * Continuous streaming ingestion + queries stress test - basically the real-time ingestion use case.
 * Tests for accuracy of data thus far ingested plus performance.
 *
 * To prepare, download the first month's worth of data from http://www.andresmh.com/nyctaxitrips/
 * Also, run this to initialize the filo-stress keyspace:
 *   `filo-cli --database filostress --command init`
 *
 * Strip the first line of each CSV file, it is not needed and will cause problems.
 *
 * Pass the _directory_ (note: not filename) containing files to feed in here.  Could keep adding new
 * files to get more input.
 * Recommended to run this with the first million rows only as a first run to make sure everything works.
 * Test at different memory settings - but recommend minimum 4G.
 *
 * Also, if you run this locally, run it using local-cluster to test clustering effects.
 */
object StreamingStress extends App {
  val taxiCsvDir = args(0)
  val numRuns = 250    // Make this higher when doing performance profiling

  def puts(s: String): Unit = {
    //scalastyle:off
    println(s)
    //scalastyle:on
  }

  // Setup SparkContext, etc.
  val conf = (new SparkConf).setAppName("stream-test")
                            .set("spark.filodb.cassandra.keyspace", "filostress")
                            .set("spark.sql.shuffle.partitions", "4")
                            .set("spark.scheduler.mode", "FAIR")
  val sc = new SparkContext(conf)
  val sql = new SQLContext(sc)
  val ssc = new StreamingContext(sc, Milliseconds(1000))

  FiloSetup.init(sc)
  implicit val ec = FiloSetup.ec

  puts(s"Truncating dataset (if it exists already)...")
  try {
    FiloSetup.client.truncateDataset(DatasetRef("taxi_streaming"), 0)
  } catch {
    case filodb.coordinator.client.ClientException(e) => puts(s"Ignoring error $e")
  }

  case class TaxiRecord(medallion: String,
                        hack_license: String,
                        vendor_id: String,
                        rate_code: Int,
                        store_and_fwd_flag: String,
                        pickup_datetime: Long,
                        dropoff_datetime: Long,
                        passenger_count: Int,
                        trip_time_in_secs: Int,
                        trip_distance: Double,
                        pickup_longitude: Double,
                        pickup_latitude: Double,
                        dropoff_longitude: Double,
                        dropoff_latitude: Double)

  val taxiStream = ssc.textFileStream(taxiCsvDir)
  val parser = new CSVParser()
  import sql.implicits._

  private def toTimeMsLong(dtString: String): Long = {
    val dt = DateTime.parse(dtString.replace(" ", "T"))
    dt.getMillis
  }

  taxiStream.map { line =>
               val parts = parser.parseLine(line)
               TaxiRecord(parts(0), parts(1), parts(2), parts(3).toInt,
                          parts(4), toTimeMsLong(parts(5)), toTimeMsLong(parts(6)), parts(7).toInt,
                          parts(8).toInt, parts(9).toDouble, parts(10).toDouble,
                          parts(11).toDouble, parts(12).toDouble, parts(13).toDouble)
             }.foreachRDD { rdd =>
              rdd.toDF.write.format("filodb.spark").
                  option("dataset", "taxi_streaming").
                  option("row_keys", "hack_license,pickup_datetime").
                  option("segment_key", ":timeslice pickup_datetime 6d").
                  option("partition_keys", ":stringPrefix medallion 2").
                  // Flushing after each small batch would be very inefficient...
                  option("flush_after_write", "false").
                  mode(SaveMode.Append).save()
             }

  ssc.start()

  val readingThread = Future {
    val taxiData = sql.filoDataset("taxi_streaming")
    (0 until numRuns).foreach { i =>
      val numRecords = taxiData.count()
      puts(s"Taxi dataset now has   ===>  $numRecords records!")
      Thread sleep 700
    }
  }

  ssc.awaitTerminationOrTimeout(160000)

  // clean up!
  FiloSetup.shutdown()
  sc.stop()
}