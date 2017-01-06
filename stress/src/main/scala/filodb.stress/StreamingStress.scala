package filodb.stress

import com.opencsv.CSVReader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

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
 * Recommended to run this with the first million rows only as a first run to make sure everything works.
 * Test at different memory settings - but recommend minimum 4G.
 *
 * Also, if you run this locally, run it using local-cluster to test clustering effects.
 */
object StreamingStress extends App {
  val taxiCsv = args(0)
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
  val ref = DatasetRef("taxi_streaming")

  FiloDriver.init(sc)
  implicit val ec = FiloDriver.ec

  puts(s"Truncating dataset (if it exists already)...")
  try {
    FiloDriver.client.truncateDataset(ref, 0)
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

  import sql.implicits._

  private def toTimeMsLong(dtString: String): Long = {
    val dt = DateTime.parse(dtString.replace(" ", "T"))
    dt.getMillis
  }

  val queue = collection.mutable.Queue[RDD[TaxiRecord]]()
  ssc.queueStream(queue)
             .foreachRDD { rdd =>
               if (rdd.isEmpty) { puts(" XXXX: EMPTY RDD!!! ") }
               else {
                 rdd.toDF.write.format("filodb.spark").
                    option("dataset", "taxi_streaming").
                    option("row_keys", "hack_license,pickup_datetime").
                    option("segment_key", ":timeslice pickup_datetime 6d").
                    option("partition_keys", ":stringPrefix medallion 2").
                    // Flushing after each small batch would be very inefficient...
                    option("flush_after_write", "false").
                    mode(SaveMode.Append).save()
               }
             }

  ssc.start()

  val csvIngestThread = Future {
    import collection.JavaConverters._

    val reader = new CSVReader(new java.io.FileReader(taxiCsv))
    val columns = reader.readNext.toSeq
    reader.iterator.asScala
          .map { parts =>
               TaxiRecord(parts(0), parts(1), parts(2), parts(3).toInt,
                          parts(4), toTimeMsLong(parts(5)), toTimeMsLong(parts(6)), parts(7).toInt,
                          parts(8).toInt, parts(9).toDouble, parts(10).toDouble,
                          parts(11).toDouble, parts(12).toDouble, parts(13).toDouble)
          }.grouped(3000)
          .foreach { records =>
            val rdd = ssc.sparkContext.parallelize(records)
            queue += rdd
          }
  }

  val readingThread = Future {
    val taxiData = sql.filoDataset("taxi_streaming")
    (0 until numRuns).foreach { i =>
      val numRecords = taxiData.count()
      puts(s"Taxi dataset now has   ===>  $numRecords records!")

      val stats = Try(FiloDriver.client.ingestionStats(ref, 0)).getOrElse("Oops, dataset not there yet")
      puts(s"  ==> Ingestion stats: $stats")
      Thread sleep 700
    }
  }

  Await.result(readingThread, 160000.seconds)
  ssc.awaitTerminationOrTimeout(160000)

  // clean up!
  FiloDriver.shutdown()
  sc.stop()
}