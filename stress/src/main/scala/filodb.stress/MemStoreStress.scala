package filodb.stress

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import scala.util.Random
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scalaxy.loops._

import filodb.core.metadata.{Column, RichProjection}
import filodb.core.store.SinglePartitionScan
import filodb.core.{Perftools, DatasetRef}
import filodb.spark.{FiloDriver, FiloRelation, FiloExecutor}

/**
 * A MemStore ingestion and concurrent query stress tester
 * 1) First it ingests the NYC Taxi dataset into memory - one medallion per partition
 * 2) At the same time, we run tons of queries in parallel
 *
 * There are 13,426 medallions in the first month of trips data.  Ingesting into 13k+ diff partitions is
 * something that was not possible with the original memtable swap/flush design, but should be easily
 * achievable with the TimeSeriesMemStore, and we also get real-time queries.
 *
 * To prepare, download the first month's worth of data from http://www.andresmh.com/nyctaxitrips/
 *
 * Recommended to run this with the first million rows only as a first run to make sure everything works.
 * Run it with LOTS of memory - 10GB+ recommended.
 */
object MemStoreStress extends App {
  import monix.execution.Scheduler.Implicits.global

  val taxiCsvFile = args(0)
  val queriesPerThread = 200000
  val queryThreads = 4

  def puts(s: String): Unit = {
    //scalastyle:off
    println(s)
    //scalastyle:on
  }

  // read in medallions
  val medallions = scala.io.Source.fromURL(getClass.getResource("/1000medallions.csv")).getLines.toArray
  val numKeys = medallions.size

  // set up SparkContext
  val conf = (new SparkConf).setMaster("local[8]")
                            .setAppName("MemStoreStress")
                            .set("spark.filodb.store", "in-memory")
                            .set("spark.sql.shuffle.partitions", "4")
                            .set("spark.scheduler.mode", "FAIR")
                            .set("spark.ui.enabled", "false")   // No need for UI when doing perf stuff
                            .set("spark.filodb.memstore.max-chunks-size", "1000")
  val sc = new SparkContext(conf)
  val sql = new SQLContext(sc)

  // ingest CSV data.  NOTE: select only some columns.  The point of the ingest test is not ingestion speed
  // or total volume but rather query and ingest concurrency
  val csvDF = sql.read.format("com.databricks.spark.csv").
                 option("header", "true").option("inferSchema", "true").
                 load(taxiCsvFile).
                 select("medallion", "rate_code", "pickup_datetime", "dropoff_datetime", "passenger_count", "trip_time_in_secs", "trip_distance", "pickup_longitude", "pickup_latitude")

  def getProjAndColumn: (RichProjection, Seq[Column]) = {
    val ref = DatasetRef("nyc_taxi")
    val datasetObj = FiloRelation.getDatasetObj(ref)
    val schema = FiloRelation.getSchema(ref, 0)
    (RichProjection(datasetObj, schema.values.toSeq), Seq(schema("rate_code")))
  }

  // fork thread to do queries
  val queryMillisFut = Future.sequence((0 until queryThreads).map(n => Future {
    // Wait a little bit for ingestion to get going
    Thread sleep 8000
    val (proj, queryColumns) = getProjAndColumn
    puts(s"Starting concurrent querying on columns $queryColumns")
    Perftools.timeMillis {
      for { i <- 0 until queriesPerThread optimized } {
        val partMethod = SinglePartitionScan(proj.partKey(medallions(i % numKeys)))
        FiloExecutor.memStore.scanChunks(proj, queryColumns, 0, partMethod).length
      }
    }
  }))

  puts("Starting memStore ingestion...")
  val ingestMillis = Perftools.timeMillis {
    csvDF.write.format("filodb.spark").
      option("dataset", "nyc_taxi").
      option("row_keys", "pickup_datetime").
      option("segment_key", ":string 0").
      option("partition_keys", "medallion").
      // This is needed for now because memstore write path doesn't handle flush - flushes are N/A to memstore
      option("flush_after_write", "false").
      mode(SaveMode.Overwrite).save()
  }

  // Output results
  puts(s"\n\n==> memStore ingestion was complete in ${ingestMillis/1000.0} seconds")
  val queryMillis = Await.result(queryMillisFut, 1 minute).sum / queryThreads
  val queryThroughput = queriesPerThread * queryThreads * 1000 / queryMillis
  puts(s"\n==> Concurrent querying of $queriesPerThread x $queryThreads threads over $queryMillis ms = $queryThroughput QPS")

  FiloDriver.shutdown()
  FiloExecutor.shutdown()
  sc.stop()
}