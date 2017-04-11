package filodb.stress

import akka.pattern.ask
import akka.util.Timeout
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import monix.eval.Task
import monix.reactive.Observable
import scala.util.Random
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scalaxy.loops._

import filodb.core.{Perftools, DatasetRef}
import filodb.coordinator.QueryCommands._
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
  val nQueries = 50000
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

  // use observables (a stream of queries) to handle queries
  val queryArgs = QueryArgs("time_group_avg", Seq("pickup_datetime", "trip_distance", "2013-01-01T00Z",
                                                  "2013-02-01T00Z", "90"))
  val ref = DatasetRef("nyc_taxi")
  var startMs = 0L
  var endMs = 0L
  implicit val timeout = Timeout(10 seconds)
  val queryFut = Observable.fromIterable((0 until nQueries))
                   .map { n =>
                     val startIndex = n % (medallions.size - 10)
                     val keys = medallions.slice(startIndex, startIndex + 10).toSeq.map(k => Seq(k))
                     AggregateQuery(ref, 0, queryArgs, MultiPartitionQuery(keys))
                   }.mapAsync(queryThreads) { qMessage =>
                     Task.fromFuture(FiloExecutor.coordinatorActor ? qMessage)
                   }.delaySubscription(8 seconds)
                   .doOnStart { x => startMs = System.currentTimeMillis }
                   .countL.runAsync
  queryFut.onComplete { case x: Any => endMs = System.currentTimeMillis }

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
  val queryCount = Await.result(queryFut, 1 minute)
  val queryThroughput = nQueries * 1000 / (endMs - startMs)
  puts(s"\n==> Concurrent querying of $nQueries queries ($queryThreads threads) over ${endMs - startMs} ms = $queryThroughput QPS")

  FiloDriver.shutdown()
  FiloExecutor.shutdown()
  sc.stop()
}