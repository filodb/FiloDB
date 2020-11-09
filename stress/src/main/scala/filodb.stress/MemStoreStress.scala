package filodb.stress

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.Timeout
import monix.eval.Task
import monix.reactive.Observable
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime

import filodb.coordinator.client.QueryCommands._
import filodb.core.{DatasetRef, Perftools}
import filodb.query._
import filodb.spark.{FiloDriver, FiloExecutor}

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
    // scalastyle:off
    println(s)
    // scalastyle:on
  }

  // read in medallions
  val medallions = scala.io.Source.fromURL(getClass.getResource("/1000medallions.csv")).getLines.toArray
  val numKeys = medallions.size

  val sess = SparkSession.builder()
    .master("local[8]")
    .appName("MemStoreStress")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.scheduler.mode", "FAIR")
    .config("spark.ui.enabled", "false")   // No need for UI when doing perf stuff
    .config("spark.filodb.memstore.max-chunks-size", "1000")
    .getOrCreate()

  // ingest CSV data.  NOTE: select only some columns.  The point of the ingest test is not ingestion speed
  // or total volume but rather query and ingest concurrency
  val csvDF = sess.read.format("com.databricks.spark.csv").
                 option("header", "true").option("inferSchema", "true").
                 load(taxiCsvFile).
                 select("medallion", "rate_code", "pickup_datetime", "dropoff_datetime",
                        "passenger_count", "trip_time_in_secs", "trip_distance",
                        "pickup_longitude", "pickup_latitude")

  // use observables (a stream of queries) to handle queries
  val startTime = DateTime.parse("2013-01-01T00Z").getMillis
  val endTime = DateTime.parse("2013-02-01T00Z").getMillis
  val interval = IntervalSelector(startTime, endTime)
  val ref = DatasetRef("nyc_taxi")
  var startMs = 0L
  var endMs = 0L
  implicit val timeout = Timeout(10 seconds)
  val queryFut = Observable.fromIterable((0 until nQueries))
                   .map { n =>
                     val startIndex = n % (medallions.size - 10)
                     val keys = medallions.slice(startIndex, startIndex + 10).toSeq.map(k => Seq(k))
                     Aggregate(AggregationOperator.Avg,
                        PeriodicSeries(
                          // RawSeries(interval, MultiPartitionQuery(keys), Seq("trip_distance")),
                          RawSeries(interval, Nil, Seq("trip_distance")),
                          startTime, 4*3600*1000, endTime))   // start to end in four hour steps
                   }.mapAsync(queryThreads) { plan =>
                     Perftools.withTrace(Task.fromFuture(FiloExecutor.coordinatorActor ? LogicalPlan2Query(ref, plan)),
                                         "time-series-query")
                   }.delaySubscription(8 seconds)
                   .doOnStart { x => startMs = System.currentTimeMillis }
                   .countL.runAsync
  queryFut.onComplete { case x: Any => endMs = System.currentTimeMillis }

  puts("Starting memStore ingestion...")
  val ingestMillis = Perftools.timeMillis {
    csvDF.write.format("filodb.spark").
      option("dataset", "nyc_taxi").
      option("row_keys", "pickup_datetime").
      option("partition_columns", "medallion:string").
      // This is needed for now because memstore write path doesn't handle flush - flushes are N/A to memstore
      option("flush_after_write", "false").
      mode(SaveMode.Overwrite).save()
  }

  // Output results
  puts(s"\n\n==> memStore ingestion was complete in ${ingestMillis/1000.0} seconds")
  val queryCount = Await.result(queryFut, 1 minute)
  val queryThroughput = nQueries * 1000 / (endMs - startMs)
  puts(s"\n==> Concurrent querying of $nQueries queries ($queryThreads threads) " +
       s"over ${endMs - startMs} ms = $queryThroughput QPS")

  FiloDriver.shutdown()
  FiloExecutor.shutdown()
  sess.stop()
}
