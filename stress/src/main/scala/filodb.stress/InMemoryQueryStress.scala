package filodb.stress

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import scala.util.Random
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import filodb.spark._

/**
 * An in-memory concurrency and query stress tester
 * 1) First it ingests the NYC Taxi dataset into memory
 * 2) Then runs tons of queries on it
 *
 * To prepare, download the first month's worth of data from http://www.andresmh.com/nyctaxitrips/
 *
 * Recommended to run this with the first million rows only as a first run to make sure everything works.
 * Run it with LOTS of memory - 8GB recommended
 */
object InMemoryQueryStress extends App {
  val taxiCsvFile = args(0)
  val numRuns = 50    // Make this higher when doing performance profiling

  def puts(s: String): Unit = {
    //scalastyle:off
    println(s)
    //scalastyle:on
  }

  // Queries
  val medallions = Array("23A89BC906FBB8BD110677FBB0B0A6C5",
                         "3F8D8853F7EF89A7630565DDA38E9526",
                         "3FE07421C46041F4801C7711F5971C63",
                         "789B8DC7F3CB06A645B0CDC24591B832",
                         "18E80475A4E491022BC2EF8559DABFD8",
                         "761033F2C6F96EBFA9F578E968FDEDE5",
                         "E4C72E0EE95C31D6B1FEFCF3F876EF90",
                         "AF1421FCAA4AE912BDFC996F8A9B5675",
                         "FB085B55ABF581ADBAD3E16283C78C01",
                         "29CBE2B638D6C9B7239D2CA7A72A70E9")

  // trip info for a single driver within a given time range
  val singleDriverQueries = (1 to 20).map { i =>
    val medallion = medallions(Random.nextInt(medallions.size))
    s"SELECT avg(trip_distance), avg(passenger_count) from nyc_taxi where medallion = '$medallion'" +
    s" AND pickup_datetime > '2013-01-15T00Z' AND pickup_datetime < '2013-01-22T00Z'"
  }

  // average trip distance by day for several days

  val allQueries = singleDriverQueries

  // Setup SparkContext, etc.
  val conf = (new SparkConf).setMaster("local[8]")
                            .setAppName("test")
                            .set("spark.filodb.store", "in-memory")
                            .set("spark.sql.shuffle.partitions", "4")
                            .set("spark.scheduler.mode", "FAIR")
                            .set("spark.ui.enabled", "false")   // No need for UI when doing perf stuff
                            .set("spark.filodb.memtable.min-free-mb", "50")
  val sc = new SparkContext(conf)
  val sql = new SQLContext(sc)

  // Ingest file - note, this will take several minutes
  puts("Starting ingestion...")
  val csvDF = sql.read.format("com.databricks.spark.csv").
                 option("header", "true").option("inferSchema", "true").
                 load(taxiCsvFile)
  csvDF.write.format("filodb.spark").
    option("dataset", "nyc_taxi").
    option("row_keys", "hack_license,pickup_datetime").
    option("segment_key", ":timeslice pickup_datetime 6d").
    option("partition_keys", ":stringPrefix medallion 2").
    mode(SaveMode.Overwrite).save()
  puts("Ingestion done.")

  val taxiDF = sql.filoDataset("nyc_taxi")
  taxiDF.registerTempTable("nyc_taxi")
  val numRecords = taxiDF.count()
  puts(s"Ingested $numRecords records")

  // run queries

  import scala.concurrent.ExecutionContext.Implicits.global

  val cachedDF = new collection.mutable.HashMap[String, DataFrame]

  def getCachedDF(query: String): DataFrame =
    cachedDF.getOrElseUpdate(query, sql.sql(query))

  def runQueries(queries: Array[String], numQueries: Int = 1000): Unit = {
    val startMillis = System.currentTimeMillis
    val futures = (0 until numQueries).map(i => getCachedDF(queries(Random.nextInt(queries.size))).rdd.collectAsync)
    val fut = Future.sequence(futures.asInstanceOf[Seq[Future[Array[_]]]])
    Await.result(fut, Duration.Inf)
    val endMillis = System.currentTimeMillis
    val qps = numQueries / ((endMillis - startMillis) / 1000.0)
    puts(s"Ran $numQueries queries in ${endMillis - startMillis} millis.  QPS = $qps")
  }

  puts("Warming up...")
  runQueries(allQueries.toArray, 100)
  Thread sleep 2000
  puts("Now running queries for real...")
  (0 until numRuns).foreach { i => runQueries(allQueries.toArray) }

  // clean up!
  FiloSetup.shutdown()
  sc.stop()
}