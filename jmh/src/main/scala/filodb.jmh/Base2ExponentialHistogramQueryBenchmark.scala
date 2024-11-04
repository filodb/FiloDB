package filodb.jmh

import java.util.concurrent.TimeUnit

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

import akka.actor.ActorSystem
import ch.qos.logback.classic.{Level, Logger}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.reactive.Observable
import org.openjdk.jmh.annotations._

import filodb.coordinator.queryplanner.SingleClusterPlanner
import filodb.core.SpreadChange
import filodb.core.binaryrecord2.RecordContainer
import filodb.core.memstore.{SomeData, TimeSeriesMemStore}
import filodb.core.metadata.Schemas
import filodb.core.query.{QueryConfig, QueryContext}
import filodb.core.store.StoreConfig
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query.{QueryError => QError, QueryResult => QueryResult2}
import filodb.timeseries.TestTimeseriesProducer

//scalastyle:off regex
/**
 * A macrobenchmark (IT-test level) for QueryEngine2 aggregations, in-memory only (no on-demand paging)
 * No ingestion occurs while query test is running -- this is intentional to allow for query-focused CPU
 * and memory allocation analysis.
 * Ingests a fixed amount of tsgenerator data in Prometheus schema (time, value, tags) and runs various queries.
 */
@State(Scope.Thread)
class Base2ExponentialHistogramQueryBenchmark extends StrictLogging {
  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.DEBUG)

  import filodb.coordinator._
  import client.Client.{actorAsk, asyncAsk}
  import client.QueryCommands._
  import NodeClusterActor._

  val startTime = System.currentTimeMillis - (3600*1000)
  val queryIntervalMin = 55  // # minutes between start and stop

  // TODO: move setup and ingestion to another trait
  val system = ActorSystem("test", ConfigFactory.load("filodb-defaults.conf")
    .withValue("filodb.memstore.ingestion-buffer-mem-size", ConfigValueFactory.fromAnyRef("30MB")))

  private val cluster = FilodbCluster(system)
  cluster.join()

  private val coordinator = cluster.coordinatorActor
  private val clusterActor = cluster.clusterSingleton(ClusterRole.Server, None)

  // Set up Prometheus dataset in cluster, initialize in metastore
  private val dataset = TestTimeseriesProducer.dataset
  private val shardMapper = new ShardMapper(numShards)
  (0 until numShards).foreach { s =>
    shardMapper.updateFromEvent(IngestionStarted(dataset.ref, s, coordinator))
  }

  Await.result(cluster.metaStore.initialize(), 3.seconds)

  val storeConf = StoreConfig(ConfigFactory.parseString("""
                                                          | flush-interval = 1h
                                                          | shard-mem-size = 96MB
                                                          | groups-per-shard = 4
                                                          | demand-paging-enabled = false
                  """.stripMargin))
  val command = SetupDataset(dataset, DatasetResourceSpec(numShards, 1), noOpSource, storeConf)
  actorAsk(clusterActor, command) { case DatasetVerified => println(s"dataset setup") }
  coordinator ! command

  val queryConfig = QueryConfig(cluster.settings.allConfig.getConfig("filodb.query"))

  import monix.execution.Scheduler.Implicits.global

  final val numShards = 8
  final val numSamplesPerTs = 720   // 2 hours * 3600 / 10 sec interval
  final val numSeries = 100
  final val numQueries = 100
  final val numBuckets = 320
  val spread = 3

  // Manually pump in data ourselves so we know when it's done.
  // TODO: ingest into multiple shards
  Thread sleep 2000    // Give setup command some time to set up dataset shards etc.
  val (producingFut, containerStream) = TestTimeseriesProducer.metricsToContainerStream(startTime, numShards, numSeries,
    numMetricNames = 1, numSamplesPerTs * numSeries, dataset, shardMapper, spread,
    publishIntervalSec = 10, expHist = true)
  val ingestTask = containerStream.groupBy(_._1)
    // Asynchronously subcribe and ingest each shard
    .mapParallelUnordered(numShards) { groupedStream =>
      val shard = groupedStream.key
      println(s"Starting ingest exp histograms on shard $shard...")
      val shardStream = groupedStream.zipWithIndex.flatMap { case ((_, bytes), idx) =>
        val data = bytes.map { array => SomeData(RecordContainer(array), idx) }
        Observable.fromIterable(data)
      }
      Task.fromFuture(cluster.memStore.startIngestion(dataset.ref, shard, shardStream, global))
    }.countL.runToFuture
  Await.result(producingFut, 90.seconds)
  Thread sleep 2000
  cluster.memStore.asInstanceOf[TimeSeriesMemStore].refreshIndexForTesting(dataset.ref) // commit lucene index
  println(s"Ingestion ended")

  // Stuff for directly executing queries ourselves
  val engine = new SingleClusterPlanner(dataset, Schemas(dataset.schema), shardMapper, 0,
    queryConfig, "raw")

  /**
   * ## ========  Queries ===========
   * They are designed to match all the time series (common case) under a particular metric and job
   */
  val histQuantileQuery =
    """histogram_quantile(0.7, sum(rate(http_request_latency_delta{_ws_="demo",_ns_="App-2"}[5m])))"""
  val queries = Seq(histQuantileQuery)
  val queryTime = startTime + (7 * 60 * 1000)  // 5 minutes from start until 60 minutes from start
  val queryStep = 120        // # of seconds between each query sample "step"
  val qParams = TimeStepParams(queryTime/1000, queryStep, (queryTime/1000) + queryIntervalMin*60)
  val logicalPlans = queries.map { q => Parser.queryRangeToLogicalPlan(q, qParams) }
  val queryCommands = logicalPlans.map { plan =>
    LogicalPlan2Query(dataset.ref, plan, QueryContext(Some(new StaticSpreadProvider(SpreadChange(0, spread))), 20000))
  }

  var queriesSucceeded = 0
  var queriesFailed = 0

  @TearDown
  def shutdownFiloActors(): Unit = {
    cluster.shutdown()
    println(s"Succeeded: $queriesSucceeded   Failed: $queriesFailed")
  }

  // Window = 5 min and step=2.5 min, so 50% overlap
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(numQueries)
  def histQuantileQueries(): Unit = {
    val futures = (0 until numQueries).map { n =>
      val qCmd = queryCommands(n % queryCommands.length)
      val time = System.currentTimeMillis
      val f = asyncAsk(coordinator, qCmd.copy(qContext = qCmd.qContext.copy(queryId = n.toString, submitTime = time)))
      f.foreach {
        case q: QueryResult2 => queriesSucceeded += 1
        case e: QError       => queriesFailed += 1
      }
      f
    }
    Await.result(Future.sequence(futures), 60.seconds)
  }


//  val querySched = Scheduler.singleThread(s"benchmark-query")
//
//  // NOTE: cannot really be compared with above because this is query witin one shard only!!  However running the
//  // query single threaded makes it easier to figure out where the performance hit is.
//  @Benchmark
//  @BenchmarkMode(Array(Mode.Throughput))
//  @OutputTimeUnit(TimeUnit.SECONDS)
//  @OperationsPerInvocation(numQueries)
//  def singleThreadedRawQuery(): Long = {
//    val querySession = QuerySession(qContext, queryConfig)
//
//    val f = Observable.fromIterable(0 until numQueries).mapEval { n =>
//        execPlan.execute(cluster.memStore, querySession)(querySched)
//      }.executeOn(querySched)
//      .countL.runToFuture
//    Await.result(f, 60.seconds)
//  }

}
