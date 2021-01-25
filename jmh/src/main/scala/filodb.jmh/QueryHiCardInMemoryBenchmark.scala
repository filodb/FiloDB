package filodb.jmh

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.ActorSystem
import ch.qos.logback.classic.{Level, Logger}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.reactive.Observable
import org.openjdk.jmh.annotations._

import filodb.coordinator.queryplanner.SingleClusterPlanner
import filodb.core.binaryrecord2.RecordContainer
import filodb.core.memstore.{SomeData, TimeSeriesMemStore}
import filodb.core.metadata.Schemas
import filodb.core.query.{PlannerParams, PromQlQueryParams, QueryConfig, QueryContext, QuerySession}
import filodb.core.store.StoreConfig
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query.exec.ExecPlan
import filodb.timeseries.TestTimeseriesProducer

//scalastyle:off regex
/**
 * Benchmark for high-cardinality sum() aggregation, each query aggregates 2000 time series.
 * Scan is performed at the lowest leaf of an ExecPlan tree for various queries
 */
@State(Scope.Thread)
class QueryHiCardInMemoryBenchmark extends StrictLogging {
  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.WARN)

  import filodb.coordinator._
  import client.Client.actorAsk
  import NodeClusterActor._

  val numShards = 1
  val numSeries = 8000   // NOTE: num of time series queried is this / 4
  val samplesDuration = 15.minutes
  val numSamples = (samplesDuration / 10.seconds).toInt   // # samples PER TIME SERIES
  val ingestionStartTime = System.currentTimeMillis - samplesDuration.toMillis
  val spread = 0
  val config = ConfigFactory.load("filodb-defaults.conf")
  val queryConfig = new QueryConfig(config.getConfig("filodb.query"))
  implicit val _ = queryConfig.askTimeout

  // TODO: move setup and ingestion to another trait
  val system = ActorSystem("test", config)
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
                  | shard-mem-size = 2GB
                  | ingestion-buffer-mem-size = 1GB
                  | groups-per-shard = 4
                  | demand-paging-enabled = false
                  """.stripMargin))
  val command = SetupDataset(dataset, DatasetResourceSpec(numShards, 1), noOpSource, storeConf)
  actorAsk(clusterActor, command) { case DatasetVerified => println(s"dataset setup") }
  coordinator ! command

  import monix.execution.Scheduler.Implicits.global

  // Manually pump in data ourselves so we know when it's done.
  Thread sleep 2000    // Give setup command some time to set up dataset shards etc.
  val (producingFut, containerStream) = TestTimeseriesProducer.metricsToContainerStream(ingestionStartTime,
    numShards, numSeries, numSamples * numSeries, dataset, shardMapper, spread)
  val ingestTask = containerStream.groupBy(_._1)
                    // Asynchronously subcribe and ingest each shard
                    .mapAsync(numShards) { groupedStream =>
                      val shard = groupedStream.key
                      println(s"Starting ingest on shard $shard...")
                      val shardStream = groupedStream.zipWithIndex.flatMap { case ((_, bytes), idx) =>
                        // println(s"   XXX: -->  Got ${bytes.map(_.size).sum} bytes in shard $shard")
                        val data = bytes.map { array => SomeData(RecordContainer(array), idx) }
                        Observable.fromIterable(data)
                      }
                      Task.fromFuture(cluster.memStore.ingestStream(dataset.ref, shard, shardStream, global))
                    }.countL.runAsync
  Await.result(producingFut, 200.seconds)
  Thread sleep 2000
  val store = cluster.memStore.asInstanceOf[TimeSeriesMemStore]
  store.refreshIndexForTesting(dataset.ref) // commit lucene index
  println(s"Ingestion ended")

  // Stuff for directly executing queries ourselves
  val engine = new SingleClusterPlanner(dataset.ref, Schemas(dataset.schema), shardMapper, 0,
    queryConfig)

  val numQueries = 100       // Please make sure this number matches the OperationsPerInvocation below
  val queryIntervalSec = samplesDuration.toSeconds  // # minutes between start and stop
  val queryStep = 15        // # of seconds between each query sample "step"

  val dummyPromQlQueryParams = PromQlQueryParams("dummy", 0, 1000, 100) // not real
  val qContext = QueryContext(dummyPromQlQueryParams,
    plannerParams = PlannerParams(queryTimeoutMillis = Int.MaxValue, shardOverrides = Some(Seq(0))))
  private def toExecPlan(query: String): ExecPlan = {
    val queryStartTime = ingestionStartTime + 7.minutes.toMillis  // 7 minutes from start until 60 minutes from start
    val qParams = TimeStepParams(queryStartTime/1000, queryStep, queryStartTime/1000 + queryIntervalSec)
    val execPlan = engine.materialize(Parser.queryRangeToLogicalPlan(query, qParams), qContext)
    var child = execPlan
    while (child.children.size > 0) child = child.children(0)
    child
  }

  @TearDown
  def shutdownFiloActors(): Unit = {
    cluster.shutdown()
  }

  val scanSumOfRate = toExecPlan("""sum(rate(heap_usage{_ws_="demo",_ns_="App-2"}[5m]))""")
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @OperationsPerInvocation(100)
  def scanSumOfRateBenchmark(): Unit = {
    (0 until numQueries).foreach { _ =>
      val querySession = QuerySession(qContext, queryConfig)
      Await.result(scanSumOfRate.execute(store, querySession).runAsync, 60.seconds)
    }
  }

  val scanSumSumOverTime = toExecPlan("""sum(sum_over_time(heap_usage{_ws_="demo",_ns_="App-2"}[5m]))""")
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @OperationsPerInvocation(100)
  def scanSumOfSumOverTimeBenchmark(): Unit = {
    (0 until numQueries).foreach { _ =>
      val querySession = QuerySession(qContext, queryConfig)
      Await.result(scanSumSumOverTime.execute(store, querySession).runAsync, 60.seconds)
    }
  }

}