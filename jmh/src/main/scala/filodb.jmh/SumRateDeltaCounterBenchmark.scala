package filodb.jmh

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.ActorSystem
import ch.qos.logback.classic.{Level, Logger}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.openjdk.jmh.annotations._

import filodb.coordinator.client.QueryCommands.StaticSpreadProvider
import filodb.coordinator.queryplanner.SingleClusterPlanner
import filodb.core.{GlobalConfig, SpreadChange}
import filodb.core.binaryrecord2.RecordContainer
import filodb.core.memstore.{SomeData, TimeSeriesMemStore}
import filodb.core.metadata.{Dataset, Schemas}
import filodb.core.query.{PerQueryLimits, PlannerParams, QueryConfig, QueryContext, QuerySession}
import filodb.core.store.StoreConfig
import filodb.gateway.GatewayServer
import filodb.memory.format.vectors.SimdNativeMethods
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.timeseries.TestTimeseriesProducer

/**
 * JMH benchmark comparing SIMD-on vs SIMD-off for sum(rate(...)) queries on delta counters.
 *
 * Two parameters:
 *   - `rateWindow`: the rate window size (5m, 15m, 30m, 1h, 6h) — controls how many doubles
 *     each doubleReader.sum() call processes (30, 90, 180, 360, 2160 at 10s interval).
 *     SIMD benefit scales with this.
 *   - `queryRange`: the overall query time range (1h, 6h, 1d) — controls how many steps
 *     (and thus how many sum() calls) per query.
 *
 * Ingests 7 days of delta counter data (10s interval, 100 series) once.
 *
 * Run all combinations:
 *   sbt -Drust.optimize=true "jmh/jmh:run -i 5 -wi 3 -f 0 \
 *     filodb.jmh.SumRateDeltaCounterBenchmark"
 *
 * Run specific combination:
 *   sbt -Drust.optimize=true "jmh/jmh:run -i 5 -wi 3 -f 0 \
 *     -p rateWindow=1h -p queryRange=6h filodb.jmh.SumRateDeltaCounterBenchmark"
 */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(0)
class SumRateDeltaCounterBenchmark extends StrictLogging {
  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.WARN)

  val numShards = 8
  val numSeries = 100
  val numQueries = 10
  val spread = 3
  val publishIntervalSec: Int = Integer.getInteger("bench.publishIntervalSec", 10)
  val daysOfData: Int = Integer.getInteger("bench.daysOfData", 7)
  val samplesPerSeries: Int = daysOfData * 24 * 3600 / publishIntervalSec
  val totalSamples: Int = samplesPerSeries * numSeries

  val startTime: Long = System.currentTimeMillis - (daysOfData.toLong * 24 * 3600 * 1000)
  val dataEndTime: Long = startTime + (daysOfData.toLong * 24 * 3600 * 1000)

  // Rate window — controls elements per doubleReader.sum() call
  @Param(Array("5m", "15m", "30m", "1h", "6h", "1d", "2d", "7d"))
  var rateWindow: String = _

  // Query time range — controls number of steps (sum() calls) per query
  @Param(Array("1h", "6h", "1d"))
  var queryRange: String = _

  private val clusterState = SumRateDeltaCounterBenchmark.getOrCreateCluster(
    numShards, numSeries, totalSamples, startTime, spread, publishIntervalSec)

  val queryConfig: QueryConfig = clusterState.queryConfig
  val querySched: Scheduler = Scheduler.singleThread(s"benchmark-query")

  val qContext: QueryContext = QueryContext(plannerParams =
    new PlannerParams(spreadOverride = Some(StaticSpreadProvider(SpreadChange(0, spread))),
      enforcedLimits = PerQueryLimits(execPlanSamples = 1000000),
      queryTimeoutMillis = 10.minutes.toMillis.toInt))

  private def toSeconds(s: String): Long = s match {
    case "5m"  => 300L
    case "15m" => 900L
    case "30m" => 1800L
    case "1h"  => 3600L
    case "6h"  => 21600L
    case "12h" => 43200L
    case "1d"  => 86400L
    case "2d"  => 172800L
    case "7d"  => 604800L
    case _     => 300L
  }

  var execPlan: filodb.query.exec.ExecPlan = _

  @Setup(org.openjdk.jmh.annotations.Level.Trial)
  def setupExecPlan(): Unit = {
    val rangeSec = toSeconds(queryRange)
    val queryEndTime = dataEndTime
    val queryStartTime = queryEndTime - (rangeSec * 1000)
    val stepSec = 60  // fixed 60s step
    val samplesPerWindow = toSeconds(rateWindow) / publishIntervalSec
    val query = s"""sum(rate(heap_usage_delta0{_ws_="demo",_ns_="App-0"}[$rateWindow]))"""
    val qParams = TimeStepParams(queryStartTime / 1000, stepSec, queryEndTime / 1000)
    val lp = Parser.queryRangeToLogicalPlan(query, qParams)
    execPlan = clusterState.engine.materialize(lp, qContext).children.head
    val numSteps = rangeSec / stepSec
    //scalastyle:off regex
    println(s"[BENCH] query: $query")
    println(s"[BENCH] rateWindow=$rateWindow (${samplesPerWindow} samples/window), " +
      s"queryRange=$queryRange, step=${stepSec}s, numSteps=$numSteps, publishInterval=${publishIntervalSec}s")
    //scalastyle:on regex
  }

  private def runQueries(): Long = {
    val f = Observable.fromIterable(0 until numQueries).mapEval { _ =>
      val querySession = QuerySession(qContext, queryConfig)
      execPlan.execute(clusterState.memStore, querySession)(querySched)
    }.executeOn(querySched)
     .countL.runToFuture(monix.execution.Scheduler.Implicits.global)
    Await.result(f, 10.minutes)
  }

  // ── Benchmarks ────────────────────────────────────────────────────────

  @Benchmark
  @OperationsPerInvocation(10)
  def simdOff(): Long = {
    SimdNativeMethods.enabled = false
    runQueries()
  }

  @Benchmark
  @OperationsPerInvocation(10)
  def simdOn(): Long = {
    SimdNativeMethods.enabled = true
    runQueries()
  }
}

//scalastyle:off regex
//scalastyle:off method.length
object SumRateDeltaCounterBenchmark extends StrictLogging {

  case class ClusterState(
    cluster: filodb.coordinator.FilodbCluster,
    memStore: TimeSeriesMemStore,
    engine: SingleClusterPlanner,
    queryConfig: QueryConfig
  )

  @volatile private var instance: ClusterState = _

  def getOrCreateCluster(numShards: Int, numSeries: Int, totalSamples: Int,
                         startTime: Long, spread: Int, publishIntervalSec: Int): ClusterState = {
    if (instance != null) return instance

    synchronized {
      if (instance != null) return instance

      import filodb.coordinator._
      import client.Client.actorAsk
      import NodeClusterActor._

      val system = ActorSystem("test", ConfigFactory.load("filodb-defaults.conf")
        .withValue("filodb.memstore.ingestion-buffer-mem-size", ConfigValueFactory.fromAnyRef("1GB"))
        .withValue("akka.cluster.jmx.multi-mbeans-in-same-jvm", ConfigValueFactory.fromAnyRef("on")))

      val cluster = FilodbCluster(system)
      cluster.join()

      val coordinator = cluster.coordinatorActor
      val clusterActor = cluster.clusterSingleton(ClusterRole.Server, None)

      val dataset = Dataset("prometheus", Schemas.deltaCounter)
      val shardMapper = new ShardMapper(numShards)
      (0 until numShards).foreach { s =>
        shardMapper.updateFromEvent(IngestionStarted(dataset.ref, s, coordinator))
      }

      Await.result(cluster.metaStore.initialize(), 3.seconds)

      val storeConf = StoreConfig(ConfigFactory.parseString("""
                      | flush-interval = 1h
                      | shard-mem-size = 3GB
                      | groups-per-shard = 4
                      | demand-paging-enabled = false
                      """.stripMargin))
      val command = SetupDataset(dataset, DatasetResourceSpec(numShards, 1), noOpSource, storeConf,
        overrideSchema = false)
      actorAsk(clusterActor, command) { case DatasetVerified => println("dataset setup") }
      coordinator ! command

      val queryConfig = QueryConfig(cluster.settings.allConfig.getConfig("filodb.query"))

      import monix.execution.Scheduler.Implicits.global

      Thread sleep 2000

      val (shardQueues, containerStream) = GatewayServer.shardingPipeline(
        GlobalConfig.systemConfig, numShards, dataset)
      val producingFut = scala.concurrent.Future {
        val data = TestTimeseriesProducer.timeSeriesData(startTime, numSeries, numMetricNames = 1,
          publishIntervalSec = publishIntervalSec, schema = Schemas.deltaCounter)
        var count = 0
        data.take(totalSamples)
          .foreach { rec =>
            val shard = shardMapper.ingestionShard(rec.shardKeyHash, rec.partitionKeyHash, spread)
            while (!shardQueues(shard).offer(rec)) { Thread sleep 50 }
            count += 1
            if (count % 1000000 == 0) println(s"Ingested $count / $totalSamples samples")
          }
      }
      containerStream.groupBy(_._1)
        .mapParallelUnordered(numShards) { groupedStream =>
          val shard = groupedStream.key
          val shardStream = groupedStream.zipWithIndex.flatMap { case ((_, bytes), idx) =>
            val data = bytes.map { array => SomeData(RecordContainer(array), idx) }
            Observable.fromIterable(data)
          }
          Task.fromFuture(cluster.memStore.startIngestion(dataset.ref, shard, shardStream, global))
        }.countL.runToFuture
      Await.result(producingFut, 30.minutes)
      Thread sleep 5000
      cluster.memStore.asInstanceOf[TimeSeriesMemStore].refreshIndexForTesting(dataset.ref)
      println(s"Ingestion complete: $numSeries series x ${totalSamples / numSeries} samples/series (7 days)")

      val engine = new SingleClusterPlanner(dataset, Schemas(dataset.schema), shardMapper, 0,
        queryConfig, "raw")

      instance = ClusterState(cluster, cluster.memStore.asInstanceOf[TimeSeriesMemStore], engine, queryConfig)

      Runtime.getRuntime.addShutdownHook(new Thread(() => {
        cluster.shutdown()
      }))

      instance
    }
  }
}
//scalastyle:on method.length
//scalastyle:on regex
