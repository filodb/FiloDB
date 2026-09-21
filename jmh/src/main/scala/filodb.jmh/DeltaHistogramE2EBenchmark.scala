package filodb.jmh

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._

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
import filodb.core.query._
import filodb.core.store.StoreConfig
import filodb.gateway.GatewayServer
import filodb.memory.format.vectors.SimdNativeMethods
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.timeseries.TestTimeseriesProducer

//scalastyle:off regex
//scalastyle:off method.length
/**
 * End-to-end benchmark for delta histogram rate queries.
 * Measures: sum(rate(delta_hist[window]))
 *
 * Uses GatewayServer sharding pipeline + TestTimeseriesProducer.genHistogramData
 * for realistic ingestion (proper sharding, multi-shard, stream ingestion).
 *
 * Setup: 100 delta-histogram series, 7 days at 10s interval, 20 buckets.
 * Parameterized by rateWindow: 5m, 15m, 1h.
 *
 * Run all:
 *   sbt "jmh/jmh:run -i 5 -wi 3 -f 0 filodb.jmh.DeltaHistogramE2EBenchmark"
 * Run specific:
 *   sbt "jmh/jmh:run -i 5 -wi 3 -f 0 -p rateWindow=5m filodb.jmh.DeltaHistogramE2EBenchmark"
 */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(0)
class DeltaHistogramE2EBenchmark extends StrictLogging {
  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger]
    .setLevel(Level.WARN)

  val numShards = 8
  val numSeries = 100
  val numQueries = 10
  val spread = 3
  val publishIntervalSec = 10
  val hoursOfData = 2
  val samplesPerSeries: Int = hoursOfData * 3600 / publishIntervalSec
  val totalSamples: Int = samplesPerSeries * numSeries

  val startTime: Long =
    System.currentTimeMillis - (hoursOfData.toLong * 3600 * 1000)
  val dataEndTime: Long =
    startTime + (hoursOfData.toLong * 3600 * 1000)

  @Param(Array("5m", "15m", "1h"))
  var rateWindow: String = _

  private val clusterState =
    DeltaHistogramE2EBenchmark.getOrCreateCluster(
      numShards, numSeries, totalSamples,
      startTime, spread, publishIntervalSec)

  val queryConfig: QueryConfig = clusterState.queryConfig
  val querySched: Scheduler = Scheduler.singleThread(s"bench-query")

  val qContext: QueryContext = QueryContext(plannerParams =
    new PlannerParams(
      spreadOverride =
        Some(StaticSpreadProvider(SpreadChange(0, spread))),
      enforcedLimits =
        PerQueryLimits(execPlanSamples = 1000000),
      queryTimeoutMillis = 10.minutes.toMillis.toInt))

  private def toSeconds(s: String): Long = s match {
    case "5m"  => 300L
    case "15m" => 900L
    case "1h"  => 3600L
    case _     => 300L
  }

  var execPlan: filodb.query.exec.ExecPlan = _

  @Setup(org.openjdk.jmh.annotations.Level.Trial)
  def setupExecPlan(): Unit = {
    val rangeSec = toSeconds(rateWindow)
    val queryEndTime = dataEndTime
    val queryStartTime = queryEndTime - (3600 * 1000) // 1h query range
    val stepSec = 60
    val samplesPerWindow = rangeSec / publishIntervalSec
    // Metric name for delta histograms from genHistogramData
    val query =
      s"""sum(rate(http_request_latency_delta""" +
      s"""{_ws_="demo",_ns_="App-0"}[$rateWindow]))"""
    val qParams = TimeStepParams(
      queryStartTime / 1000, stepSec, queryEndTime / 1000)
    val lp = Parser.queryRangeToLogicalPlan(query, qParams)
    execPlan =
      clusterState.engine.materialize(lp, qContext).children.head
    val numSteps = 3600 / stepSec
    println(s"[BENCH] query: $query")
    println(s"[BENCH] rateWindow=$rateWindow " +
      s"($samplesPerWindow samples/window), " +
      s"queryRange=1h, step=${stepSec}s, numSteps=$numSteps")

    // Validate: run query with simd on and off, compare results
    validateResults()
  }

  private def executeOne(simd: Boolean)
    : filodb.query.QueryResponse = {
    SimdNativeMethods.deltaHistogramSumEnabled = simd
    val qs = QuerySession(qContext, queryConfig)
    val f = execPlan.execute(clusterState.memStore, qs)(querySched)
    Await.result(
      f.runToFuture(querySched), 60.seconds)
  }

  private def validateResults(): Unit = {
    val jvmResult = executeOne(simd = false)
    val nativeResult = executeOne(simd = true)

    (jvmResult, nativeResult) match {
      case (jvm: filodb.query.QueryResult,
            nat: filodb.query.QueryResult) =>
        require(jvm.result.size == nat.result.size,
          s"Result size mismatch: jvm=${jvm.result.size}" +
          s" native=${nat.result.size}")
        require(jvm.result.size > 0,
          "Query returned 0 results — check metric name/filters")
        jvm.result.zip(nat.result).zipWithIndex
          .foreach { case ((jvmRv, natRv), i) =>
            val jvmRows = jvmRv.rows().map(_.getDouble(1)).toList
            val natRows = natRv.rows().map(_.getDouble(1)).toList
            require(jvmRows.size == natRows.size,
              s"RV[$i] row count mismatch: " +
              s"jvm=${jvmRows.size} native=${natRows.size}")
            jvmRows.zip(natRows).zipWithIndex
              .foreach { case ((j, n), row) =>
                require(j == n || (j.isNaN && n.isNaN),
                  s"RV[$i] row[$row] mismatch: jvm=$j native=$n")
              }
          }
        println(s"[VALIDATE] OK: ${jvm.result.size} range vectors," +
          s" values match between JVM and native paths")

      case (jvmErr: filodb.query.QueryError, _) =>
        throw new RuntimeException(
          s"JVM query failed: ${jvmErr.t.getMessage}")
      case (_, natErr: filodb.query.QueryError) =>
        throw new RuntimeException(
          s"Native query failed: ${natErr.t.getMessage}")
      case _ =>
        throw new RuntimeException("Unexpected query response")
    }
  }

  private def runQueries(): Long = {
    val f = Observable.fromIterable(0 until numQueries)
      .mapEval { _ =>
        val qs = QuerySession(qContext, queryConfig)
        execPlan.execute(clusterState.memStore, qs)(querySched)
      }.executeOn(querySched)
      .countL
      .runToFuture(monix.execution.Scheduler.Implicits.global)
    Await.result(f, 10.minutes)
  }

  @Benchmark
  @OperationsPerInvocation(10)
  def simdOff(): Long = {
    SimdNativeMethods.deltaHistogramSumEnabled = false
    runQueries()
  }

  @Benchmark
  @OperationsPerInvocation(10)
  def simdOn(): Long = {
    SimdNativeMethods.deltaHistogramSumEnabled = true
    runQueries()
  }
}

object DeltaHistogramE2EBenchmark extends StrictLogging {

  case class ClusterState(
    cluster: filodb.coordinator.FilodbCluster,
    memStore: TimeSeriesMemStore,
    engine: SingleClusterPlanner,
    queryConfig: QueryConfig
  )

  @volatile private var instance: ClusterState = _

  def getOrCreateCluster(
    numShards: Int, numSeries: Int, totalSamples: Int,
    startTime: Long, spread: Int, publishIntervalSec: Int
  ): ClusterState = {
    if (instance != null) return instance

    synchronized {
      if (instance != null) return instance

      import filodb.coordinator._
      import client.Client.actorAsk
      import NodeClusterActor._

      val system = akka.actor.ActorSystem("test",
        ConfigFactory.load("filodb-defaults.conf")
          .withValue(
            "filodb.memstore.ingestion-buffer-mem-size",
            ConfigValueFactory.fromAnyRef("1GB"))
          .withValue(
            "akka.cluster.jmx.multi-mbeans-in-same-jvm",
            ConfigValueFactory.fromAnyRef("on")))

      val cluster = FilodbCluster(system)
      cluster.join()

      val coordinator = cluster.coordinatorActor
      val clusterActor =
        cluster.clusterSingleton(ClusterRole.Server, None)

      // delta-histogram schema: counter=false, histogram column
      // → SUBTYPE_H_SIMPLE → DeltaHistogramReader
      val dataset =
        Dataset("prometheus", Schemas.deltaHistogram)
      val shardMapper = new ShardMapper(numShards)
      (0 until numShards).foreach { s =>
        shardMapper.updateFromEvent(
          IngestionStarted(dataset.ref, s, coordinator))
      }

      Await.result(cluster.metaStore.initialize(), 3.seconds)

      val storeConf = StoreConfig(ConfigFactory.parseString(
        """
        | flush-interval = 1h
        | shard-mem-size = 3GB
        | groups-per-shard = 4
        | demand-paging-enabled = false
        """.stripMargin))
      val command = SetupDataset(
        dataset, DatasetResourceSpec(numShards, 1),
        noOpSource, storeConf, overrideSchema = false)
      actorAsk(clusterActor, command) {
        case DatasetVerified => println("dataset setup")
      }
      coordinator ! command

      val queryConfig = QueryConfig(
        cluster.settings.allConfig.getConfig("filodb.query"))

      import monix.execution.Scheduler.Implicits.global

      Thread sleep 2000

      // Ingest delta histogram data via GatewayServer pipeline
      val (shardQueues, containerStream) =
        GatewayServer.shardingPipeline(
          GlobalConfig.systemConfig, numShards, dataset)
      val producingFut = scala.concurrent.Future {
        val data = TestTimeseriesProducer.genHistogramData(
          startTime, numSeries,
          histSchema = Schemas.deltaHistogram,
          numBuckets = 20)
        var count = 0
        data.take(totalSamples)
          .foreach { rec =>
            val shard = shardMapper.ingestionShard(
              rec.shardKeyHash, rec.partitionKeyHash, spread)
            while (!shardQueues(shard).offer(rec)) {
              Thread sleep 50
            }
            count += 1
            if (count % 500000 == 0)
              println(s"Ingested $count / $totalSamples")
          }
      }
      containerStream.groupBy(_._1)
        .mapParallelUnordered(numShards) { groupedStream =>
          val shard = groupedStream.key
          val shardStream = groupedStream.zipWithIndex
            .flatMap { case ((_, bytes), idx) =>
              val data = bytes.map { array =>
                SomeData(RecordContainer(array), idx)
              }
              Observable.fromIterable(data)
            }
          Task.fromFuture(cluster.memStore.startIngestion(
            dataset.ref, shard, shardStream, global))
        }.countL.runToFuture
      Await.result(producingFut, 30.minutes)
      Thread sleep 5000
      cluster.memStore.asInstanceOf[TimeSeriesMemStore]
        .refreshIndexForTesting(dataset.ref)
      println(s"Ingestion complete: $numSeries series x " +
        s"${totalSamples / numSeries} samples/series (2 hours)")

      val engine = new SingleClusterPlanner(
        dataset, Schemas(dataset.schema),
        shardMapper, 0, queryConfig, "raw")

      instance = ClusterState(
        cluster,
        cluster.memStore.asInstanceOf[TimeSeriesMemStore],
        engine, queryConfig)

      Runtime.getRuntime.addShutdownHook(new Thread(() => {
        cluster.shutdown()
      }))

      instance
    }
  }
}
//scalastyle:on method.length
//scalastyle:on regex
