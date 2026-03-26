package filodb.jmh

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._

import ch.qos.logback.classic.{Level, Logger}
import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler
import monix.reactive.Observable
import org.openjdk.jmh.annotations._

import filodb.coordinator.client.QueryCommands.StaticSpreadProvider
import filodb.core.SpreadChange
import filodb.core.query.{PerQueryLimits, PlannerParams, QueryContext, QuerySession}
import filodb.memory.format.vectors.SimdNativeMethods
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser

/**
 * JMH benchmark comparing SIMD-on vs SIMD-off for sum_over_time and count_over_time queries.
 *
 * These are pure columnar operations — each step calls doubleReader.sum() or doubleReader.count()
 * directly on the chunk, without rate calculation or cross-series aggregation overhead.
 *
 * Two parameters:
 *   - `window`: the over_time window (5m, 15m, 30m, 1h, 6h, 1d, 2d) — controls elements per
 *     doubleReader.sum()/count() call.
 *   - `queryRange`: overall query time range (1h, 6h, 1d).
 *
 * Shares the same 7-day delta counter data from SumRateDeltaCounterBenchmark companion object.
 *
 * Run all:
 *   sbt -Drust.optimize=true "jmh/jmh:run -i 5 -wi 3 -f 0 \
 *     filodb.jmh.SumCountOverTimeBenchmark"
 *
 * Run specific:
 *   sbt -Drust.optimize=true "jmh/jmh:run -i 3 -wi 2 -f 0 \
 *     -p window=1h -p queryRange=6h filodb.jmh.SumCountOverTimeBenchmark"
 */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(0)
class SumCountOverTimeBenchmark extends StrictLogging {
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

  @Param(Array("5m", "15m", "30m", "1h", "6h", "1d", "2d"))
  var window: String = _

  @Param(Array("1h", "6h", "1d"))
  var queryRange: String = _

  // Reuse the same cluster singleton as SumRateDeltaCounterBenchmark
  private val clusterState = SumRateDeltaCounterBenchmark.getOrCreateCluster(
    numShards, numSeries, totalSamples, startTime, spread, publishIntervalSec)

  val queryConfig = clusterState.queryConfig
  val querySched: Scheduler = Scheduler.singleThread(s"benchmark-query-sot")

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
    case "1d"  => 86400L
    case "2d"  => 172800L
    case _     => 300L
  }

  var sumExecPlan: filodb.query.exec.ExecPlan = _
  var countExecPlan: filodb.query.exec.ExecPlan = _

  @Setup(org.openjdk.jmh.annotations.Level.Trial)
  def setupExecPlans(): Unit = {
    val rangeSec = toSeconds(queryRange)
    val queryEndTime = dataEndTime
    val queryStartTime = queryEndTime - (rangeSec * 1000)
    val stepSec = 60
    val samplesPerWindow = toSeconds(window) / publishIntervalSec

    val sumQuery = s"""sum(sum_over_time(heap_usage_delta0{_ws_="demo",_ns_="App-0"}[$window]))"""
    val countQuery = s"""sum(count_over_time(heap_usage_delta0{_ws_="demo",_ns_="App-0"}[$window]))"""

    val qParams = TimeStepParams(queryStartTime / 1000, stepSec, queryEndTime / 1000)

    val sumLp = Parser.queryRangeToLogicalPlan(sumQuery, qParams)
    sumExecPlan = clusterState.engine.materialize(sumLp, qContext).children.head

    val countLp = Parser.queryRangeToLogicalPlan(countQuery, qParams)
    countExecPlan = clusterState.engine.materialize(countLp, qContext).children.head

    val numSteps = rangeSec / stepSec
    //scalastyle:off regex
    println(s"[BENCH] sum query: $sumQuery")
    println(s"[BENCH] count query: $countQuery")
    println(s"[BENCH] window=$window (${samplesPerWindow} samples/window), " +
      s"queryRange=$queryRange, step=${stepSec}s, numSteps=$numSteps, publishInterval=${publishIntervalSec}s")
    //scalastyle:on regex
  }

  private def runQueries(plan: filodb.query.exec.ExecPlan): Long = {
    val f = Observable.fromIterable(0 until numQueries).mapEval { _ =>
      val querySession = QuerySession(qContext, queryConfig)
      plan.execute(clusterState.memStore, querySession)(querySched)
    }.executeOn(querySched)
     .countL.runToFuture(monix.execution.Scheduler.Implicits.global)
    Await.result(f, 10.minutes)
  }

  // ── sum_over_time benchmarks ──────────────────────────────────────────

  @Benchmark
  @OperationsPerInvocation(10)
  def sumOverTimeSimdOff(): Long = {
    SimdNativeMethods.enabled = false
    runQueries(sumExecPlan)
  }

  @Benchmark
  @OperationsPerInvocation(10)
  def sumOverTimeSimdOn(): Long = {
    SimdNativeMethods.enabled = true
    runQueries(sumExecPlan)
  }

  // ── count_over_time benchmarks ────────────────────────────────────────

  @Benchmark
  @OperationsPerInvocation(10)
  def countOverTimeSimdOff(): Long = {
    SimdNativeMethods.enabled = false
    runQueries(countExecPlan)
  }

  @Benchmark
  @OperationsPerInvocation(10)
  def countOverTimeSimdOn(): Long = {
    SimdNativeMethods.enabled = true
    runQueries(countExecPlan)
  }
}
