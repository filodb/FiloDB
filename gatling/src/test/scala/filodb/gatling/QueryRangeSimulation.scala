package filodb.gatling

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import ch.qos.logback.classic.{Level, LoggerContext}
import io.gatling.core.Predef._
import io.gatling.http.Predef._
import io.gatling.http.check.HttpCheck
import org.slf4j.LoggerFactory


/**
 * PromQL Range queries simulation that runs different types of queries against a FiloDB/Prometheus server.
 */
trait QueryRangeSimulation extends Simulation {


  /**
   * Number of concurrent users per second to simulate
   */
  def numUsers: Int

  /**
   * The name of the query being run
   */
  def queryName: String

  /**
   * The PromQL query to run. When `_ns_="App-$appNum"` is used, the variable will be replaced with
   * actual application number from 0 to numApps-1.
   */
  def promQL: String

  /**
   * Warmup requests are marked silent, so they warm up the JVMs (JIT, Kryo pools, Arrow allocators,
   * connection pools) without being included in the reported latencies.
   *
   * Warmup must reach the measured rate: code paths and pools that are only exercised under high
   * concurrency stay cold at a low warmup rate, and the step up to full load then builds a queue that
   * takes tens of seconds to drain. So the warmup ramps from 1 user/sec to numUsers and then holds at
   * numUsers before the measured phase starts.
   */
  def warmupRampDuration: FiniteDuration = 30.seconds

  /**
   * Duration for which warmup holds at the full measured rate (numUsers per second) after the ramp.
   */
  def warmupHoldDuration: FiniteDuration = 60.seconds

  /**
   * Whether each response must contain at least one series. Override to false for queries that are
   * expected to return an empty result.
   */
  def expectNonEmptyResult: Boolean = true

  def runLoadTest(): Unit = {
    val baseUrl = "http://localhost:8080/promql/promperf/"
    //val baseUrl = "http://localhost:9900"
    val numApps = 1
    val testDuration = 3.minutes
    // Before each run, change this to the right timestamp relevant to data stored in server
    val startSecs = 1794609550L
    // Log all HTTP requests
    val context: LoggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    context.getLogger("io.gatling").setLevel(Level.INFO)

    val jobNameFeeder = for {
      x <- 0 until numApps
    } yield {
      Seq(Map("query" -> promQL.replace("$appNum", x.toString)))
    }
    print(s"jobNameFeeder: $jobNameFeeder")
    val jobFeeder = Iterator.continually(jobNameFeeder.flatten).flatten
    val endSecs = startSecs + 3 * 60 * 60 // 3hrs

    //val httpConfig = http.baseUrl(s"$baseUrl/promql/promperf/")
    val httpConfig = http.baseUrl(s"$baseUrl/")
    val baseChecks: Seq[HttpCheck] = Seq(status is 200, jsonPath("$.status") is "success")
    val responseChecks: Seq[HttpCheck] =
      if (expectNonEmptyResult) baseChecks :+ (jsonPath("$.data.result[*]").count.gte(1): HttpCheck)
      else baseChecks
    def queryRangeRequest(requestName: String) =
      http(requestName)
        .get("api/v1/query_range")
        .queryParam("query", f"#{query}")
        .queryParam("start", startSecs)
        .queryParam("end", endSecs)
        .queryParam("step", "60")
        .check(responseChecks: _*)

    // Silent requests are excluded from the stats and reports
    val warmupScenario =
      scenario(s"Warmup-$queryName")
        .feed(jobFeeder)
        .exec(queryRangeRequest("warmup_query_range").silent)

    val queryRangeScenario =
      scenario(s"QueryRange-$queryName")
        .feed(jobFeeder)
        .exec(queryRangeRequest("query_range"))

    // The measured scenario starts only after all warmup users have completed
    setUp(
      warmupScenario.inject(
        rampUsersPerSec(1).to(numUsers).during(warmupRampDuration),
        constantUsersPerSec(numUsers).during(warmupHoldDuration)
      ).andThen(
        queryRangeScenario.inject(
          constantUsersPerSec(numUsers).during(testDuration)
        )
      )
    ).protocols(httpConfig.shareConnections)
  }
}

class BinaryJoinQueryRangeSimulation extends QueryRangeSimulation {
  override val queryName = "BinaryJoin"
  override val promQL = """count(heap_usage0{_ns_="App-$appNum",_ws_="demo"} + heap_usage1{_ns_="App-$appNum",_ws_="demo"})"""
  override val numUsers = 10
  runLoadTest()
}

class SumOfSumOverTimeQueryRangeSimulation extends QueryRangeSimulation {
  override val queryName = "SumOfSumOverTime"
  override val promQL = """sum(sum_over_time(heap_usage0{_ns_="App-$appNum",_ws_="demo"}[5m]))"""
  override val numUsers = 100
  runLoadTest()
}

class ZeroResultQueryRangeSimulation extends QueryRangeSimulation {
  override val queryName = "ZeroResult"
  override val promQL = """heap_usage0{_ns_="App-$appNum",_ws_="demo"} < 0"""
  override val numUsers = 30
  override val expectNonEmptyResult = false
  runLoadTest()
}

class GetAllTimeseriesQueryRangeSimulation extends QueryRangeSimulation {
  override val queryName = "GetAllTimeseriesQuery"
  override val promQL = """heap_usage0{_ns_="App-$appNum",_ws_="demo"}"""
  override val numUsers = 10
  runLoadTest()
}