package filodb.gatling

import scala.concurrent.duration.DurationInt

import ch.qos.logback.classic.{Level, LoggerContext}
import io.gatling.core.Predef._
import io.gatling.http.Predef._
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

  def runLoadTest(): Unit = {
    //val baseUrl = "http://localhost:8080"
    val baseUrl = "http://localhost:9900"
    val numApps = 1
    val testDuration = 3.minutes
    // Before each run, change this to the right timestamp relevant to data stored in server
    val startSecs = 1769277850L
    // Log all HTTP requests
    val context: LoggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    context.getLogger("io.gatling").setLevel(Level.INFO)

    val jobNameFeeder = for {
      x <- 0 until numApps
    } yield {
      Seq(Map("query" -> promQL.replace("$appNum", x.toString)))
    }
    val jobFeeder = Iterator.continually(jobNameFeeder.flatten).flatten
    val endSecs = startSecs + 3 * 60 * 60 // 3hrs

    //val httpConfig = http.baseUrl(s"$baseUrl/promql/promperf/")
    val httpConfig = http.baseUrl(s"$baseUrl/")
    val queryRangeScenario =
      scenario(s"QueryRange-$queryName")
        .feed(jobFeeder)
        .exec(
          http("query_range")
            .get("api/v1/query_range")
            .queryParam("query", f"$${query}")
            .queryParam("start", startSecs)
            .queryParam("end", endSecs)
            .queryParam("step", "60")
            .check(status is 200,
              jsonPath("$.status") is "success",
              jsonPath("$.data.result[*]").count.gte(1)
            )
        )


    setUp(
      queryRangeScenario.inject(
        constantUsersPerSec(10).during(30.seconds),
        constantUsersPerSec(numUsers) during(testDuration)
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
  override val numUsers = 20
  runLoadTest()
}
class GetAllTimeseriesQueryRangeSimulation extends QueryRangeSimulation {
  override val queryName = "GetAllTimeseriesQuery"
  override val promQL = """heap_usage0{_ns_="App-$appNum",_ws_="demo"}"""
  override val numUsers = 10
  runLoadTest()
}