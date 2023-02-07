package filodb.gatling

import scala.concurrent.duration.DurationInt

import ch.qos.logback.classic.{Level, LoggerContext}
import io.gatling.core.Predef._
import io.gatling.http.Predef._
import org.slf4j.LoggerFactory


/**
 * TimeRangeQuerySimulation queries mosaic-queryservice with
 * random application name and kpi with configured user count asynchronously.
 *
 */
class QueryRangeSimulation extends Simulation {

  object Configuration {
    val baseUrl = "http://localhost:8080"
    val numApps = 1
    val numUsers = 20
    val testDuration = 3.minutes
    val startSecs = 1670818224L // Before each run, change this to the right timestamp relevant to data stored in server

    // Uncomment one query from here:
    val queryName = "BinaryJoin"
    val query = """count(heap_usage0{_ns_="App-$appNum",_ws_="demo"} + heap_usage1{_ns_="App-$appNum",_ws_="demo"})"""

//    val queryName = "SumOfSumOverTime"
//    val query = """sum(sum_over_time(heap_usage0{_ns_="App-$appNum",_ws_="demo"}[5m]))"""

  }

  import Configuration._

  // Log all HTTP requests
  val context: LoggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
  context.getLogger("io.gatling").setLevel(Level.INFO)

  val jobNameFeeder = for {
    x <- 0 until numApps
  } yield {
      Seq(Map("query" -> query.replace("$appNum", x.toString)))
  }
  val oneRepeat = jobNameFeeder.flatten
  val jobFeeder = Iterator.continually(oneRepeat).flatten
  val endSecs = startSecs + 3 * 60 * 60 // 3hrs

  val httpConfig = http.baseUrl(s"$baseUrl/promql/promperf/")
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
    queryRangeScenario.inject(constantUsersPerSec(Configuration.numUsers) during(Configuration.testDuration))
  ).protocols(httpConfig.shareConnections)

}
