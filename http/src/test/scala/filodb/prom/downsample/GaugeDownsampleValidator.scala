package filodb.prom.downsample

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import akka.actor.{ActorSystem, CoordinatedShutdown}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, Uri}
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport

import filodb.http.PromCirceSupport
import filodb.prometheus.query.PrometheusModel.SuccessResponse

/**
  * Use this tool to validate raw data against downsampled data for gauges
  *
  */
object GaugeDownsampleValidator extends App with StrictLogging {

  import FailFastCirceSupport._
  // DO NOT REMOVE PromCirceSupport import below assuming it is unused - Intellij removes it in auto-imports :( .
  // Needed to override Sampl case class Encoder.
  import PromCirceSupport._
  import io.circe.generic.auto._

  val config = ConfigFactory.load()
  val rawPromql = config.getString("raw-data-promql")
  val filodbHttpEndpoint = config.getString("query-endpoint")
  val flushIntervalHours = config.getDuration("flush-interval")
  val queryRange = config.getDuration("query-range")

  require((rawPromql.endsWith(""",__col__="value"}[@@@@s]""")),
    """Raw Data PromQL should end with ,__col__="value"}[@@@@s]""")

  val rawToDsPromQl = Map (
    s"""min_over_time($rawPromql)""" ->
      s"""min_over_time(${rawPromql.replace("\"value\"", "\"min\"")})""",
    s"""max_over_time($rawPromql)""" ->
      s"""max_over_time(${rawPromql.replace("\"value\"", "\"max\"")})""",
    s"""sum_over_time($rawPromql)""" ->
      s"""sum_over_time(${rawPromql.replace("\"value\"", "\"sum\"")})""",
    s"""count_over_time($rawPromql)""" ->
      s"""sum_over_time(${rawPromql.replace("\"value\"", "\"count\"")})"""
  )

  val now = System.currentTimeMillis()
  val endTime = (now - flushIntervalHours.toMillis) / 1000
  val lastHourEnd = (endTime / 1.hour.toSeconds) * 1.hour.toSeconds
  val startTime = (lastHourEnd - queryRange.toMillis / 1000)

  val urlPrefixRaw = s"$filodbHttpEndpoint/promql/prometheus/api"

  implicit val as = ActorSystem()
  implicit val materializer = ActorMaterializer()

  // TODO configure
  val urlPrefixes = Array (s"$filodbHttpEndpoint/promql/prometheus_ds_1m/api",
                           s"$filodbHttpEndpoint/promql/prometheus_ds_15m/api",
                           s"$filodbHttpEndpoint/promql/prometheus_ds_1hr/api")

  // TODO configure
  val steps = Array(1.minute.toSeconds,
                    15.minutes.toSeconds,
                    1.hour.toSeconds)

  val params = Map( "start" -> startTime.toString, "end" -> endTime.toString)

  val results = for {
    (step, urlPrefix) <- steps.zip(urlPrefixes)
    (rawPromQL, dsPromQL) <- rawToDsPromQl
  } yield {

    val dsPromQLFull = dsPromQL.replace("@@@@", step.toString)
    val rawPromQLFull = rawPromQL.replace("@@@@", step.toString)

    val dsParams = params ++ Map("step" -> step.toString, "query" -> dsPromQLFull)
    val dsUrl = Uri(s"$urlPrefix/v1/query_range").withQuery(Query(dsParams))
    val dsRespFut = Http().singleRequest(HttpRequest(uri = dsUrl)).flatMap(Unmarshal(_).to[SuccessResponse])
    val dsResp = try {
      Some(Await.result(dsRespFut, 10.seconds))
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        None
    }

    val rawParams = params ++ Map("step" -> step.toString, "query" -> rawPromQLFull)
    val rawUrl = Uri(s"$urlPrefixRaw/v1/query_range").withQuery(Query(rawParams))
    val rawRespFut = Http().singleRequest(HttpRequest(uri = rawUrl)).flatMap(Unmarshal(_).to[SuccessResponse])
    val rawResp = try {
      Some(Await.result(rawRespFut, 10.seconds))
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        None
    }

    val dsNorm = dsResp.get.data.copy(result =
      dsResp.get.data.result.sortWith((a, b) => a.metric("instance").compareTo(b.metric("instance")) > 0))
    val rawNorm = rawResp.get.data.copy(result =
      rawResp.get.data.result.sortWith((a, b) => a.metric("instance").compareTo(b.metric("instance")) > 0))

    logger.info(s"Step=${step}s testStatus=${dsNorm == rawNorm} rawUrl=$rawUrl  dsUrl=$dsUrl")

    if (dsNorm != rawNorm) {
      logger.error(s"Raw results: $rawNorm")
      logger.error(s"DS results: $dsNorm")
    }
    dsNorm == rawNorm
  }

  if (results.exists(b => !b))
    logger.info("***There was a failure. See logs for details")
  else
    logger.info("Success")

  CoordinatedShutdown(as).run(CoordinatedShutdown.UnknownReason)

}
