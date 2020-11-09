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

import filodb.query.PromCirceSupport
import filodb.query.SuccessResponse

/**
  * Use this tool to validate raw data against downsampled data for gauges.
  *
  *
  * Run as main class with following system properties:
  *
  * -Dquery-endpoint=https://myFiloDbEndpoint.com
  * -Draw-data-promql=jvm_threads::value{_ns=\"myApplication\",measure=\"daemon\"}[@@@@s]
  * -Dflush-interval=12h
  * -Dquery-range=6h
  *
  * raw-data-promql property value should end with '}[@@@@s]'.
  * The lookback window is replaced by validation tool when running the query.
  *
  * TODO This validator needs to be modified to reflect the newer approach to store downsampled data
  *
  */
object GaugeDownsampleValidator extends App with StrictLogging {

  import FailFastCirceSupport._
  // DO NOT REMOVE PromCirceSupport import below assuming it is unused - Intellij removes it in auto-imports :( .
  // Needed to override Sampl case class Encoder.
  import PromCirceSupport._
  import io.circe.generic.auto._

  case class DownsampleValidation(name: String, rawQuery: String, dsQuery: String)
  case class DownsampleLevel(step: Duration, endpoint: String)

  val config = ConfigFactory.load()
  val rawPromql = config.getString("raw-data-promql")
  val filodbHttpEndpoint = config.getString("query-endpoint")
  val flushIntervalHours = config.getDuration("flush-interval")
  val queryRange = config.getDuration("query-range")

  require((rawPromql.endsWith("""}[@@@@s]""")),
    """Raw Data PromQL should end with }[@@@@s]""")

  // List of validations to perform
  val validations = Seq (
    DownsampleValidation("min", s"""min_over_time($rawPromql)""",
      s"""min_over_time(${rawPromql.replace("\"value\"", "\"min\"")})"""),
    DownsampleValidation("max", s"""max_over_time($rawPromql)""",
      s"""max_over_time(${rawPromql.replace("\"value\"", "\"max\"")})"""),
      DownsampleValidation("sum", s"""sum_over_time($rawPromql)""",
      s"""sum_over_time(${rawPromql.replace("\"value\"", "\"sum\"")})"""),
        DownsampleValidation("count", s"""count_over_time($rawPromql)""",
      s"""sum_over_time(${rawPromql.replace("\"value\"", "\"count\"")})""")
  )

  val now = System.currentTimeMillis()
  val endTime = (now - flushIntervalHours.toMillis) / 1000
  val lastHourEnd = (endTime / 1.hour.toSeconds) * 1.hour.toSeconds
  val startTime = (lastHourEnd - queryRange.toMillis / 1000)

  val urlPrefixRaw = s"$filodbHttpEndpoint/promql/prometheus/api"

  implicit val as = ActorSystem()
  implicit val materializer = ActorMaterializer()

  // TODO configure dataset name etc.
  val downsampleLevels = Seq (
    DownsampleLevel(1.minute, s"$filodbHttpEndpoint/promql/prometheus_ds_1m/api"),
    DownsampleLevel(15.minutes, s"$filodbHttpEndpoint/promql/prometheus_ds_15m/api"),
    DownsampleLevel(60.minutes, s"$filodbHttpEndpoint/promql/prometheus_ds_1hr/api"))

  val params = Map( "start" -> startTime.toString, "end" -> endTime.toString)

  // validation loop:
  val results = for {
    level <- downsampleLevels // for each downsample dataset
    validation <- validations // for each validation
  } yield {

    val step = level.step.toSeconds
    // invoke query on downsample dataset
    val dsPromQLFull = validation.dsQuery.replace("@@@@", step.toString)
    val dsParams = params ++ Map("step" -> step.toString, "query" -> dsPromQLFull)
    val dsUrl = Uri(s"${level.endpoint}/v1/query_range").withQuery(Query(dsParams))
    val dsRespFut = Http().singleRequest(HttpRequest(uri = dsUrl)).flatMap(Unmarshal(_).to[SuccessResponse])
    val dsResp = try {
      Some(Await.result(dsRespFut, 10.seconds))
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        None
    }

    // invoke query on raw dataset
    val rawPromQLFull = validation.rawQuery.replace("@@@@", step.toString)
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

    // normalize the results by sorting the range vectors so we can do comparison
    val dsNorm = dsResp.get.data.copy(result =
      dsResp.get.data.result.sortWith((a, b) => a.metric("instance").compareTo(b.metric("instance")) > 0))
    val rawNorm = rawResp.get.data.copy(result =
      rawResp.get.data.result.sortWith((a, b) => a.metric("instance").compareTo(b.metric("instance")) > 0))

    logger.info(s"Downsampler=${validation.name} step=${step}s validationResult=${dsNorm == rawNorm} " +
      s"rawUrl=$rawUrl dsUrl=$dsUrl")

    if (dsNorm != rawNorm) {
      logger.error(s"Raw results: $rawNorm")
      logger.error(s"DS  results: $dsNorm")
    }
    dsNorm == rawNorm
  }

  CoordinatedShutdown(as).run(CoordinatedShutdown.UnknownReason)

  if (results.exists(b => !b)) {
    logger.info("Validation had a failure. See logs for details.")
    System.exit(10)
  }
  else
    logger.info("Validation was a success")


}
