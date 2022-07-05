package filodb.core.query

import scala.concurrent.duration.FiniteDuration

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

object QueryConfig {
  val DefaultVectorsLimit = 150

  def apply(queryConfig: Config): QueryConfig = {
    val askTimeout = queryConfig.as[FiniteDuration]("ask-timeout")
    val staleSampleAfterMs = queryConfig.getDuration("stale-sample-after").toMillis
    val minStepMs = queryConfig.getDuration("min-step").toMillis
    val fastReduceMaxWindows = queryConfig.getInt("fastreduce-max-windows")
    val routingConfig = queryConfig.getConfig("routing")
    val parser = queryConfig.as[String]("parser")
    val translatePromToFilodbHistogram = queryConfig.getBoolean("translate-prom-to-filodb-histogram")
    val fasterRateEnabled = queryConfig.as[Option[Boolean]]("faster-rate").getOrElse(false)
    val allowPartialResultsMetadataQuery = queryConfig.getBoolean("allow-partial-results-metadataquery")
    val allowPartialResultsRangeQuery = queryConfig.getBoolean("allow-partial-results-rangequery")
    QueryConfig(askTimeout, staleSampleAfterMs, minStepMs, fastReduceMaxWindows, parser, translatePromToFilodbHistogram,
      fasterRateEnabled, routingConfig.as[Option[Long]]("remote.http.timeout"),
      routingConfig.as[Option[String]]("remote.http.endpoint"), allowPartialResultsRangeQuery,
      allowPartialResultsMetadataQuery )
  }

  import scala.concurrent.duration._
  /**
   * IMPORTANT: Use this for testing only, using this for anything other than testing may yield undesired behavior
   */
  val unitTestingQueryConfig = QueryConfig(10.seconds, 5.minutes.toMillis, 1, 50, "antlr", true, true, None, None,
    false)

}

case class QueryConfig(askTimeout: FiniteDuration,
                       staleSampleAfterMs: Long,
                       minStepMs: Long,
                       fastReduceMaxWindows: Int,
                       parser: String,
                       translatePromToFilodbHistogram: Boolean,
                       fasterRateEnabled: Boolean,
                       remoteHttpTimeoutMs: Option[Long],
                       remoteHttpEndpoint: Option[String],
                       allowPartialResultsRangeQuery: Boolean = false,
                       allowPartialResultsMetadataQuery: Boolean = false)

