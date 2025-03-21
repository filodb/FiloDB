package filodb.core.query

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.{DurationInt, FiniteDuration}

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
    val enforceResultByteLimit = queryConfig.as[Boolean]("enforce-result-byte-limit")
    val allowPartialResultsMetadataQuery = queryConfig.getBoolean("allow-partial-results-metadataquery")
    val allowPartialResultsRangeQuery = queryConfig.getBoolean("allow-partial-results-rangequery")
    val grpcDenyList = queryConfig.getString("grpc.partitions-deny-list")
    val containerOverrides = queryConfig.as[Map[String, Int]]("container-size-overrides")
    val numRvsPerResultMessage = queryConfig.getInt("num-rvs-per-result-message")

    val supportRemoteRawExport = queryConfig.getBoolean("routing.enable-remote-raw-exports")
    val  enableApproximatelyEqualCheckInStitch =
      queryConfig.getBoolean("routing.enable-approximate-equals-in-stitch")
    val maxRemoteRawExportTimeRange =
      FiniteDuration(
        queryConfig.getDuration("routing.max-time-range-remote-raw-export").toMillis, TimeUnit.MILLISECONDS)
    val periodOfUncertaintyMs = queryConfig.getDuration("routing.period-of-uncertainty-ms").toMillis

    val rc = RoutingConfig(
      supportRemoteRawExport,
      maxRemoteRawExportTimeRange,
      enableApproximatelyEqualCheckInStitch,
      periodOfUncertaintyMs
      )

    val scCachingEnabled = queryConfig.as[Boolean]("single.cluster.cache.enabled")
    val scCacheSize = queryConfig.as[Int]("single.cluster.cache.cache-size")
    val cachingConfig = CachingConfig(scCachingEnabled, scCacheSize)

    QueryConfig(askTimeout, staleSampleAfterMs, minStepMs, fastReduceMaxWindows, parser, translatePromToFilodbHistogram,
      fasterRateEnabled, routingConfig.as[Option[String]]("partition_name"),
      routingConfig.as[Option[Long]]("remote.http.timeout"),
      routingConfig.as[Option[String]]("remote.http.endpoint"),
      routingConfig.as[Option[String]]("remote.grpc.endpoint"),
      numRvsPerResultMessage, enforceResultByteLimit,
      allowPartialResultsRangeQuery, allowPartialResultsMetadataQuery,
      grpcDenyList.split(",").map(_.trim.toLowerCase).toSet,
      None,
      containerOverrides, rc, cachingConfig)
  }

  import scala.concurrent.duration._
  /**
   * IMPORTANT: Use this for testing only, using this for anything other than testing may yield undesired behavior
   */
  val unitTestingQueryConfig = QueryConfig(askTimeout = 10.seconds,
                                           staleSampleAfterMs = 5.minutes.toMillis,
                                           minStepMs = 1,
                                           fastReduceMaxWindows = 50,
                                           parser = "antlr",
                                           translatePromToFilodbHistogram = true,
                                           fasterRateEnabled = true,
                                           partitionName = None,
                                           remoteHttpTimeoutMs = None,
                                           remoteHttpEndpoint = None,
                                           remoteGrpcEndpoint = None,
                                           enforceResultByteLimit = false,
                                           allowPartialResultsRangeQuery = false,
                                           allowPartialResultsMetadataQuery = true,
                                           recordContainerOverrides =
                                             Map("filodb-query-exec-aggregate-large-container" -> 65536,
                                                  "filodb-query-exec-metadataexec"             -> 8192))
}
case class RoutingConfig(
                          supportRemoteRawExport: Boolean                = false,
                          maxRemoteRawExportTimeRange: FiniteDuration    = 3 days,
                          enableApproximatelyEqualCheckInStitch: Boolean = true,
                          periodOfUncertaintyMs: Long                    = (5 minutes).toMillis
                        )

case class CachingConfig(
                        singleClusterPlannerCachingEnabled: Boolean = true,
                        singleClusterPlannerCachingSize: Int = 2048
                        )

case class QueryConfig(askTimeout: FiniteDuration,
                       staleSampleAfterMs: Long,
                       minStepMs: Long,
                       fastReduceMaxWindows: Int,
                       parser: String,
                       translatePromToFilodbHistogram: Boolean,
                       fasterRateEnabled: Boolean,
                       partitionName: Option[String],
                       remoteHttpTimeoutMs: Option[Long],
                       remoteHttpEndpoint: Option[String],
                       remoteGrpcEndpoint: Option[String],
                       numRvsPerResultMessage: Int = 100,
                       enforceResultByteLimit: Boolean = false,
                       allowPartialResultsRangeQuery: Boolean = false,
                       allowPartialResultsMetadataQuery: Boolean = true,
                       grpcPartitionsDenyList: Set[String] = Set.empty,
                       plannerSelector: Option[String] = None,
                       recordContainerOverrides: Map[String, Int] = Map.empty,
                       routingConfig: RoutingConfig               = RoutingConfig(),
                       cachingConfig: CachingConfig               = CachingConfig(),
                       enableLocalDispatch: Boolean = false)
