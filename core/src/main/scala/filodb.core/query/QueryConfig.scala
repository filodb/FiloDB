package filodb.core.query

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters._

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

import filodb.core.metadata.Column.ColumnType

object QueryConfig {
  val DefaultVectorsLimit = 150
  // scalastyle:off method.length
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
    val tenantsWithDisabledRemoteStitch : Set[String] =
      queryConfig.getStringList("routing.disabled-remote-stitch-tenants").asScala.toSet
    val stitchDisabledTenantColumn = queryConfig.getString("routing.disabled-remote-stitch-tenant-column-name")

    val rc = RoutingConfig(
        supportRemoteRawExport,
        maxRemoteRawExportTimeRange,
        enableApproximatelyEqualCheckInStitch,
        periodOfUncertaintyMs,
        tenantsWithDisabledRemoteStitch,
        stitchDisabledTenantColumn
    )

    val scCachingEnabled = queryConfig.as[Boolean]("single.cluster.cache.enabled")
    val scCacheSize = queryConfig.as[Int]("single.cluster.cache.cache-size")
    val cachingConfig = CachingConfig(scCachingEnabled, scCacheSize)
    val enableLocalDispatch = queryConfig.getBoolean("enable-local-dispatch")

    QueryConfig(askTimeout, staleSampleAfterMs, minStepMs, fastReduceMaxWindows, parser, translatePromToFilodbHistogram,
      fasterRateEnabled, routingConfig.as[Option[String]]("partition_name"),
      routingConfig.as[Option[Long]]("remote.http.timeout"),
      routingConfig.as[Option[String]]("remote.http.endpoint"),
      routingConfig.as[Option[String]]("remote.grpc.endpoint"),
      numRvsPerResultMessage, enforceResultByteLimit,
      allowPartialResultsRangeQuery, allowPartialResultsMetadataQuery,
      grpcDenyList.split(",").map(_.trim.toLowerCase).toSet,
      None,
      containerOverrides, rc, cachingConfig, enableLocalDispatch)
  }
  // scalastyle:on method.length

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
                          periodOfUncertaintyMs: Long                    = (5 minutes).toMillis,
                          tenantsWithDisabledRemoteStitch: Set[String]   = Set.empty,
                          stitchDisabledTenantColumn: String             = ""

                        )

case class CachingConfig(
                        singleClusterPlannerCachingEnabled: Boolean = true,
                        singleClusterPlannerCachingSize: Int = 2048
                        )

/**
 * Defines how counts of "samples scanned" are computed.
 *
 * A "scanned sample" is a unit that should correlate well with partition saturation.
 *   In other words: if any partition is saturated with a samples-scanned rate R, all other partitions--
 *   regardless of their distinct ingestion/query loads-- should also become saturated at
 *   that same samples-scanned rate.
 *
 * Scanned samples are counted for various dimensions of a query: rows, series, partition-key bytes, etc.
 *   These parameters should be tuned so partition samples-scanned rates correlate well with partition saturation.
 *
 * All default values maintain legacy behavior.
 *
 * *** NOTE!!! *******************************************************************************
 * All Class values are serialized as their .getName() strings.
 * Deserialization will fail if class names and packages are not consistent across partitions.
 * *******************************************************************************************
 *
 * @param leafSamplesEnabled toggle whether-or-not leaf samples are counted.
 * @param execResultSamplesEnabled toggle whether-or-not immediate doExecute samples are counted.
 * @param execChildSamplesEnabled toggle whether-or-not ExecPlan child samples are counted.
 * @param rvtSamplesEnabled toggle whether-or-not RangeVectorTransformer samples are counted.
 * @param rvtChildSamplesEnabled toggle whether-or-not RangeVectorTransformer child samples are counted.
 * @param srvSamplesEnabled toggle whether-or-not SerializedRangeVector samples are counted.
 * @param valueColumnToRowMultiplier maps value column types to multipliers applied to samples added per row.
 * @param defaultSamplesPerRow the default count of samples added per row; overridden by classToSamplesPerRow.
 * @param defaultSamplesPerSeries the count of samples added per time-series; overridden by classToSamplesPerSeries.
 * @param defaultSamplesPerPartKeyByte the count of samples added per partition key byte;
 *                                     overridden by classToSamplesPerPartKeyByte.
 * @param classToSamplesPerRow maps classes to the count of samples added per row; overrides defaultSamplesPerRow.
 * @param classToSamplesPerSeries maps classes to the count of samples added per time-series;
 *                                overrides defaultSamplesPerSeries.
 * @param classToSamplesPerPartKeyByte maps classes to the count of samples added per partition-key byte;
 *                                     overrides defaultSamplesPerPartKeyByte.
 * @param defaultSamplesPerChildRow the default count of samples added per child row;
 *                                  overridden by classToSamplesPerChildRow.
 * @param defaultSamplesPerChildSeries the default count of samples added per child time-series;
 *                                     overridden by classToSamplesPerChildSeries.
 * @param defaultSamplesPerChildPartKeyByte the default count of samples added per child partition key byte;
 *                                          overridden by classToSamplesPerChildPartKeyByte.
 * @param classToSamplesPerChildRow maps classes to the count of samples added per child row;
 *                                  overrides defaultSamplesPerChildRow.
 * @param classToSamplesPerChildSeries maps classes to the count of samples added per child time-series;
 *                                     overrides defaultSamplesPerChildSeries.
 * @param classToSamplesPerChildPartKeyByte maps classes to the count of samples added per child partition-key byte;
 *                                          overrides defaultSamplesPerChildPartKeyByte.
 */
case class SamplesScannedConfig(
                                 leafSamplesEnabled: Boolean = true,
                                 execResultSamplesEnabled: Boolean = false,
                                 execChildSamplesEnabled: Boolean = false,
                                 rvtSamplesEnabled: Boolean = false,
                                 rvtChildSamplesEnabled: Boolean = false,
                                 srvSamplesEnabled: Boolean = false,

                                 valueColumnToRowMultiplier: Map[ColumnType, Double] = Map(
                                   ColumnType.HistogramColumn -> 20
                                 ),

                                 defaultSamplesPerRow: Double = 1.0,
                                 defaultSamplesPerSeries: Double = 0.0,
                                 defaultSamplesPerPartKeyByte: Double = 0.0,
                                 classToSamplesPerRow: Map[Class[_], Double] = Map(),
                                 classToSamplesPerSeries: Map[Class[_], Double] = Map(),
                                 classToSamplesPerPartKeyByte: Map[Class[_], Double] = Map(),

                                 defaultSamplesPerChildRow: Double = 0.0,
                                 defaultSamplesPerChildSeries: Double = 0.0,
                                 defaultSamplesPerChildPartKeyByte: Double = 0.0,
                                 classToSamplesPerChildRow: Map[Class[_], Double] = Map(),
                                 classToSamplesPerChildSeries: Map[Class[_], Double] = Map(),
                                 classToSamplesPerChildPartKeyByte: Map[Class[_], Double] = Map()
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
                       enableLocalDispatch: Boolean = false,
                       samplesScannedConfig: SamplesScannedConfig = SamplesScannedConfig())
