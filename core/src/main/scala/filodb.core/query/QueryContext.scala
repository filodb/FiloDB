// scalastyle:off file.size.limit

package filodb.core.query

import java.util.UUID
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.{ArrayBuffer, SortedSet}
import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import net.ceedubs.ficus.Ficus._

import filodb.core.{QueryTimeoutException, SpreadChange, SpreadProvider, TargetSchemaChange, TargetSchemaProvider}
import filodb.memory.EvictionLock

trait TsdbQueryParams

/**
  * This class provides PromQl query parameters
  * Config has routing parameters
 *  startSecs, stepSecs, endSecs should not be used for query execution as it can be changed by query planner
  */
case class PromQlQueryParams(promQl: String, startSecs: Long, stepSecs: Long, endSecs: Long , remoteQueryPath:
                            Option[String] = None, verbose: Boolean = false) extends TsdbQueryParams

case object UnavailablePromQlQueryParams extends TsdbQueryParams

case class PerQueryLimits(
        execPlanSamples: Int = 1000000,       // Limit on ExecPlan results in samples, default is 1,000,000
        execPlanLeafSamples: Int = 1000000,   // Limit on ExecPlanLeaf results in samples, default is 1,000,000
        execPlanResultBytes: Long = 18000000, // Limit on ExecPlan results in bytes, default is 18MB
        groupByCardinality: Int = 100000,     // Limit on "group by" clause results, default is 100K
        joinQueryCardinality: Int = 100000,   // Limit on binary join input size, default is 100K
        timeSeriesSamplesScannedBytes: Long = 300000000, // max estimated data scanned per shard, default is 300 MB
        timeSeriesScanned: Int = 1000000,     // Limit on max number of time series scanned, default is 1M
        rawScannedBytes: Long = 200000000)    // Limit on max actual data scanned per shard. default is 200 MB
object PerQueryLimits {

  def defaultEnforcedLimits(): PerQueryLimits = {
    PerQueryLimits()
  }

  def defaultWarnLimits(): PerQueryLimits = {
    PerQueryLimits(
      execPlanSamples = 50000,
      execPlanResultBytes = 15000000,
      groupByCardinality = 50000,
      joinQueryCardinality = 50000,
      timeSeriesSamplesScannedBytes = 150000000,
      timeSeriesScanned = 500000,
      rawScannedBytes = 100000000
    )
  }

}

object QueryWarnings {

}
case class QueryWarnings(
  execPlanSamples: AtomicInteger = new AtomicInteger(0),
  execPlanLeafSamples: AtomicInteger = new AtomicInteger(0),
  execPlanResultBytes: AtomicLong = new AtomicLong(0),
  groupByCardinality: AtomicInteger = new AtomicInteger(0),
  joinQueryCardinality: AtomicInteger = new AtomicInteger(0),
  timeSeriesSamplesScannedBytes: AtomicLong = new AtomicLong(0),
  timeSeriesScanned: AtomicInteger = new AtomicInteger(0),
  rawScannedBytes: AtomicLong = new AtomicLong(0)
) {

  def hasWarnings() : Boolean = {
    execPlanSamples.get() > 0 ||
    execPlanLeafSamples.get() > 0 ||
    execPlanResultBytes.get() > 0 ||
    groupByCardinality.get() > 0 ||
    joinQueryCardinality.get() > 0 ||
    timeSeriesSamplesScannedBytes.get() > 0 ||
    timeSeriesScanned.get() > 0 ||
    rawScannedBytes.get() > 0
  }

  def merge(warnings: QueryWarnings) : Unit = {
    updateExecPlanSamples(warnings.execPlanSamples.get())
    updateExecPlanLeafSamples(warnings.execPlanLeafSamples.get())
    updateExecPlanResultBytes(warnings.execPlanResultBytes.get())
    updateGroupByCardinality(warnings.groupByCardinality.get())
    updateJoinQueryCardinality(warnings.joinQueryCardinality.get())
    updateTimeSeriesSampleScannedBytes(warnings.timeSeriesSamplesScannedBytes.get())
    updateTimeSeriesScanned(warnings.timeSeriesScanned.get())
    updateRawScannedBytes(warnings.rawScannedBytes.get())
  }

  def updateExecPlanSamples(samples: Int): Unit = {
    execPlanSamples.updateAndGet(s => if (s < samples) samples else s)
  }

  def updateExecPlanLeafSamples(leafSamples: Int): Unit = {
    execPlanLeafSamples.updateAndGet(s => if (s < leafSamples) leafSamples else s)
  }

  def updateExecPlanResultBytes(bytes: Long): Unit = {
    execPlanResultBytes.updateAndGet(b => if (b < bytes) bytes else b)
  }

  def updateGroupByCardinality(cardinality: Int): Unit = {
    groupByCardinality.updateAndGet(c => if (c < cardinality) cardinality else c)
  }

  def updateJoinQueryCardinality(cardinality: Int): Unit = {
    joinQueryCardinality.updateAndGet(c => if (c < cardinality) cardinality else c)
  }

  def updateTimeSeriesScanned(series: Int): Unit = {
    timeSeriesScanned.updateAndGet(s => if (s<series) series else s)
  }

  def updateTimeSeriesSampleScannedBytes(bytes: Long): Unit = {
    timeSeriesSamplesScannedBytes.updateAndGet(b => if (b<bytes) bytes else b)
  }

  def updateRawScannedBytes(bytes: Long): Unit = {
    rawScannedBytes.updateAndGet(b => if (b < bytes) bytes else b)
  }

  override def equals(w2Compare: Any): Boolean = {
    w2Compare match {
      case w2: QueryWarnings =>
        execPlanSamples.get().equals(w2.execPlanSamples.get()) &&
          execPlanLeafSamples.get().equals(w2.execPlanLeafSamples.get()) &&
          execPlanResultBytes.get().equals(w2.execPlanResultBytes.get()) &&
          groupByCardinality.get().equals(w2.groupByCardinality.get()) &&
          joinQueryCardinality.get().equals(w2.joinQueryCardinality.get()) &&
          timeSeriesSamplesScannedBytes.get().equals(w2.timeSeriesSamplesScannedBytes.get()) &&
          timeSeriesScanned.get().equals(w2.timeSeriesScanned.get()) &&
          rawScannedBytes.get().equals(w2.rawScannedBytes.get())
      case _ => false
    }
  }

  override def hashCode(): Int = {
    var c = execPlanSamples.get().hashCode()
    c = 31 * c + execPlanResultBytes.get().hashCode()
    c = 31 * c + groupByCardinality.get().hashCode()
    c = 31 * c + joinQueryCardinality.get().hashCode()
    c = 31 * c + timeSeriesSamplesScannedBytes.get().hashCode()
    c = 31 * c + timeSeriesScanned.get().hashCode()
    c = 31 * c + rawScannedBytes.get().hashCode()
    c
  }
}

case class DownPartition(name: String, downWorkUnits: scala.collection.mutable.Set[DownWorkUnit])
extends Ordered[DownPartition] {
  override def compare(that: DownPartition): Int = {
    name.compareTo(that.name)
  }
}
//object DownPartition {
//  def asImmutableSet(partitions: scala.collection.mutable.Set[DownPartition]) : Set[DownPartition] = partitions.toSet
//}
case class DownWorkUnit(name: String, downClusters: scala.collection.mutable.Set[DownCluster])
extends Ordered[DownWorkUnit] {
  override def compare(that: DownWorkUnit): Int = {
    name.compareTo(that.name)
  }
}
// Assume that the same shards are shared for various datasets withing the same partition, dc, and cluster
case class DownCluster(
  clusterType: String,
  downShards: scala.collection.mutable.Set[Int] = new scala.collection.mutable.LinkedHashSet[Int]
) extends Ordered[DownCluster] {
  override def compare(that: DownCluster): Int = {
    clusterType.compareTo(that.clusterType)
  }
}

object DownCluster {
  def create(clusterType: String): DownCluster = {
    DownCluster(clusterType)
  }
}

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
 * @param fixedRowMultiplier if present, overrides other row-multiplier configs. This single multiplier is applied
 *                           regardless of how many schema columns are present; the usual sum is skipped.
 * @param defaultRowMultiplier multiplier applied to row count for all non-histogram columns value types.
 * @param histogramRowMultiplier multiplier applied to row count for all non-
 *                               exponential histogram columns value types.
 * @param exponentialHistogramRowMultiplier multiplier applied to row count for all
 *                                          exponential histogram columns value types.
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

                                 fixedRowMultiplier: Option[Double] = None,
                                 defaultRowMultiplier: Double = 1.0,
                                 histogramRowMultiplier: Double = 25.0,
                                 exponentialHistogramRowMultiplier: Double = 50.0,

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

object SamplesScannedConfig {
  // scalastyle:off method.length
  def apply(config: Config): SamplesScannedConfig = {
    val defaults = SamplesScannedConfig()
    SamplesScannedConfig(
      config.as[Option[Boolean]]("leaf-samples-enabled")
        .getOrElse(defaults.leafSamplesEnabled),
      config.as[Option[Boolean]]("exec-result-samples-enabled")
        .getOrElse(defaults.execResultSamplesEnabled),
      config.as[Option[Boolean]]("exec-child-samples-enabled")
        .getOrElse(defaults.execChildSamplesEnabled),
      config.as[Option[Boolean]]("rvt-samples-enabled")
        .getOrElse(defaults.rvtSamplesEnabled),
      config.as[Option[Boolean]]("rvt-child-samples-enabled")
        .getOrElse(defaults.rvtChildSamplesEnabled),
      config.as[Option[Boolean]]("srv-samples-enabled")
        .getOrElse(defaults.srvSamplesEnabled),

      config.as[Option[Double]]("fixed-row-multiplier")
        .orElse(defaults.fixedRowMultiplier),
      config.as[Option[Double]]("default-row-multiplier")
        .getOrElse(defaults.defaultRowMultiplier),
      config.as[Option[Double]]("histogram-row-multiplier")
        .getOrElse(defaults.histogramRowMultiplier),
      config.as[Option[Double]]("exponential-histogram-row-multiplier")
        .getOrElse(defaults.exponentialHistogramRowMultiplier),

      config.as[Option[Double]]("default-samples-per-row")
        .getOrElse(defaults.defaultSamplesPerRow),
      config.as[Option[Double]]("default-samples-per-series")
        .getOrElse(defaults.defaultSamplesPerSeries),
      config.as[Option[Double]]("default-samples-per-part-key-byte")
        .getOrElse(defaults.defaultSamplesPerPartKeyByte),
      config.as[Option[Map[String, Double]]]("class-to-samples-per-row")
        .map { classNameToVal => classNameToVal.map { case (name, value) => Class.forName(name) -> value }}
        .getOrElse(defaults.classToSamplesPerRow),
      config.as[Option[Map[String, Double]]]("class-to-samples-per-series")
        .map { classNameToVal => classNameToVal.map { case (name, value) => Class.forName(name) -> value } }
        .getOrElse(defaults.classToSamplesPerSeries),
      config.as[Option[Map[String, Double]]]("class-to-samples-per-part-key-byte")
        .map { classNameToVal => classNameToVal.map { case (name, value) => Class.forName(name) -> value } }
        .getOrElse(defaults.classToSamplesPerPartKeyByte),

      config.as[Option[Double]]("default-samples-per-child-row")
        .getOrElse(defaults.defaultSamplesPerChildRow),
      config.as[Option[Double]]("default-samples-per-child-series")
        .getOrElse(defaults.defaultSamplesPerChildSeries),
      config.as[Option[Double]]("default-samples-per-child-part-key-byte")
        .getOrElse(defaults.defaultSamplesPerPartKeyByte),
      config.as[Option[Map[String, Double]]]("class-to-samples-per-child-row")
        .map { classNameToVal => classNameToVal.map { case (name, value) => Class.forName(name) -> value } }
        .getOrElse(defaults.classToSamplesPerChildRow),
      config.as[Option[Map[String, Double]]]("class-to-samples-per-child-series")
        .map { classNameToVal => classNameToVal.map { case (name, value) => Class.forName(name) -> value } }
        .getOrElse(defaults.classToSamplesPerChildSeries),
      config.as[Option[Map[String, Double]]]("class-to-samples-per-child-part-key-byte")
        .map { classNameToVal => classNameToVal.map { case (name, value) => Class.forName(name) -> value } }
        .getOrElse(defaults.classToSamplesPerChildPartKeyByte)
    )
  }
  //scalastyle:on method.length
}

case class PlannerParams(applicationId: String = "filodb",
                         spread: Option[Int] = None,
                         spreadOverride: Option[SpreadProvider] = None,
                         shardOverrides: Option[Seq[Int]] = None,
                         targetSchemaProviderOverride: Option[TargetSchemaProvider] = None,
                         queryTimeoutMillis: Int = 60000, // set default to match default http-request-timeout
                         enforcedLimits: PerQueryLimits = PerQueryLimits.defaultEnforcedLimits(),
                         warnLimits: PerQueryLimits = PerQueryLimits.defaultWarnLimits(),
                         queryOrigin: Option[String] = None, // alert/dashboard/rr/api/etc
                         queryOriginId: Option[String] = None, // an ID of rr/alert
                         queryPrincipal: Option[String] = None, // user, entity initiating query
                         timeSplitEnabled: Boolean = false,
                         minTimeRangeForSplitMs: Long = 1.day.toMillis,
                         splitSizeMs: Long = 1.day.toMillis,
                         skipAggregatePresent: Boolean = false,
                         processFailure: Boolean = true,
                         processMultiPartition: Boolean = false,
                         allowPartialResults: Boolean = false,
                         reduceShardKeyRegexFanout: Boolean = true,
                         maxShardKeyRegexFanoutBatchSize: Int = 10,
                         useProtoExecPlans: Boolean = false,
                         allowNestedAggregatePushdown: Boolean = true,
                         downPartitions: scala.collection.mutable.Set[DownPartition] = SortedSet[DownPartition](),
                         failoverMode: FailoverMode = LegacyFailoverMode,
                         buddyGrpcEndpoint: Option[String] = None,
                         buddyGrpcTimeoutMs: Option[Long] = None,
                         localShardMapper: Option[ActiveShardMapper] = None,
                         buddyShardMapper: Option[ActiveShardMapper] = None,
                         samplesScannedConfig: SamplesScannedConfig = SamplesScannedConfig()
                        )

object PlannerParams {
  def apply(constSpread: Option[SpreadProvider], sampleLimit: Int): PlannerParams =
    PlannerParams(spreadOverride = constSpread, enforcedLimits = PerQueryLimits(execPlanSamples = sampleLimit))
  def apply(constSpread: Option[SpreadProvider], partialResults: Boolean): PlannerParams =
    PlannerParams(spreadOverride = constSpread, allowPartialResults = partialResults)
}
/**
  * This class provides general query processing parameters
  */
final case class QueryContext(origQueryParams: TsdbQueryParams = UnavailablePromQlQueryParams,
                              queryId: String = UUID.randomUUID().toString,
                              submitTime: Long = System.currentTimeMillis(),
                              plannerParams: PlannerParams = PlannerParams(),
                              traceInfo: Map[String, String] = Map.empty[String, String]) {

  /**
   * Check timeout. If shouldThrow is true, exception is thrown. Otherwise exception is returned as return value.
   */
  def checkQueryTimeout(checkingFrom: String, shouldThrow: Boolean = true): Option[QueryTimeoutException] = {
    val queryTimeElapsed = System.currentTimeMillis() - submitTime
    if (queryTimeElapsed >= plannerParams.queryTimeoutMillis) {
      val ex = QueryTimeoutException(queryTimeElapsed, checkingFrom)
      if (shouldThrow) throw ex
      else Some(ex)
    } else None
  }

  def queryTimeRemaining: Long = {
    val timeElapsed = System.currentTimeMillis() - submitTime
    plannerParams.queryTimeoutMillis - timeElapsed
  }

  def getQueryLogLine(msg: String): String = {
    val promQl = origQueryParams match {
      case PromQlQueryParams(query: String, _, _, _, _, _) => query
      case UnavailablePromQlQueryParams => "unknown query"
    }
    val logLine = msg +
      s" promQL = -=# ${promQl} #=-" +
      s" queryOrigin = ${plannerParams.queryOrigin}" +
      s" queryPrincipal = ${plannerParams.queryPrincipal}" +
      s" queryOriginId = ${plannerParams.queryOriginId}" +
      s" queryId = ${queryId}"
    logLine
  }
}

object QueryContext {

  def apply(constSpread: Option[SpreadProvider], sampleLimit: Int): QueryContext =
    QueryContext(plannerParams = PlannerParams(constSpread, sampleLimit))

  def apply(queryParams: TsdbQueryParams, constSpread: Option[SpreadProvider],
            allowPartialResults: Boolean): QueryContext =
    QueryContext(origQueryParams = queryParams, plannerParams = PlannerParams(constSpread, allowPartialResults))

  /**
    * Creates a spreadFunc that looks for a particular filter with keyName Equals a value, and then maps values
    * present in the spreadMap to specific spread values, with a default if the filter/value not present in the map
    */
  def simpleMapSpreadFunc(shardKeyNames: Seq[String],
                          spreadMap: collection.mutable.Map[collection.Map[String, String], Int],
                          defaultSpread: Int): Seq[ColumnFilter] => Seq[SpreadChange] = {
    filters: Seq[ColumnFilter] =>
      val shardKeysInQuery = filters.collect {
        case ColumnFilter(key, Filter.Equals(filtVal: String)) if shardKeyNames.contains(key) => key -> filtVal
      }
      Seq(SpreadChange(spread = spreadMap.getOrElse(shardKeysInQuery.toMap, defaultSpread)))
  }

  import collection.JavaConverters._

  def simpleMapSpreadFunc(shardKeyNames: java.util.List[String],
                          spreadMap: java.util.Map[java.util.Map[String, String], Integer],
                          defaultSpread: Int): Seq[ColumnFilter] => Seq[SpreadChange] = {
    val spreadAssignment: collection.mutable.Map[collection.Map[String, String], Int]= spreadMap.asScala.map {
      case (d, v) => d.asScala -> v.toInt
    }

    simpleMapSpreadFunc(shardKeyNames.asScala.toSeq, spreadAssignment, defaultSpread)
  }

  /**
   * A functional TargetSchemaProvider which takes a targetSchema config that has key as shardKey/values mapped to
   * TargetSchema.
   * for e.g in the following config, first key has targetSchema as `_ws_,_ns_,_instanceId_`, All the metrics coming
   * from aService/aClient for an `_instanceId_` will be routed to a single shard.
   * {
   *  {"_ws_" -> "aService", "_ns_" ->"aClient" : ["_ws_","_ns_",_instanceId_"]},
   *  {"_ws_" -> "bService", "_ns_" ->"bClient" : ["_ws_","_ns_","_resourceId_"]}
   * }
   * @param shardKeyNames
   * @param targetSchemaMap
   * @param optionalShardKey look up targetSchemaMap excluding this filter (for e.g target-schema is defined at
   *                         _ws_ = "cService", then all the timeseries published from cService will use same
   *                         target-schema irrespective of the namespace.
   * @return
   */
  def mapTargetSchemaFunc(shardKeyNames: Seq[String],
                          targetSchemaMap: Map[Map[String, String], Seq[TargetSchemaChange]],
                          optionalShardKey: String)
          : Seq[ColumnFilter] => Seq[TargetSchemaChange] = {
    filters: Seq[ColumnFilter] =>
      val shardKeysInQuery = filters.collect {
        case ColumnFilter(key, Filter.Equals(filtVal: String)) if shardKeyNames.contains(key) => key -> filtVal
      }.toMap
      val nonOptShardKeys = filters.collect {
        case ColumnFilter(key, Filter.Equals(filtVal: String))
          if key != optionalShardKey && shardKeyNames.contains(key) => key -> filtVal
      }.toMap
      val defaultSchema = targetSchemaMap.get(nonOptShardKeys)
      val schema = targetSchemaMap.get(shardKeysInQuery)
      schema.orElse(defaultSchema) match {
        case Some(targetSchemaChanges) => targetSchemaChanges
        case None => Seq.empty
      }
  }

  def mapTargetSchemaFunc(shardKeyNames: java.util.List[String],
                          targetSchemaMap: java.util.Map[java.util.Map[String, String],
                          java.util.List[TargetSchemaChange]],
                          optionalShardKey: String)
          : Seq[ColumnFilter] => Seq[TargetSchemaChange] = {
    val targetSchema: Map[Map[String, String], Seq[TargetSchemaChange]] = targetSchemaMap.asScala.map {
      case (d, v) => d.asScala.toMap -> v.asScala.toSeq
    }.toMap
    mapTargetSchemaFunc(shardKeyNames.asScala.toSeq, targetSchema, optionalShardKey)
  }

}

/**
  * Placeholder for query related information. Typically passed along query execution path.
 * QuerySession should never be serialized and sent/received over the wire to a peer Filodb or client node.
  *
  * IMPORTANT: The param catchMultipleLockSetErrors should be false
  * only in unit test code for ease of use.
  *
  * IMPORTANT: QuerySession object should be closed after use as such
  * `monixTask.guarantee(Task.eval(querySession.close()))`
  *
  */
case class QuerySession(qContext: QueryContext,
                        queryConfig: QueryConfig,
                        streamingDispatch: Boolean = false,
                        catchMultipleLockSetErrors: Boolean = false,
                        var flightAllocator: Option[FlightAllocator] = None,
                        // in case of target schemas, when the child Exec plan is run, if the
                        //  the execution happens locally and thus no serialization is necessary
                        preventRangeVectorSerialization: Boolean = false) extends StrictLogging {

  val queryStats: QueryStats = QueryStats()
  // used to track number of times timeout is checked during query execution, to gate and control excessive checks
  val timeoutCheckCountDuringScan = new AtomicInteger(0)
  val warnings: QueryWarnings = QueryWarnings()
  private var lock: Option[EvictionLock] = None
  var resultCouldBePartial: Boolean = false
  var partialResultsReason: Option[String] = None

  def setLock(toSet: EvictionLock): Unit = {
    if (catchMultipleLockSetErrors && lock.isDefined)
      throw new IllegalStateException(s"Assigning eviction lock to session two times $qContext")
    lock = Some(toSet)
  }

  def close(skipAllocatorClose: Boolean = false): Unit = {
    lock.foreach(_.releaseSharedLock(qContext.queryId))
    lock = None
    if (!skipAllocatorClose) {
      flightAllocator.foreach(_.close())
      flightAllocator = None
    }
  }
}

case class Stat() {
  val timeSeriesScanned = new AtomicLong
  val dataBytesScanned = new AtomicLong
  val samplesScanned = new AtomicLong
  val resultBytes = new AtomicLong
  val cpuNanos = new AtomicLong

  override def toString: String = s"(timeSeriesScanned=$timeSeriesScanned, " +
    s"dataBytesScanned=$dataBytesScanned, samplesScanned=$samplesScanned, resultBytes=$resultBytes, cpuNanos=$cpuNanos)"
  def add(s: Stat): Unit = {
    timeSeriesScanned.addAndGet(s.timeSeriesScanned.get())
    dataBytesScanned.addAndGet(s.dataBytesScanned.get())
    samplesScanned.addAndGet(s.samplesScanned.get())
    resultBytes.addAndGet(s.resultBytes.get())
    cpuNanos.addAndGet(s.cpuNanos.get())
  }
}

case class QueryStats() {

  /**
   * Stores all stats entries.
   * Helpful in scenarios where e.g. entries need frequent iteration but
   *   Iterator allocations should be prevented.
   * Also useful for efficient size() calculation (as opposed to TrieMap.size(),
   *   which allocates an Object on the heap and has some computation overhead).
   */
  private val entries = ArrayBuffer[(Seq[String], Stat)]()
  private val keyToEntryIndex = new TrieMap[Seq[String], Integer]()

  /**
   * Set true if Nil is ever added as a key.
   * Useful in cases where this needs to be known efficiently
   *   (e.g. while samples-scanned counters are updated).
   **/
  @volatile private var containsNilKey = false;

  private val lock = new ReentrantReadWriteLock()
  private def readLock = lock.readLock()
  private def writeLock = lock.writeLock()

  override def toString: String = {
    readLock.lock()
    try {
      entries.toString()
    } finally {
      readLock.unlock()
    }
  }

  // scalastyle:off null
  // NOTE: null is used to avoid Option allocations.
  /**
   * Returns the index of the key's entry if it exists. Else returns null.
   * @param entryConfirmedExists as an optimization, "true" skips "contains" checks;
   *   an error will be thrown if this is "true" but the entry does not exist.
   */
  private def getNullableIndex(key: Seq[String],
                               entryConfirmedExists: Boolean = false): Integer = {
    // No locks required; keyToEntryIndex is a thread-safe data structure.
    if (entryConfirmedExists || keyToEntryIndex.contains(key)) keyToEntryIndex(key) else null
  }
  // scalastyle:on null

  // scalastyle:off null
  // NOTE: null is used to avoid Option allocations.
  /**
   * Returns the index of the key's entry if it exists. Else returns null.
   * More efficient than public [[get]] because [[Option]] allocations are avoided.
   * @param entryConfirmedExists as an optimization, "true" skips "contains" checks;
   *   an error will be thrown if this is "true" but the entry does not exist.
   */
  private def getNullableStat(key: Seq[String],
                              entryConfirmedExists: Boolean = false,
                              acquireReadLock: Boolean = true): Stat = {
    // NOTE: cannot acquire the lock after the first "return null"
    //   short-circuit; if clear() is called just after we get the
    //   index, we will access an index that does not exist!
    if (acquireReadLock) readLock.lock()
    try {
      val index = getNullableIndex(key, entryConfirmedExists)
      if (index == null) {
        return null
      }
      entries(index)._2
    } finally {
      if (acquireReadLock) readLock.unlock()
    }
  }
  // scalastyle:on null

  /**
   * Inserts a [[Stat]] into the [[QueryStats]].
   * If the argument key's entry already exists, the existing [[Stat]] is overwritten.
   *
   * @param entryConfirmedNotPresent as an optimization, "true" skips "contains" checks;
   *   undefined behavior if this is "true" but the entry already exists.
   */
  private def putInternal(key: Seq[String],
                          value: Stat,
                          entryConfirmedNotPresent: Boolean = false): Unit = {

    writeLock.lock()
    try {
      if (key.isEmpty) {
        containsNilKey = true
      }
      if (entryConfirmedNotPresent || !keyToEntryIndex.contains(key)) {
        entries.addOne((key, value))
        keyToEntryIndex.put(key, entries.size - 1)
      } else {
        // The key's entry must exist if we've entered this block.
        val index = getNullableIndex(key, entryConfirmedExists = true)
        entries(index) = (key, value)
      }
    } finally {
      writeLock.unlock()
    }
  }

  /**
   * Either returns the currently-stored [[Stat]] or returns a new,
   *   empty one that is stored against the argument key.
   */
  private def getOrPutEmptyStat(key: Seq[String]): Stat = {
    // Two checks to avoid acquiring a lock for every call...
    // First check: is the key already in the trie?
    // If it is: just return its value.
    val hasKeyAtFirstCheck = keyToEntryIndex.contains(key)
    if (hasKeyAtFirstCheck) {
      return getNullableStat(key, entryConfirmedExists = true)
    }
    // Otherwise, grab the lock.
    writeLock.lock()
    try {
      // Second check: was it added just before we acquired the lock?
      // If not, add it; else return its value.
      val hasKeyAtSecondCheck = keyToEntryIndex.contains(key)
      if (!hasKeyAtSecondCheck) {
        val newStat = Stat()
        putInternal(key, newStat, entryConfirmedNotPresent = true)
        newStat
      } else {
        // NOTE: calling this outside the lock might race with clear().
        getNullableStat(key, entryConfirmedExists = true)
      }
    } finally {
      writeLock.unlock()
    }
  }

  /**
   * Add the argument [[QueryStats]]' counters to this [[QueryStats]]' counters.
   */
  def add(s: QueryStats): Unit = {
    s.foreach { case (key, stat) => getOrPutEmptyStat(key).add(stat) }
  }

  /**
   * Returns all keys in the [[QueryStats]].
   * NOTE: not intended to be highly performant.
   */
  def keys(): Seq[Seq[String]] = {
    readLock.lock()
    try {
      entries.map { case (key, stat) => key }.toSeq
    } finally {
      readLock.unlock()
    }
  }

  /**
   * Adds the (key, value) pair to the [[QueryStats]].
   */
  def put(key: Seq[String], value: Stat): Unit = {
    putInternal(key, value)
  }

  /**
   * Returns the entry's value (if it exists).
   */
  def get(key: Seq[String]): Option[Stat] = {
    Option(getNullableStat(key))
  }

  // NOTE: Safe to be called from samples-scanned infrastructure;
  //   all QueryStats entries are added by the time any samples-scanned-
  //   tracking method is called.
  /**
   * Returns the count of elements in the [[QueryStats]].
   *
   * *** THREAD-SAFETY NOTE ***
   * This method *can* be called concurrently with any other that
   *   does not add/remove [[QueryStats]] entries.
   * It *cannot* be called concurrently with any method that
   *   adds/removes [[QueryStats]] entries.
   */
  def unsafeSize(): Int = {
    entries.size
  }

  // NOTE: Safe to be called from samples-scanned infrastructure;
  //   all QueryStats entries are added by the time any samples-scanned-
  //   tracking method is called.
  /**
   * Adds a total sample count to the argument [[QueryStats]]; the total is divided
   *   evenly across all samples-scanned counters.
   * NOTE: if Nil is the only [[QueryStats]] key, all samples are counted
   *   against it. If Nil exists with other keys, samples are divided
   *   among the non-Nil keys only.
   *
   * *** THREAD-SAFETY NOTE ***
   * This method *can* be called concurrently with itself or any other method
   *   that does not add/remove [[QueryStats]] entries.
   * It *cannot* be called concurrently with any method
   *   that adds/removes [[QueryStats]] entries.
   */
  def unsafeAddSamplesScanned(totalSampleCount: Long): Unit = {
    // NOTE: this method is called O(num_series) times; it must be super efficient.
    //   No locks are acquired below, and allocations are skipped wherever possible.

    // QueryStats keys are updated for all except Nil *unless* Nil
    //   is the only entry. Nil is sometimes added to QueryStats as a default.
    val hasSingleEmptyKey = unsafeSize() == 1 && containsNilKey
    if (hasSingleEmptyKey) {
      getNullableStat(Nil, entryConfirmedExists = true, acquireReadLock = false)
        .samplesScanned.addAndGet(totalSampleCount)
      return
    }

    val nonNilKeyCount = unsafeSize() - (if (containsNilKey) 1 else 0)
    val samplesPerCounter = Math.ceil(
      totalSampleCount.asInstanceOf[Double] / nonNilKeyCount
    ).asInstanceOf[Long]

    // NOTE: `while` avoids a Range allocation.
    var i = 0
    while (i < unsafeSize()) {
      val (key, stat) = entries(i)
      if (key.nonEmpty) {
        stat.samplesScanned.addAndGet(samplesPerCounter)
      }
      i += 1
    }
  }

   /**
    * Applies a consumer to each entry.
    * NOTE: not intended to be highly performant.
    */
   def foreach(consumer: ((Seq[String], Stat)) => Unit): Unit = {
     readLock.lock()
     try {
       entries.foreach(consumer)
     } finally {
       readLock.unlock()
     }
   }

  /**
   * Applies a mapper function to all entries of the [[QueryStats]].
   * NOTE: not intended to be highly performant.
   */
  def map[T](function: ((Seq[String], Stat)) => T): Iterable[T] = {
    readLock.lock()
    try {
      entries.map(function)
    } finally {
      readLock.unlock()
    }
  }

  /**
   * Clear all entries from the [[QueryStats]].
   */
  def clear(): Unit = {
    writeLock.lock()
    try {
      entries.clear()
      keyToEntryIndex.clear()
      containsNilKey = false
    } finally {
      writeLock.unlock()
    }
  }

  /**
   * Counter for number of time series scanned by query
   * @param group typically a tuple of (clusterType, dataset, WS, NS, metricName),
   *              and if tuple is not available, pass Nil. If Nil is passed,
   *              then head group is used if it exists.
   */
  def getTimeSeriesScannedCounter(group: Seq[String] = Nil): AtomicLong = {
    val theNs = if (group.isEmpty && keyToEntryIndex.size == 1) keyToEntryIndex.head._1 else group
    getOrPutEmptyStat(theNs).timeSeriesScanned
  }

  /**
   * Counter for amount of raw ingested (compressed) data scanned by query
   * @param group typically a tuple of (clusterType, dataset, WS, NS, metricName),
   *              and if tuple is not available, pass Nil. If Nil is passed,
   *              then head group is used if it exists.
   */
  def getDataBytesScannedCounter(group: Seq[String] = Nil): AtomicLong = {
    val theNs = if (group.isEmpty && keyToEntryIndex.size == 1) keyToEntryIndex.head._1 else group
    getOrPutEmptyStat(theNs).dataBytesScanned
  }

  /**
   * Counter for number of samples scanned by query
   * @param group typically a tuple of (clusterType, dataset, WS, NS, metricName),
   *              and if tuple is not available, pass Nil. If Nil is passed,
   *              then head group is used if it exists.
   */
  def getSamplesScannedCounter(group: Seq[String] = Nil): AtomicLong = {
    val theNs = if (group.isEmpty && keyToEntryIndex.size == 1) keyToEntryIndex.head._1 else group
    getOrPutEmptyStat(theNs).samplesScanned
  }

  /**
   * Counter for size of the materialized query result
   * @param group typically a tuple of (clusterType, dataset, WS, NS, metricName),
   *              and if tuple is not available, pass Nil. If Nil is passed,
   *              then head group is used if it exists.
   */
  def getResultBytesCounter(group: Seq[String] = Nil): AtomicLong = {
    val theNs = if (group.isEmpty && keyToEntryIndex.size == 1) keyToEntryIndex.head._1 else group
    getOrPutEmptyStat(theNs).resultBytes
  }

  /**
   * Counter for CPU Nano seconds consumed by query
   *
   * @param group typically a tuple of (clusterType, dataset, WS, NS, metricName),
   *              and if tuple is not available, pass Nil. If Nil is passed,
   *              then head group is used if it exists.
   */
  def getCpuNanosCounter(group: Seq[String] = Nil): AtomicLong = {
    val theNs = if (group.isEmpty && keyToEntryIndex.size == 1) keyToEntryIndex.head._1 else group
    getOrPutEmptyStat(theNs).cpuNanos
  }

  /**
   * Returns the sum of CPU nanos for all entries.
   */
  def totalCpuNanos: Long = {
    readLock.lock()
    try {
      entries.map { case (key, stat) => stat }.map(_.cpuNanos.get()).sum
    } finally {
      readLock.unlock()
    }
  }
}

object QuerySession {
  def makeForTestingOnly(): QuerySession = QuerySession(QueryContext(),
    QueryConfig.unitTestingQueryConfig, streamingDispatch = false)
}

// scalastyle:on file.size.limit
