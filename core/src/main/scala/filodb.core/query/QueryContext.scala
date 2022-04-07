package filodb.core.query

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._

import filodb.core.{SpreadChange, SpreadProvider, TargetSchemaChange, TargetSchemaProvider}
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

case class PlannerParams(applicationId: String = "filodb",
                         spread: Option[Int] = None,
                         spreadOverride: Option[SpreadProvider] = None,
                         shardOverrides: Option[Seq[Int]] = None,
                         targetSchema: Option[TargetSchemaProvider] = None,
                         queryTimeoutMillis: Int = 30000,
                         sampleLimit: Int = 1000000,
                         groupByCardLimit: Int = 100000,
                         joinQueryCardLimit: Int = 100000,
                         timeSplitEnabled: Boolean = false,
                         minTimeRangeForSplitMs: Long = 1.day.toMillis,
                         splitSizeMs: Long = 1.day.toMillis,
                         skipAggregatePresent: Boolean = false,
                         processFailure: Boolean = true,
                         processMultiPartition: Boolean = false,
                         allowPartialResults: Boolean = false)
object PlannerParams {
  def apply(constSpread: Option[SpreadProvider], sampleLimit: Int): PlannerParams =
    PlannerParams(spreadOverride = constSpread, sampleLimit = sampleLimit)
}
/**
  * This class provides general query processing parameters
  */
final case class QueryContext(origQueryParams: TsdbQueryParams = UnavailablePromQlQueryParams,
                              queryId: String = UUID.randomUUID().toString,
                              submitTime: Long = System.currentTimeMillis(),
                              plannerParams: PlannerParams = PlannerParams(),
                              traceInfo: Map[String, String] = Map.empty[String, String])

object QueryContext {
  def apply(constSpread: Option[SpreadProvider], sampleLimit: Int): QueryContext =
    QueryContext(plannerParams = PlannerParams(constSpread, sampleLimit))

  def apply(queryParams: TsdbQueryParams, constSpread: Option[SpreadProvider]): QueryContext =
    QueryContext(origQueryParams = queryParams, plannerParams = PlannerParams(spreadOverride = constSpread))

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

    simpleMapSpreadFunc(shardKeyNames.asScala, spreadAssignment, defaultSpread)
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
                          targetSchemaMap: Map[Map[String, String], Seq[String]],
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
      val defaultSchema = targetSchemaMap.getOrElse(nonOptShardKeys, Seq.empty)
      Seq(TargetSchemaChange(schema = targetSchemaMap.getOrElse(shardKeysInQuery, defaultSchema)))
  }

  def mapTargetSchemaFunc(shardKeyNames: java.util.List[String],
                          targetSchemaMap: java.util.Map[java.util.Map[String, String], java.util.List[String]],
                          optionalShardKey: String)
          : Seq[ColumnFilter] => Seq[TargetSchemaChange] = {
    val targetSchema: Map[Map[String, String], Seq[String]] = targetSchemaMap.asScala.map {
      case (d, v) => d.asScala.toMap -> v.asScala.toSeq
    }.toMap
    mapTargetSchemaFunc(shardKeyNames.asScala, targetSchema, optionalShardKey)
  }

}

/**
  * Placeholder for query related information. Typically passed along query execution path.
  *
  * IMPORTANT: The param catchMultipleLockSetErrors should be false
  * only in unit test code for ease of use.
  */
case class QuerySession(qContext: QueryContext,
                        queryConfig: QueryConfig,
                        catchMultipleLockSetErrors: Boolean = false) {

  val queryStats: QueryStats = QueryStats()
  private var lock: Option[EvictionLock] = None
  var resultCouldBePartial: Boolean = false
  var partialResultsReason: Option[String] = None

  def setLock(toSet: EvictionLock): Unit = {
    if (catchMultipleLockSetErrors && lock.isDefined)
      throw new IllegalStateException(s"Assigning eviction lock to session two times $qContext")
    lock = Some(toSet)
  }

  def close(): Unit = {
    lock.foreach(_.releaseSharedLock(qContext.queryId))
    lock = None
  }
}

case class Stat() {
  val timeSeriesScanned = new AtomicLong
  val dataBytesScanned = new AtomicLong
  val resultBytes = new AtomicLong
  override def toString: String = s"(timeSeriesScanned=$timeSeriesScanned, " +
    s"dataBytesScanned=$dataBytesScanned, resultBytes=$resultBytes)"
  def add(s: Stat): Unit = {
    timeSeriesScanned.addAndGet(s.timeSeriesScanned.get())
    dataBytesScanned.addAndGet(s.dataBytesScanned.get())
    resultBytes.addAndGet(s.resultBytes.get())
  }
}

case class QueryStats() {

  val stat = TrieMap[Seq[String], Stat]()

  override def toString: String = stat.toString()

  def add(s: QueryStats): Unit = {
    s.stat.foreach(kv => stat.getOrElseUpdate(kv._1, Stat()).add(kv._2))
  }

  def clear(): Unit = {
    stat.clear()
  }

  /**
   * Counter for number of time series scanned by query
   * @param group typically a tuple of (clusterType, dataset, WS, NS, metricName),
   *              and if tuple is not available, pass Nil. If Nil is passed,
   *              then head group is used if it exists.
   */
  def getTimeSeriesScannedCounter(group: Seq[String] = Nil): AtomicLong = {
    val theNs = if (group.isEmpty && stat.size == 1) stat.head._1 else group
    stat.getOrElseUpdate(theNs, Stat()).timeSeriesScanned
  }

  /**
   * Counter for amount of raw ingested (compressed) data scanned by query
   * @param group typically a tuple of (clusterType, dataset, WS, NS, metricName),
   *              and if tuple is not available, pass Nil. If Nil is passed,
   *              then head group is used if it exists.
   */
  def getDataBytesScannedCounter(group: Seq[String] = Nil): AtomicLong = {
    val theNs = if (group.isEmpty && stat.size == 1) stat.head._1 else group
    stat.getOrElseUpdate(theNs, Stat()).dataBytesScanned
  }

  /**
   * Counter for size of the materialized query result
   * @param group typically a tuple of (clusterType, dataset, WS, NS, metricName),
   *              and if tuple is not available, pass Nil. If Nil is passed,
   *              then head group is used if it exists.
   */
  def getResultBytesCounter(group: Seq[String] = Nil): AtomicLong = {
    val theNs = if (group.isEmpty && stat.size == 1) stat.head._1 else group
    stat.getOrElseUpdate(theNs, Stat()).resultBytes
  }

}

object QuerySession {
  def makeForTestingOnly(): QuerySession = QuerySession(QueryContext(), EmptyQueryConfig)
}