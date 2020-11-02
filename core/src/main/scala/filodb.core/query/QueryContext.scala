package filodb.core.query

import java.util.UUID
import java.util.concurrent.locks.Lock

import filodb.core.{SpreadChange, SpreadProvider}

trait TsdbQueryParams

/**
  * This class provides PromQl query parameters
  * Config has routing parameters
  */
case class PromQlQueryParams(promQl: String, startSecs: Long, stepSecs: Long, endSecs: Long , remoteQueryPath:
                            Option[String] = None, verbose: Boolean = false) extends TsdbQueryParams

case object UnavailablePromQlQueryParams extends TsdbQueryParams

case class PlannerParams(applicationId: String = "filodb",
                        spread: Option[Int] = None,
                        spreadOverride: Option[SpreadProvider] = None,
                        shardOverrides: Option[Seq[Int]] = None,
                        queryTimeoutMillis: Int = 30000,
                        sampleLimit: Int = 1000000,
                        groupByCardLimit: Int = 100000,
                        joinQueryCardLimit: Int = 100000,
                        skipAggregatePresent: Boolean = false,
                        processFailure: Boolean = true,
                        processMultiPartition: Boolean = false)
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
                              plannerParams: PlannerParams = PlannerParams())

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
}

/**
  * Placeholder for query related information. Typically passed along query execution path.
  */
case class QuerySession(qContext: QueryContext,
                        queryConfig: QueryConfig,
                        var lock: Option[Lock] = None) {
  def close(): Unit = {
    lock.foreach(_.unlock())
    lock = None
  }
}

object QuerySession {
  def forTestingOnly: QuerySession = QuerySession(QueryContext(), EmptyQueryConfig)
}