package filodb.query

import java.util.UUID

import filodb.core.{SpreadChange, SpreadProvider}
import filodb.core.query.{ColumnFilter, Filter}

trait TsdbQueryParams
case class PromQlQueryParams(promQl: String, start: Long, step: Long, end: Long,
                             spread: Option[Int] = None, processFailure: Boolean = true) extends TsdbQueryParams
object UnavailablePromQlQueryParams extends TsdbQueryParams

/**
  * This class provides general query processing parameters
  */
final case class QueryContext(origQueryParams: TsdbQueryParams = UnavailablePromQlQueryParams,
                              spreadOverride: Option[SpreadProvider] = None,
                              queryTimeoutSecs: Int = 30,
                              sampleLimit: Int = 1000000,
                              shardOverrides: Option[Seq[Int]] = None,
                              queryId: String = UUID.randomUUID().toString,
                              submitTime: Long = System.currentTimeMillis())

object QueryContext {
  def apply(constSpread: Option[SpreadProvider], sampleLimit: Int): QueryContext =
    QueryContext(spreadOverride = constSpread, sampleLimit = sampleLimit)

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
