package filodb.core.query

import java.util.UUID

import com.typesafe.config.{Config, ConfigRenderOptions}

import filodb.core.{SpreadChange, SpreadProvider}

trait TsdbQueryParams

/**
  * This class provides PromQl query paramaters
  * Config has routing parameters
  */
case class PromQlQueryParams(config: String, promQl: String, startSecs: Long, stepSecs: Long, endSecs: Long,
                             spread: Option[Int] = None, processFailure: Boolean = true) extends TsdbQueryParams
object PromQlQueryParams {
  def apply(config: Config, promQl: String, startSecs: Long, stepSecs: Long, endSecs: Long,
            spread: Option[Int], processFailure: Boolean): PromQlQueryParams = {
    // Config object requires custom serialization
    PromQlQueryParams(config.root().render(ConfigRenderOptions.concise()), promQl, startSecs, stepSecs, endSecs, spread,
      processFailure)
  }

  def apply(config: Config, promQl: String, startSecs: Long, stepSecs: Long, endSecs: Long): PromQlQueryParams =
    PromQlQueryParams(config.root().render(ConfigRenderOptions.concise()), promQl, startSecs, stepSecs, endSecs, None,
      true)
}
case object UnavailablePromQlQueryParams extends TsdbQueryParams

/**
  * This class provides general query processing parameters
  */
final case class QueryContext(origQueryParams: TsdbQueryParams = UnavailablePromQlQueryParams,
                              spreadOverride: Option[SpreadProvider] = None,
                              queryTimeoutMillis: Int = 30000,
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
