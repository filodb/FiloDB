package filodb.query

import filodb.core.{SpreadChange, SpreadProvider}
import filodb.core.query.{ColumnFilter, Filter}

/**
  * This class provides general query processing parameters
  *
  * @param spreadFunc a function that returns chronologically ordered spread changes for the filter
  */
final case class QueryOptions(spreadProvider: Option[SpreadProvider] = None,
                              parallelism: Int = 16,
                              queryTimeoutSecs: Int = 30,
                              sampleLimit: Int = 1000000,
                              shardOverrides: Option[Seq[Int]] = None,
                              processFailures: Boolean = true)

object QueryOptions {
  def apply(constSpread: Option[SpreadProvider], sampleLimit: Int): QueryOptions =
    QueryOptions(spreadProvider = constSpread, sampleLimit = sampleLimit)

  /**
    * Creates a spreadFunc that looks for a particular filter with keyName Equals a value, and then maps values
    * present in the spreadMap to specific spread values, with a default if the filter/value not present in the map
    */
  def simpleMapSpreadFunc(keyName: String,
                          spreadMap: collection.mutable.Map[collection.Map[String, String], Int],
                          defaultSpread: Int): Seq[ColumnFilter] => Seq[SpreadChange] = {
    filters: Seq[ColumnFilter] =>
      filters.collectFirst {
        case ColumnFilter(key, Filter.Equals(filtVal: String)) if key == keyName => filtVal
      }.map { tagValue =>
        Seq(SpreadChange(spread = spreadMap.getOrElse(collection.mutable.Map(keyName->tagValue), defaultSpread)))
      }.getOrElse(Seq(SpreadChange(defaultSpread)))
  }

  import collection.JavaConverters._

  def simpleMapSpreadFunc(keyName: String,
                          spreadMap: java.util.Map[java.util.Map[String, String], Integer],
                          defaultSpread: Int): Seq[ColumnFilter] => Seq[SpreadChange] = {
    val spreadAssignment: collection.mutable.Map[collection.Map[String, String], Int]= spreadMap.asScala.map {
      case (d, v) => d.asScala -> v.toInt
    }

    simpleMapSpreadFunc(keyName, spreadAssignment, defaultSpread)
  }
}
