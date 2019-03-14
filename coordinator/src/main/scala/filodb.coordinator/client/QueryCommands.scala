package filodb.coordinator.client

import filodb.core.query.{ColumnFilter, Filter}
import filodb.query.{LogicalPlan => LogicalPlan2, QueryCommand}

object QueryCommands {
  import filodb.core._

  // These correspond to the ColumnStore PartitionScan methods, but take in raw data ie strings, ints
  // Which partitions should I query?
  sealed trait PartitionQuery
  final case class SinglePartitionQuery(key: Seq[Any]) extends PartitionQuery
  final case class MultiPartitionQuery(keys: Seq[Seq[Any]]) extends PartitionQuery
  final case class FilteredPartitionQuery(filters: Seq[ColumnFilter]) extends PartitionQuery

  /**
   * Returns a Seq[String] of the first *limit* tags or columns indexed
   * Or Nil if the dataset is not found.
   */
  final case class GetIndexNames(dataset: DatasetRef,
                                 limit: Int = 10,
                                 submitTime: Long = System.currentTimeMillis()) extends QueryCommand

  /**
   * Returns a Seq[(String, Int)] of the top *limit* most popular values indexed for a given tag/column.
   * Or Nil if the dataset or indexName is not found.
   * @param shardOpt the shard to query for index values, if None, then the first shard is picked
   */
  final case class GetIndexValues(dataset: DatasetRef,
                                  indexName: String,
                                  shard: Int,
                                  limit: Int = 100,
                                  submitTime: Long = System.currentTimeMillis()) extends QueryCommand


  final case class SpreadChange(time: Long = 0L, spread: Int = 1)

  trait SpreadProvider {
    def spreadFunc(filter: Seq[ColumnFilter]): scala.Seq[SpreadChange]
  }

  case class StaticSpreadProvider(spreadChange: SpreadChange = SpreadChange()) extends SpreadProvider {
    def spreadFunc(filter: Seq[ColumnFilter]): scala.Seq[SpreadChange] = {
      Seq(spreadChange)
    }
  }

  /**
    * Serialize with care! would be based on the provided function.
    * @param f
    */
  case class FunctionalSpreadProvider(f: Seq[ColumnFilter] => Seq[SpreadChange] = { _ => Seq(SpreadChange()) })
    extends SpreadProvider {
    def spreadFunc(filter: Seq[ColumnFilter]): scala.Seq[SpreadChange] = {
      f (filter)
    }
  }

  /**
    * This class provides general query processing parameters
    * @param spreadFunc a function that returns chronologically ordered spread changes for the filter
    */
  final case class QueryOptions(spreadProvider: SpreadProvider = StaticSpreadProvider(),
                                parallelism: Int = 16,
                                queryTimeoutSecs: Int = 30,
                                sampleLimit: Int = 1000000,
                                shardOverrides: Option[Seq[Int]] = None)

  object QueryOptions {
    def apply(constSpread: Int, sampleLimit: Int): QueryOptions =
      QueryOptions(spreadProvider = StaticSpreadProvider(SpreadChange(0, constSpread)), sampleLimit = sampleLimit)

    /**
     * Creates a spreadFunc that looks for a particular filter with keyName Equals a value, and then maps values
     * present in the spreadMap to specific spread values, with a default if the filter/value not present in the map
     */
    def simpleMapSpreadFunc(keyName: String,
                            spreadMap: collection.Map[String, Int],
                            defaultSpread: Int): Seq[ColumnFilter] => Seq[SpreadChange] = {
      filters: Seq[ColumnFilter] =>
        filters.collectFirst {
          case ColumnFilter(key, Filter.Equals(filtVal: String)) if key == keyName => filtVal
        }.map { tagValue =>
          Seq(SpreadChange(spread = spreadMap.getOrElse(tagValue, defaultSpread)))
        }.getOrElse(Seq(SpreadChange(defaultSpread)))
    }

    import collection.JavaConverters._

    def simpleMapSpreadFunc(keyName: String,
                            spreadMap: java.util.Map[String, Int],
                            defaultSpread: Int): Seq[ColumnFilter] => Seq[SpreadChange] =
      simpleMapSpreadFunc(keyName, spreadMap.asScala, defaultSpread)
  }

  /**
   * Executes a query using a LogicalPlan and returns the result as one message to the client.
   * Depends on queryOptions, the query will fan out to multiple nodes and shards as needed to gather
   * results.
   * @param dataset the dataset (and possibly database) to query
   * @param logicalPlan the LogicalPlan for the query to run
   * @param queryOptions options to control routing of query
   * @return AggregateResponse, or BadQuery, BadArgument, WrongNumberOfArgs, UndefinedColumns
   */
  final case class LogicalPlan2Query(dataset: DatasetRef,
                                     logicalPlan: LogicalPlan2,
                                     queryOptions: QueryOptions = QueryOptions(),
                                     submitTime: Long = System.currentTimeMillis()) extends QueryCommand

  // Error responses from query
  final case class UndefinedColumns(undefined: Set[String]) extends ErrorResponse
  final case class BadArgument(msg: String) extends ErrorResponse with QueryResponse
  final case class BadQuery(msg: String) extends ErrorResponse with QueryResponse
  final case class WrongNumberOfArgs(actual: Int, expected: Int) extends ErrorResponse with QueryResponse
}
