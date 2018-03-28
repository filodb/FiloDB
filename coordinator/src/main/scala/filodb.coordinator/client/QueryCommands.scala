package filodb.coordinator.client

import filodb.core.query.{ColumnFilter, CombinerFunction, ExecPlan, Result}

object QueryCommands {
  import filodb.core._

  // These correspond to the ColumnStore PartitionScan methods, but take in raw data ie strings, ints
  // Which partitions should I query?
  sealed trait PartitionQuery
  final case class SinglePartitionQuery(key: Seq[Any]) extends PartitionQuery
  final case class MultiPartitionQuery(keys: Seq[Seq[Any]]) extends PartitionQuery
  final case class FilteredPartitionQuery(filters: Seq[ColumnFilter]) extends PartitionQuery

  // Which data within a partition should I query?
  sealed trait DataQuery
  case object AllPartitionData extends DataQuery   // All the data in a partition
  final case class KeyRangeQuery(start: Seq[Any], end: Seq[Any]) extends DataQuery
  // most recent lastMillis milliseconds of data.  The row key must be a single Long or Timestamp column
  // consisting of milliseconds since Epoch.  A shortcut for KeyRangeQuery.
  final case class MostRecentTime(lastMillis: Long) extends DataQuery
  // most recent single sample of data
  case object MostRecentSample extends DataQuery

  /**
   * Returns a Seq[String] of the first *limit* tags or columns indexed
   * Or Nil if the dataset is not found.
   */
  final case class GetIndexNames(dataset: DatasetRef,
                                 limit: Int = 10,
                                 submitTime: Long = System.currentTimeMillis()) extends QueryCommand

  /**
   * Returns a Seq[String] of the first *limit* values indexed for a given tag/column.
   * Or Nil if the dataset or indexName is not found.
   */
  final case class GetIndexValues(dataset: DatasetRef,
                                  indexName: String,
                                  limit: Int = 100,
                                  submitTime: Long = System.currentTimeMillis()) extends QueryCommand

  /**
   * Specifies details about the aggregation query to execute on the FiloDB server.
   * TODO: deprecated, remove when old combiner-aggregate pipeline is removed
   * @param functionName the name of the function for aggregating raw data
   * @param column       name of column holding the data to query
   * @param args         optional arguments to the function, may be none
   * @param dataQuery    selector/filter for data within a partition/time series.  This could be
   *                       MostRecentTime(nMillis) or MostRecentSample, or a specific range.
   * @param combinerName the name of a "combiner" which combines results from first level functions, such
   *                     as topk, bottomk, histogram, etc.
   */
  final case class QueryArgs(functionName: String,
                             column: String,
                             args: Seq[String] = Nil,
                             dataQuery: DataQuery = AllPartitionData,
                             combinerName: String = CombinerFunction.default,
                             combinerArgs: Seq[String] = Nil)

  final case class QueryOptions(shardKeySpread: Int = 1,
                                parallelism: Int = 16,
                                queryTimeoutSecs: Int = 30,
                                itemLimit: Int = 100,
                                shardOverrides: Option[Seq[Int]] = None)

  /**
   * Executes a query using a LogicalPlan and returns the result as one message to the client.
   * Depends on queryOptions, the query will fan out to multiple nodes and shards as needed to gather
   * results.
   * @param dataset the dataset (and possibly database) to query
   * @param plan the LogicalPlan for the query to run
   * @param queryOptions options to control routing of query
   * @return AggregateResponse, or BadQuery, BadArgument, WrongNumberOfArgs, UndefinedColumns
   */
  final case class LogicalPlanQuery(dataset: DatasetRef,
                                    plan: LogicalPlan,
                                    queryOptions: QueryOptions = QueryOptions(),
                                    submitTime: Long = System.currentTimeMillis()) extends QueryCommand

  /**
   * INTERNAL API only.
   * Executes a query using an ExecPlan (physical plan).
   * @param dataset the DatasetRef to be queried
   * @param execPlan the ExecPlan containing the physical execution query plan
   * @param limit the limit to the number of items returned
   */
  final case class ExecPlanQuery(dataset: DatasetRef,
                                 execPlan: ExecPlan[_, _],
                                 limit: Int,
                                 submitTime: Long = System.currentTimeMillis()) extends QueryCommand

  // Error responses from query
  final case class UndefinedColumns(undefined: Set[String]) extends ErrorResponse
  final case class BadArgument(msg: String) extends ErrorResponse with QueryResponse
  final case class BadQuery(msg: String) extends ErrorResponse with QueryResponse
  final case class WrongNumberOfArgs(actual: Int, expected: Int) extends ErrorResponse with QueryResponse

  final case class QueryError(id: Long, t: Throwable) extends ErrorResponse with QueryResponse {
    override def toString: String = s"QueryError id=$id ${t.getClass.getName} ${t.getMessage}\n" +
                                    t.getStackTrace.map(_.toString).mkString("\n")
  }

  final case class QueryResult(id: Long, result: Result) extends QueryResponse
}
