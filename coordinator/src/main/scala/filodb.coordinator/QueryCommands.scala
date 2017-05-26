package filodb.coordinator

import akka.actor.ActorRef
import java.nio.ByteBuffer
import scala.language.existentials

import filodb.core.query.{ColumnFilter, CombinerFunction}

object QueryCommands {
  import filodb.core._
  import filodb.core.Types._

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

  /**
   * Returns a Seq[String] of the first *limit* tags or columns indexed
   * Or Nil if the dataset is not found.
   */
  final case class GetIndexNames(dataset: DatasetRef,
                                 limit: Int = 10) extends QueryCommand

  /**
   * Returns a Seq[String] of the first *limit* values indexed for a given tag/column.
   * Or Nil if the dataset or indexName is not found.
   */
  final case class GetIndexValues(dataset: DatasetRef,
                                  indexName: String,
                                  limit: Int = 100) extends QueryCommand

  /**
   * Executes a query which returns the raw FiloVectors for the client to process
   * @param dataset the dataset (and possibly database) to query
   * @param columns the name of the columns to query.  Data will be returned for each chunkset in the same
   *                column order.
   * @param partitionQuery which partitions to query and filter on
   * @param dataQuery which data within a partition to return
   * @return QueryInfo followed by successive QueryRawChunks, followed by QueryEndRaw or QueryError
   */
  final case class RawQuery(dataset: DatasetRef,
                            version: Int,
                            columns: Seq[String],
                            partitionQuery: PartitionQuery,
                            dataQuery: DataQuery) extends QueryCommand

  /**
   * Specifies details about the aggregation query to execute on the FiloDB server.
   * @param functionName the name of the function for aggregating raw data
   * @param combinerName the name of a "combiner" which combines results from first level functions, such
   *                     as topk, bottomk, histogram, etc.
   */
  final case class QueryArgs(functionName: String,
                             args: Seq[String] = Nil,
                             combinerName: String = CombinerFunction.default,
                             combinerArgs: Seq[String] = Nil)

  /**
   * Executes a query which performs aggregation and returns the result as one message to the client
   * @param dataset the dataset (and possibly database) to query
   * @param version the version of the dataset to query.  Ignored for MemStores.
   * @param query   the QueryArgs specifying the name of the query function and the arguments as strings
   * @param partitionQuery which partitions to query and filter on
   * @param dataQuery optionally, which parts of a partition to query on.  Some functions such as time
   *                aggregates already control and take care of this, but others don't.  If the function
   *                does not specify this and this is also left unspecified then this defaults to
   *                AllPartitionData.
   * @return AggregateResponse, or BadQuery, BadArgument, WrongNumberOfArgs, UndefinedColumns
   */
  final case class AggregateQuery(dataset: DatasetRef,
                                  version: Int,
                                  query: QueryArgs,
                                  partitionQuery: PartitionQuery,
                                  dataQuery: Option[DataQuery] = None) extends QueryCommand

  // Error responses from query
  final case class BadArgument(msg: String) extends ErrorResponse with QueryResponse
  final case class BadQuery(msg: String) extends ErrorResponse with QueryResponse
  final case class WrongNumberOfArgs(actual: Int, expected: Int) extends ErrorResponse with QueryResponse

  /**
   * Metadata info about a query.
   * @param columnStrings serialized version of Column objects, use DataColumn.fromString()
   */
  final case class QueryInfo(id: Long, dataset: DatasetRef, columnStrings: Seq[String]) extends QueryResponse
  final case class QueryEndRaw(id: Long) extends QueryResponse
  final case class QueryError(id: Long, t: Throwable) extends ErrorResponse with QueryResponse {
    override def toString: String = s"QueryError id=$id ${t.getClass.getName} ${t.getMessage}\n" +
                                    t.getStackTrace.map(_.toString).mkString("\n")
  }

  final case class QueryRawChunks(queryID: Long,
                                  chunkID: ChunkID,
                                  buffers: Array[ByteBuffer]) extends QueryResponse

  final case class AggregateResponse[R](id: Long,
                                        elementClass: Class[_],
                                        elements: Array[R]) extends QueryResponse
}
