package filodb.query

import enumeratum.{Enum, EnumEntry}

import filodb.core.{DatasetRef, NodeCommand, NodeResponse}
import filodb.core.memstore.ratelimit.CardinalityRecord
import filodb.core.query.{QueryStats, QueryWarnings, RangeVector, ResultSchema}

trait QueryCommand extends NodeCommand with java.io.Serializable {
  def submitTime: Long
  def dataset: DatasetRef
}

sealed trait QueryResponse extends NodeResponse with java.io.Serializable {
  def id: String
}

final case class QueryError(id: String,
                            queryStats: QueryStats,
                            t: Throwable) extends QueryResponse with filodb.core.ErrorResponse {
  override def toString: String = s"QueryError id=$id ${t.getClass.getName} ${t.getMessage}\n" +
    t.getStackTrace.map(_.toString).mkString("\n")
}

/**
  * Use this exception to raise user errors when inside the context of an observable.
  * Currently no other way to raise user errors when returning an observable
  */
class BadQueryException(message: String) extends RuntimeException(message)

case class RemoteQueryFailureException(statusCode: Int, requestStatus: String, errorType: String, errorMessage: String )
  extends RuntimeException {
  override def getMessage: String = {
    val sb = new StringBuilder(64)
    sb.append("[").append(this.requestStatus).append("] ").append(this.statusCode)
    if (this.errorType != null) sb.append(" (").append(this.errorType).append(")")
    if (this.errorMessage != null) sb.append(" - \"").append(this.errorMessage).append("\"")
    val cause = getCause
    if (cause == null) return sb.toString
    sb.append("; ").append("nested exception is ").append(cause)
    sb.toString
  }
}

sealed trait QueryResultType extends EnumEntry
object QueryResultType extends Enum[QueryResultType] {
  val values = findValues
  case object RangeVectors extends QueryResultType
  case object InstantVector extends QueryResultType
  case object Scalar extends QueryResultType
}

final case class QueryResult(id: String,
                             resultSchema: ResultSchema,
                             result: Seq[RangeVector],
                             queryStats: QueryStats = QueryStats(),
                             warnings: QueryWarnings = QueryWarnings(),
                             mayBePartial: Boolean = false,
                             partialResultReason: Option[String] = None) extends QueryResponse {
  def resultType: QueryResultType = {
    result match {
      case Nil => QueryResultType.RangeVectors
      case Seq(one)  if one.key.labelValues.isEmpty && one.numRows.contains(1) => QueryResultType.Scalar
      case many: Seq[RangeVector] => if (many.forall(_.numRows.contains(1))) QueryResultType.InstantVector
                                      else QueryResultType.RangeVectors
    }
  }
}

final case class TopkCardinalityResult(card: Seq[CardinalityRecord])

object QueryResponseConverter {

  implicit class QueryResponseToStreamingResponse(qr: QueryResponse) {
      def toStreamingResponse: Seq[StreamQueryResponse] = qr match {
        case QueryError(id, queryStats, t) => StreamQueryError(id, queryStats, t) :: Nil
        case QueryResult(id, resultSchema, result, queryStats, warnings, mayBePartial, partialResultReason) =>
          (StreamQueryResultHeader(id, resultSchema) :: result.map(StreamQueryResult(id, _)).toList) :::
            List(StreamQueryResultFooter(id, queryStats, warnings, mayBePartial, partialResultReason))
      }

  }
}


sealed trait StreamQueryResponse extends NodeResponse with java.io.Serializable {
  def queryId: String
  def isLast: Boolean = false
}

final case class StreamQueryResultHeader(queryId: String,
                                         resultSchema: ResultSchema) extends StreamQueryResponse

final case class StreamQueryResult(queryId: String,
                                   result: RangeVector) extends StreamQueryResponse

final case class StreamQueryResultFooter(queryId: String,
                                         queryStats: QueryStats = QueryStats(),
                                         warnings: QueryWarnings = QueryWarnings(),
                                         mayBePartial: Boolean = false,
                                         partialResultReason: Option[String] = None) extends StreamQueryResponse {
  override def isLast: Boolean = true
}

final case class StreamQueryError(queryId: String,
                                  queryStats: QueryStats,
                                  t: Throwable) extends StreamQueryResponse with filodb.core.ErrorResponse {
  override def isLast: Boolean = true
  override def toString: String = s"StreamQueryError id=$queryId ${t.getClass.getName} ${t.getMessage}\n" +
    t.getStackTrace.map(_.toString).mkString("\n")
}