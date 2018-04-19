package filodb.query

import enumeratum.{Enum, EnumEntry}

import filodb.core.query.RangeVector

trait QueryResponse {
  def id: String
}

final case class QueryError(id: String, t: Throwable) extends QueryResponse {
  override def toString: String = s"QueryError id=$id ${t.getClass.getName} ${t.getMessage}\n" +
    t.getStackTrace.map(_.toString).mkString("\n")
}

/**
  * Use this exception to raise user errors when inside the context of an observable.
  * Currently no other way to raise user errors when returning an observable
  */
class BadQueryException(message: String) extends RuntimeException

sealed trait QueryResultType extends EnumEntry
object QueryResultType extends Enum[QueryResultType] {
  val values = findValues
  case object MultipleRangeVectors extends QueryResultType
  case object InstantVector extends QueryResultType
  case object Scalar extends QueryResultType
}

final case class QueryResult(id: String, result: Seq[RangeVector]) extends QueryResponse {
  def resultType: QueryResultType = ???
}


