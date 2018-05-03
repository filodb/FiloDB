package filodb.prometheus.ast

import filodb.query.{PeriodicSeriesPlan, RawSeriesPlan}

trait Base {

  trait Expression

  trait Series

  trait PeriodicSeries extends Series {
    def toPeriodicSeriesPlan(queryParams: QueryParams): PeriodicSeriesPlan
  }

  trait SimpleSeries extends Series {
    def toRawSeriesPlan(queryParams: QueryParams, isRoot: Boolean): RawSeriesPlan
  }

  case class QueryParams(start: Long, step: Long, end: Long)

  /**
    * An identifier is an unquoted string
    */
  case class Identifier(str: String) extends Expression


}
