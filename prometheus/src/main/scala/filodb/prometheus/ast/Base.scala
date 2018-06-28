package filodb.prometheus.ast

import filodb.query.{PeriodicSeriesPlan, RawSeriesPlan}

/**
 * NOTE: start and end are in SECONDS since Epoch
 */
case class QueryParams(start: Long, step: Long, end: Long)

trait Base {

  trait Expression

  trait Series

  trait PeriodicSeries extends Series {
    def toPeriodicSeriesPlan(queryParams: QueryParams): PeriodicSeriesPlan
  }

  trait SimpleSeries extends Series {
    def toRawSeriesPlan(queryParams: QueryParams, isRoot: Boolean): RawSeriesPlan
  }


  /**
    * An identifier is an unquoted string
    */
  case class Identifier(str: String) extends Expression


}
