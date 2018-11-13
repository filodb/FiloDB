package filodb.prometheus.ast

import filodb.query.{MetadataQueryPlan, PeriodicSeriesPlan, RawSeriesPlan}

/**
 * NOTE: start and end are in SECONDS since Epoch
 */
case class QueryParams(start: Long, step: Long, end: Long)

case class MetadataQueryParams(start: Long, end: Long, lookBackTimeInMillis: Long = 86400000)

trait Base {

  trait Expression

  trait Series

  trait PeriodicSeries extends Series {
    def toPeriodicSeriesPlan(queryParams: QueryParams): PeriodicSeriesPlan
  }

  trait SimpleSeries extends Series {
    def toRawSeriesPlan(queryParams: QueryParams, isRoot: Boolean): RawSeriesPlan
  }

  trait Metadata extends Expression {
    def toMetadataQueryPlan(queryParams: MetadataQueryParams) : MetadataQueryPlan
  }

  /**
    * An identifier is an unquoted string
    */
  case class Identifier(str: String) extends Expression


}
