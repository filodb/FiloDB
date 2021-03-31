package filodb.prometheus.parse

import filodb.prometheus.ast._
import filodb.query.LogicalPlan

/**
  * Parser routes requests to LegacyParser or AntlrParser.
  */
object PromParser {
  def parseQuery(query: String): Expression = {
    LegacyParser.parseQuery(query)
  }

  def parseFilter(query: String): InstantExpression = {
    LegacyParser.parseFilter(query)
  }

  def parseLabelValueFilter(query: String): Seq[LabelMatch] = {
    LegacyParser.parseLabelValueFilter(query)
  }

  def metadataQueryToLogicalPlan(query: String, timeParams: TimeRangeParams,
                                 fetchFirstLastSampleTimes: Boolean = false): LogicalPlan = {
    LegacyParser.metadataQueryToLogicalPlan(query, timeParams, fetchFirstLastSampleTimes)
  }

  def labelValuesQueryToLogicalPlan(labelNames: Seq[String], filterQuery: Option[String],
                                    timeParams: TimeRangeParams): LogicalPlan = {
    LegacyParser.labelValuesQueryToLogicalPlan(labelNames, filterQuery, timeParams)
  }

  def queryToLogicalPlan(query: String, queryTimestamp: Long, step: Long): LogicalPlan = {
    LegacyParser.queryToLogicalPlan(query, queryTimestamp, step)
  }

  def queryRangeToLogicalPlan(query: String, timeParams: TimeRangeParams): LogicalPlan = {
    LegacyParser.queryRangeToLogicalPlan(query, timeParams)
  }
}
