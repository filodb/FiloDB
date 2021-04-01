package filodb.prometheus.parse

import filodb.core.query.{ColumnFilter, Filter}
import filodb.prometheus.ast._
import filodb.query.{LabelValues, LogicalPlan}

/**
  * Parser routes requests to LegacyParser or AntlrParser.
  */
object Parser {
  def parseQuery(query: String): Expression = {
    AntlrParser.parseQuery(query)
    //LegacyParser.parseQuery(query)
  }

  // TODO: Once fully switched to AntlrParser, get rid of the special precedence methods.
  def parseQueryWithPrecedence(query: String): Expression = {
    AntlrParser.parseQueryWithPrecedence(query)
    //LegacyParser.parseQueryWithPrecedence(query)
  }

  def parseLabelValueFilter(query: String): Seq[LabelMatch] = {
    AntlrParser.parseLabelValueFilter(query)
    //LegacyParser.parseLabelValueFilter(query)
  }

  def parseFilter(query: String): InstantExpression = {
    val expr = parseQuery(query)
    if (expr.isInstanceOf[InstantExpression]) {
      expr.asInstanceOf[InstantExpression]
    } else {
      throw new IllegalArgumentException(s"Expression $query is not a simple filter")
    }
  }

  def metadataQueryToLogicalPlan(query: String, timeParams: TimeRangeParams,
                                 fetchFirstLastSampleTimes: Boolean = false): LogicalPlan = {
    val expression = parseQuery(query)
    expression match {
      case p: InstantExpression => p.toMetadataPlan(timeParams, fetchFirstLastSampleTimes)
      case _ => throw new UnsupportedOperationException()
    }
  }

  def labelValuesQueryToLogicalPlan(labelNames: Seq[String], filterQuery: Option[String],
                                    timeParams: TimeRangeParams): LogicalPlan = {
    filterQuery match {
      case Some(filter) =>
        val columnFilters = parseLabelValueFilter(filter).map { l =>
          l.labelMatchOp match {
            case EqualMatch => ColumnFilter(l.label, Filter.Equals(l.value))
            case NotRegexMatch => ColumnFilter(l.label, Filter.NotEqualsRegex(l.value))
            case RegexMatch => ColumnFilter(l.label, Filter.EqualsRegex(l.value))
            case NotEqual(false) => ColumnFilter(l.label, Filter.NotEquals(l.value))
            case other: Any => throw new IllegalArgumentException(s"Unknown match operator $other")
          }
        }
        LabelValues(labelNames, columnFilters, timeParams.start * 1000, timeParams.end * 1000)
      case _ =>
        LabelValues(labelNames, Seq.empty, timeParams.start * 1000, timeParams.end * 1000)
    }
  }

  def queryToLogicalPlan(query: String, queryTimestamp: Long, step: Long): LogicalPlan = {
    // Remember step matters here in instant query, when lookback is provided in step factor
    // notation as in [5i]
    val defaultQueryParams = TimeStepParams(queryTimestamp, step, queryTimestamp)
    queryRangeToLogicalPlan(query, defaultQueryParams)
  }

  def queryRangeToLogicalPlan(query: String, timeParams: TimeRangeParams): LogicalPlan = {
    parseQueryWithPrecedence(query) match {
      case p: PeriodicSeries => p.toSeriesPlan(timeParams)
      case r: SimpleSeries   => r.toSeriesPlan(timeParams, isRoot = true)
      case _ => throw new UnsupportedOperationException()
    }
  }
}
