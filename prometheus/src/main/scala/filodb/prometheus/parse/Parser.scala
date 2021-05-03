package filodb.prometheus.parse

import com.typesafe.scalalogging.StrictLogging

import filodb.core.GlobalConfig
import filodb.core.query.{ColumnFilter, Filter, QueryConfig}
import filodb.prometheus.ast._
import filodb.query.{LabelValues, LogicalPlan}

/**
  * Parser routes requests to LegacyParser or AntlrParser.
  */
object Parser extends StrictLogging {
  sealed trait Mode
  case object Legacy extends Mode
  case object Antlr extends Mode
  case object Shadow extends Mode

  val mode: Mode = {
    val conf = GlobalConfig.systemConfig
    val queryConfig = new QueryConfig(conf.getConfig("filodb.query"))
    val parser = queryConfig.parser
    logger.info(s"Query parser mode: $parser")
    parser match {
      case "legacy" => Legacy
      case "antlr" => Antlr
      case "shadow" => Shadow
    }
  }

  // Only called directly by tests.
  def parseQuery(query: String): Expression = {
    mode match {
      case Antlr => AntlrParser.parseQuery(query)
      case Legacy => LegacyParser.parseQuery(query)
      case Shadow => {
        val expr = LegacyParser.parseQuery(query)
        try {
          AntlrParser.parseQuery(query)
        } catch {
          case e: Throwable => {
            logger.error(s"Antlr parse error: $query", e)
          }
        }
        expr
      }
    }
  }

  // TODO: Once fully switched to AntlrParser, get rid of the special precedence methods.
  private def parseQueryWithPrecedence(query: String): Expression = {
    mode match {
      case Antlr => AntlrParser.parseQuery(query)
      case Legacy => LegacyParser.parseQueryWithPrecedence(query)
      case Shadow => {
        val expr = LegacyParser.parseQueryWithPrecedence(query)
        try {
          AntlrParser.parseQuery(query)
        } catch {
          case e: Throwable => {
            logger.error(s"Antlr parse error: $query", e)
          }
        }
        expr
      }
    }
  }

  // Only called by tests.
  def parseLabelValueFilter(query: String): Seq[LabelMatch] = {
    mode match {
      case Antlr => AntlrParser.parseLabelValueFilter(query)
      case Legacy => LegacyParser.parseLabelValueFilter(query)
      case Shadow => {
        val labels = LegacyParser.parseLabelValueFilter(query)
        try {
          AntlrParser.parseLabelValueFilter(query)
        } catch {
          case e: Throwable => {
            logger.error(s"Antlr parse error: $query", e)
          }
        }
        labels
      }
    }
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

  // Only called by tests.
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

object ParserUtil {
  /**
   * Strip quotes and process escape codes.
   */
  def dequote(str: String): String = {
    val bob = new StringBuilder()
    var offset = 1
    while (offset < str.length() - 1) {
      var c = str.charAt(offset); offset += 1
      if (c == '\\') {
        val next = str.charAt(offset); offset += 1
        c = next match {
          case '\\' | '\'' | '"' => next
          case 'f' => '\f'
          case 'n' => '\n'
          case 'r' => '\r'
          case 't' => '\t'
          case _ => throw new IllegalArgumentException("illegal string escape: " + next)
        }
      }
      bob.append(c)
    }

    bob.toString()
  }
}
