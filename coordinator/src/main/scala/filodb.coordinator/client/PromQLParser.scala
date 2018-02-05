package filodb.coordinator.client

import scala.util.{Failure, Success, Try}

import org.parboiled2._

import filodb.coordinator.client.QueryCommands._
import filodb.core.metadata.DatasetOptions
import filodb.core.query.{ColumnFilter, Filter}

object PromQLParser {
  // We don't really want users to be able to scan every metric. Make this obscure.
  val ScanEverythingMetric = ":_really_scan_everything"

  val SecsInMinute = 60
  val SecsInHour   = 3600
  val SecsInDay    = SecsInHour * 24L
  val SecsInWeek   = SecsInDay * 7L

  sealed trait TimeSpec
  final case class DurationSecs(seconds: Long) extends TimeSpec
  final case class Special(word: String) extends TimeSpec
  case object NoTimeSpecified extends TimeSpec            // no duration specified

  val Quote = "\""
  val Space = " "
}

sealed trait PromExpr
final case class PartitionSpec(metricName: String,
                               column: String,
                               filters: Seq[ColumnFilter],
                               timeSpec: PromQLParser.TimeSpec) extends PromExpr {
  /**
   * Adds a filter for the metricName, because under the hood FiloDB indexes everything and does not
   * treat the metricName any differently.
   * @param metricCol the name of the tag or column used to represent the metric.  __name__ for Prometheus.
   * @param allowScanAll if true, and metricName matches ScanEverythingMetric, don't filter on metric
   */
  def withMetricFilter(metricCol: String, allowScanAll: Boolean): Seq[ColumnFilter] =
    if (allowScanAll && metricName == PromQLParser.ScanEverythingMetric) { filters }
    else { filters :+ ColumnFilter(metricCol, Filter.Equals(metricName)) }
}

final case class FunctionQuery(functionName: String, param: Option[String], expr: PromExpr) extends PromExpr

/**
 * A Prometheus PromQL-like query syntax parser
 * To be able to parse expressions like {{ sum(http-requests-total#avg{method="GET"}[5m]) }}
 *
 * Differences from PromQL:
 * - FiloDB has flexible schemas, so #columnName can be appended to the metrics name
 *   (there is a default)
 * - What column does the metric name map to?  TODO: allow filtering on metric name too
 * - For now very strict subset supported
 *   - nested functions can be parsed now
 */
class PromQLParser(val input: ParserInput,
                   options: DatasetOptions = DatasetOptions.DefaultOptions) extends Parser {
  import PromQLParser._
  import Filter._

  // scalastyle:off method.name
  // scalastyle:off public.methods.have.type
  def AlphaNumSymbols = CharPredicate.AlphaNum ++ CharPredicate("_-:/")
  def NameString = rule { capture(oneOrMore(AlphaNumSymbols)) }
  def QuotedString = rule { Quote ~ capture(oneOrMore(noneOf(Quote))) ~ Quote }

  def TimeRange = rule { '[' ~ Duration ~ ']' }

  def Number = rule { capture(oneOrMore(CharPredicate.Digit)) ~> (_.toLong) }

  def Duration: Rule1[TimeSpec] = rule { DurationSec | DurationMin | DurationHour | DurationDay | DurationWeek }
  def DurationSec = rule { Number ~ Space.? ~ "s" ~> DurationSecs }
  def DurationMin = rule { Number ~ Space.? ~ "m" ~>  (s => DurationSecs(s * SecsInMinute)) }
  def DurationHour = rule { Number ~ Space.? ~ "h" ~> (s => DurationSecs(s * SecsInHour)) }
  def DurationDay = rule { Number ~ Space.? ~ "d" ~>  (s => DurationSecs(s * SecsInDay)) }
  def DurationWeek = rule { Number ~ Space.? ~ "w" ~> (s => DurationSecs(s * SecsInWeek)) }

  def TagSelector = rule { '{' ~ oneOrMore(TagFilter).separatedBy(ch(',')) ~ '}' }

  def EqualsFilter =
    rule { Space.? ~ NameString ~ "=" ~ QuotedString ~> ((tag, value) => ColumnFilter(tag, Equals(value))) }

  def TagFilter: Rule1[ColumnFilter] = rule { EqualsFilter }

  def DataColumnSelector = rule { "#" ~ NameString }
  def VectorSelector =
    rule { NameString ~ DataColumnSelector.? ~ TagSelector.? ~ TimeRange.? ~>
           ((metric: String, column: Option[String], filters: Option[Seq[ColumnFilter]],
             timeSpec: Option[TimeSpec]) =>
            PartitionSpec(metric, column.getOrElse(options.valueColumn),
                          filters.getOrElse(Nil), timeSpec.getOrElse(NoTimeSpecified))) }

  def FunctionParam = rule { NameString ~ ',' ~ Space.? }
  def FunctionExpr = rule { NameString ~ '(' ~ FunctionParam.? ~ FunctionOrSelector ~ ')' ~> FunctionQuery }

  // NOTE: the FunctionExpr rule has to come first or else some VectorSelectors don't get parsed correctly
  def FunctionOrSelector: Rule1[PromExpr] = rule { FunctionExpr | VectorSelector }

  def Query: Rule1[PromExpr] = rule { (FunctionExpr | VectorSelector) ~ EOI }

  /**
   * Method to parse a PromQL query and translate it to a FiloDB LogicalPlan for optimization and querying.
   * Among other things, it runs withMetricFilter to add a filter for the metric name.
   */
  def parseToPlan(allowScanAll: Boolean): Try[LogicalPlan] = Query.run().flatMap(e => parseExpr(e, allowScanAll))

  def parseExpr(expr: PromExpr, allowScanAll: Boolean): Try[LogicalPlan] = expr match {
    case p @ PartitionSpec(_, column, _, NoTimeSpecified) =>
      val filters = p.withMetricFilter(options.metricColumn, allowScanAll)
      Success(LogicalPlan.PartitionsInstant(FilteredPartitionQuery(filters), Seq(column)))

    case p @ PartitionSpec(_, column, _, timeSpec) =>
      val timeFilter = dataQuery(timeSpec)
      val filters = p.withMetricFilter(options.metricColumn, allowScanAll)
      Success(LogicalPlan.PartitionsRange(FilteredPartitionQuery(filters), timeFilter, Seq(column)))

    case FunctionQuery(funcName, paramOpt, child: PromExpr) =>
      parseExpr(child, allowScanAll).flatMap { childPlan =>
        child match {
          case p: PartitionSpec =>
            Success(LogicalPlan.ReduceEach(funcName, paramOpt.toSeq, childPlan))
          case _ =>
            Success(LogicalPlan.ReducePartitions(funcName, paramOpt.toSeq, childPlan))
        }
      }

    case other: PromExpr =>
      Failure(new IllegalArgumentException(s"Sorry, query with AST $other cannot be supported right now."))
  }

  private def dataQuery(timeSpec: TimeSpec): DataQuery = timeSpec match {
    case DurationSecs(secs) => MostRecentTime(secs * 1000L)
    case NoTimeSpecified    => MostRecentSample
    case Special(word)      => ???
  }
}