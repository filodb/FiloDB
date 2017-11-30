package filodb.coordinator.client

import scala.util.{Failure, Success, Try}

import org.parboiled2._

import filodb.coordinator.QueryCommands._
import filodb.core.metadata.DatasetOptions
import filodb.core.query.{ColumnFilter, Filter}

object PromQLParser {
  // We don't really want users to be able to scan every metric. Make this obscure.
  val ScanEverythingMetric = ":_really_scan_everything"

  val SecsInMinute = 60
  val SecsInHour   = 3600
  val SecsInDay    = SecsInHour * 24
  val SecsInWeek   = SecsInDay * 7

  sealed trait TimeSpec
  final case class DurationSecs(seconds: Int) extends TimeSpec
  final case class Special(word: String) extends TimeSpec
  case object NoTimeSpecified extends TimeSpec            // no duration specified

  val Quote = "\""
  val Space = " "
}

sealed trait Expr
final case class PartitionSpec(metricName: String,
                               column: String,
                               filters: Seq[ColumnFilter],
                               timeSpec: PromQLParser.TimeSpec) extends Expr

sealed trait PromQuery
final case class VectorExprOnlyQuery(spec: PartitionSpec) extends PromQuery
final case class FunctionQuery(functionName: String, param: Option[String], expr: Expr)
    extends PromQuery with Expr

final case class ArgsAndPartSpec(queryArgs: QueryArgs, partSpec: PartitionSpec) {
  /**
   * Adds a filter for the metricName, because under the hood FiloDB indexes everything and does not
   * treat the metricName any differently.
   * @param metricCol the index name or column name for the name of the metric
   * @param allowScanAll if true, and metricName matches ScanEverythingMetric, don't filter on metric
   */
  def withMetricFilter(metricCol: String, allowScanAll: Boolean): ArgsAndPartSpec =
    if (allowScanAll && partSpec.metricName == PromQLParser.ScanEverythingMetric) { this }
    else { this.copy(partSpec = partSpec.copy(filters =
                     partSpec.filters :+ ColumnFilter(metricCol, Filter.Equals(partSpec.metricName)))) }

  /**
   * Replaces ONE of the arguments of the main function.
   * May throw an exception if the index number is outside of range of arguments.
   */
  def withNewArg(argIndex: Int, newArg: String): ArgsAndPartSpec =
    this.copy(queryArgs = queryArgs.copy(args = queryArgs.args.updated(argIndex, newArg)))
}

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

  def Number = rule { capture(oneOrMore(CharPredicate.Digit)) ~> (_.toInt) }

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
  def FunctionOrSelector: Rule1[Expr] = rule { FunctionExpr | VectorSelector }

  def VectorExpr = rule { VectorSelector ~> VectorExprOnlyQuery }
  def Query: Rule1[PromQuery] = rule { (FunctionExpr | VectorExpr) ~ EOI }

  /**
   * Method to parse a PromQL query and return important parameters for the FiloDB client query call
   * Among other things, it runs withMetricFilter to add a filter for the metric name.
   */
  def parseAndGetArgs(allowScanAll: Boolean): Try[ArgsAndPartSpec] = {
    Query.run().flatMap {
      // Single level of function
      case FunctionQuery(funcName, paramOpt, partSpec: PartitionSpec) =>
        val spec = QueryArgs(funcName, partSpec.column, paramOpt.toSeq, dataQuery(partSpec.timeSpec))
        Success(ArgsAndPartSpec(spec, partSpec).withMetricFilter(options.metricColumn, allowScanAll))

      // Two levels of functions.  Assume outer one is combiner.
      // For now, only support a single combiner parameter.
      case FunctionQuery(combName, combParam,
             FunctionQuery(funcName, paramOpt, partSpec: PartitionSpec)) =>
        val spec = QueryArgs(funcName, partSpec.column, paramOpt.toSeq,
                             dataQuery(partSpec.timeSpec), combName, combParam.toSeq)
        Success(ArgsAndPartSpec(spec, partSpec).withMetricFilter(options.metricColumn, allowScanAll))

      case VectorExprOnlyQuery(partSpec) =>
        // For now just return the last data point and list all series
        val spec = QueryArgs("last", partSpec.column, Nil, dataQuery(partSpec.timeSpec))
        Success(ArgsAndPartSpec(spec, partSpec).withMetricFilter(options.metricColumn, allowScanAll))

      case other: PromQuery =>
        Failure(new IllegalArgumentException(s"Sorry, query with AST $other cannot be supported right now."))
    }
  }

  private def dataQuery(timeSpec: TimeSpec): DataQuery = timeSpec match {
    case DurationSecs(secs) => MostRecentTime(secs * 1000)
    case NoTimeSpecified    => MostRecentSample
    case Special(word)      => ???
  }
}